use std::collections::{BTreeMap, BTreeSet};

use obkv::KvReader;
use serde_json::{from_slice, Value};

use super::Embedding;
use crate::index::IndexEmbeddingConfig;
use crate::update::del_add::{DelAdd, KvReaderDelAdd};
use crate::{DocumentId, FieldId, InternalError, UserError};

pub const RESERVED_VECTORS_FIELD_NAME: &str = "_vectors";

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(untagged)]
pub enum Vectors {
    ImplicitlyUserProvided(VectorOrArrayOfVectors),
    Explicit(ExplicitVectors),
}

impl Vectors {
    pub fn must_regenerate(&self) -> bool {
        match self {
            Vectors::ImplicitlyUserProvided(_) => false,
            Vectors::Explicit(ExplicitVectors { regenerate, .. }) => *regenerate,
        }
    }

    pub fn into_array_of_vectors(self) -> Option<Vec<Embedding>> {
        match self {
            Vectors::ImplicitlyUserProvided(embeddings) => {
                Some(embeddings.into_array_of_vectors().unwrap_or_default())
            }
            Vectors::Explicit(ExplicitVectors { embeddings, regenerate: _ }) => {
                embeddings.map(|embeddings| embeddings.into_array_of_vectors().unwrap_or_default())
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExplicitVectors {
    pub embeddings: Option<VectorOrArrayOfVectors>,
    pub regenerate: bool,
}

pub enum VectorState {
    Inline(Vectors),
    Manual,
    Generated,
}

impl VectorState {
    pub fn must_regenerate(&self) -> bool {
        match self {
            VectorState::Inline(vectors) => vectors.must_regenerate(),
            VectorState::Manual => false,
            VectorState::Generated => true,
        }
    }
}

pub enum VectorsState {
    NoVectorsFid,
    NoVectorsFieldInDocument,
    Vectors(BTreeMap<String, Vectors>),
}

pub struct ParsedVectorsDiff {
    old: BTreeMap<String, VectorState>,
    new: VectorsState,
}

impl ParsedVectorsDiff {
    pub fn new(
        docid: DocumentId,
        embedders_configs: &[IndexEmbeddingConfig],
        documents_diff: KvReader<'_, FieldId>,
        old_vectors_fid: Option<FieldId>,
        new_vectors_fid: Option<FieldId>,
    ) -> Result<Self, Error> {
        let mut old = match old_vectors_fid
            .and_then(|vectors_fid| documents_diff.get(vectors_fid))
            .map(KvReaderDelAdd::new)
            .map(|obkv| to_vector_map(obkv, DelAdd::Deletion))
            .transpose()
        {
            Ok(del) => del,
            // ignore wrong shape for old version of documents, use an empty map in this case
            Err(Error::InvalidMap(value)) => {
                tracing::warn!(%value, "Previous version of the `_vectors` field had a wrong shape");
                Default::default()
            }
            Err(error) => {
                return Err(error);
            }
        }
        .flatten().map_or(BTreeMap::default(), |del| del.into_iter().map(|(name, vec)| (name, VectorState::Inline(vec))).collect());
        for embedding_config in embedders_configs {
            if embedding_config.user_provided.contains(docid) {
                old.entry(embedding_config.name.to_string()).or_insert(VectorState::Manual);
            }
        }

        let new = 'new: {
            let Some(new_vectors_fid) = new_vectors_fid else {
                break 'new VectorsState::NoVectorsFid;
            };
            let Some(bytes) = documents_diff.get(new_vectors_fid) else {
                break 'new VectorsState::NoVectorsFieldInDocument;
            };
            let obkv = KvReaderDelAdd::new(bytes);
            match to_vector_map(obkv, DelAdd::Addition)? {
                Some(new) => VectorsState::Vectors(new),
                None => VectorsState::NoVectorsFieldInDocument,
            }
        };

        Ok(Self { old, new })
    }

    pub fn remove(&mut self, embedder_name: &str) -> (VectorState, VectorState) {
        let old = self.old.remove(embedder_name).unwrap_or(VectorState::Generated);
        let state_from_old = match old {
            // assume a userProvided is still userProvided
            VectorState::Manual => VectorState::Manual,
            // generated is still generated
            VectorState::Generated => VectorState::Generated,
            // weird case that shouldn't happen were the previous docs version is inline,
            // but it was removed in the new version
            // Since it is not in the new version, we switch to generated
            VectorState::Inline(_) => VectorState::Generated,
        };
        let new = match &mut self.new {
            VectorsState::Vectors(new) => {
                new.remove(embedder_name).map(VectorState::Inline).unwrap_or(state_from_old)
            }
            _ =>
            // if no `_vectors` field is present in the new document,
            // the state depends on the previous version of the document
            {
                state_from_old
            }
        };

        (old, new)
    }
}

pub struct ParsedVectors(pub BTreeMap<String, Vectors>);

impl ParsedVectors {
    pub fn from_bytes(value: &[u8]) -> Result<Self, Error> {
        let Ok(value) = from_slice(value) else {
            let value = from_slice(value).map_err(Error::InternalSerdeJson)?;
            return Err(Error::InvalidMap(value));
        };
        Ok(ParsedVectors(value))
    }

    pub fn retain_not_embedded_vectors(&mut self, embedders: &BTreeSet<String>) {
        self.0.retain(|k, _v| !embedders.contains(k))
    }
}

pub enum Error {
    InvalidMap(Value),
    InternalSerdeJson(serde_json::Error),
}

impl Error {
    pub fn to_crate_error(self, document_id: String) -> crate::Error {
        match self {
            Error::InvalidMap(value) => {
                crate::Error::UserError(UserError::InvalidVectorsMapType { document_id, value })
            }
            Error::InternalSerdeJson(error) => {
                crate::Error::InternalError(InternalError::SerdeJson(error))
            }
        }
    }
}

fn to_vector_map(
    obkv: KvReaderDelAdd,
    side: DelAdd,
) -> Result<Option<BTreeMap<String, Vectors>>, Error> {
    Ok(if let Some(value) = obkv.get(side) {
        let ParsedVectors(parsed_vectors) = ParsedVectors::from_bytes(value)?;
        Some(parsed_vectors)
    } else {
        None
    })
}

/// Represents either a vector or an array of multiple vectors.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(transparent)]
pub struct VectorOrArrayOfVectors {
    #[serde(with = "either::serde_untagged_optional")]
    inner: Option<either::Either<Vec<Embedding>, Embedding>>,
}

impl VectorOrArrayOfVectors {
    pub fn into_array_of_vectors(self) -> Option<Vec<Embedding>> {
        match self.inner? {
            either::Either::Left(vectors) => Some(vectors),
            either::Either::Right(vector) => Some(vec![vector]),
        }
    }

    pub fn from_array_of_vectors(array_of_vec: Vec<Embedding>) -> Self {
        Self { inner: Some(either::Either::Left(array_of_vec)) }
    }

    pub fn from_vector(vec: Embedding) -> Self {
        Self { inner: Some(either::Either::Right(vec)) }
    }
}

impl From<Embedding> for VectorOrArrayOfVectors {
    fn from(vec: Embedding) -> Self {
        Self::from_vector(vec)
    }
}

impl From<Vec<Embedding>> for VectorOrArrayOfVectors {
    fn from(vec: Vec<Embedding>) -> Self {
        Self::from_array_of_vectors(vec)
    }
}

#[cfg(test)]
mod test {
    use super::VectorOrArrayOfVectors;

    #[test]
    fn array_of_vectors() {
        let null: VectorOrArrayOfVectors = serde_json::from_str("null").unwrap();
        let empty: VectorOrArrayOfVectors = serde_json::from_str("[]").unwrap();
        let one: VectorOrArrayOfVectors = serde_json::from_str("[0.1]").unwrap();
        let two: VectorOrArrayOfVectors = serde_json::from_str("[0.1, 0.2]").unwrap();
        let one_vec: VectorOrArrayOfVectors = serde_json::from_str("[[0.1, 0.2]]").unwrap();
        let two_vecs: VectorOrArrayOfVectors =
            serde_json::from_str("[[0.1, 0.2], [0.3, 0.4]]").unwrap();

        insta::assert_json_snapshot!(null.into_array_of_vectors(), @"null");
        insta::assert_json_snapshot!(empty.into_array_of_vectors(), @"[]");
        insta::assert_json_snapshot!(one.into_array_of_vectors(), @r###"
        [
          [
            0.1
          ]
        ]
        "###);
        insta::assert_json_snapshot!(two.into_array_of_vectors(), @r###"
        [
          [
            0.1,
            0.2
          ]
        ]
        "###);
        insta::assert_json_snapshot!(one_vec.into_array_of_vectors(), @r###"
        [
          [
            0.1,
            0.2
          ]
        ]
        "###);
        insta::assert_json_snapshot!(two_vecs.into_array_of_vectors(), @r###"
        [
          [
            0.1,
            0.2
          ],
          [
            0.3,
            0.4
          ]
        ]
        "###);
    }
}

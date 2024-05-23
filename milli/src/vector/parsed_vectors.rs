use std::collections::{BTreeMap, BTreeSet};

use obkv::KvReader;
use serde_json::{from_slice, Value};

use super::Embedding;
use crate::update::del_add::{DelAdd, KvReaderDelAdd};
use crate::{FieldId, InternalError, UserError};

pub const RESERVED_VECTORS_FIELD_NAME: &str = "_vectors";

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(untagged)]
pub enum Vectors {
    ImplicitlyUserProvided(VectorOrArrayOfVectors),
    Explicit(ExplicitVectors),
}

impl Vectors {
    pub fn into_array_of_vectors(self) -> Vec<Embedding> {
        match self {
            Vectors::ImplicitlyUserProvided(embeddings)
            | Vectors::Explicit(ExplicitVectors { embeddings, user_provided: _ }) => {
                embeddings.into_array_of_vectors().unwrap_or_default()
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExplicitVectors {
    pub embeddings: VectorOrArrayOfVectors,
    pub user_provided: bool,
}

pub struct ParsedVectorsDiff {
    pub old: Option<BTreeMap<String, Vectors>>,
    pub new: Option<BTreeMap<String, Vectors>>,
}

impl ParsedVectorsDiff {
    pub fn new(
        documents_diff: KvReader<'_, FieldId>,
        old_vectors_fid: Option<FieldId>,
        new_vectors_fid: Option<FieldId>,
    ) -> Result<Self, Error> {
        let old = match old_vectors_fid
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
        .flatten();
        let new = new_vectors_fid
            .and_then(|vectors_fid| documents_diff.get(vectors_fid))
            .map(KvReaderDelAdd::new)
            .map(|obkv| to_vector_map(obkv, DelAdd::Addition))
            .transpose()?
            .flatten();
        Ok(Self { old, new })
    }

    pub fn remove(&mut self, embedder_name: &str) -> (Option<Vectors>, Option<Vectors>) {
        let old = self.old.as_mut().and_then(|old| old.remove(embedder_name));
        let new = self.new.as_mut().and_then(|new| new.remove(embedder_name));
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

    pub fn retain_user_provided_vectors(&mut self, embedders: &BTreeSet<String>) {
        self.0.retain(|k, v| match v {
            Vectors::ImplicitlyUserProvided(_) => true,
            Vectors::Explicit(ExplicitVectors { embeddings: _, user_provided }) => {
                *user_provided
                // if the embedder is not in the config, then never touch it
                || !embedders.contains(k)
            }
        });
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

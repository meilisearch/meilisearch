use std::cell::RefCell;
use std::fmt::Debug;

use bumpalo::Bump;
use serde_json::Value;

use super::json_template::{self, JsonTemplate};
use super::Embedding;
use crate::prompt::error::RenderPromptError;
use crate::prompt::Prompt;
use crate::update::new::document::Document;
use crate::update::new::vector_document::VectorDocument;
use crate::{GlobalFieldsIdsMap, UserError};

pub trait Extractor<'a> {
    type Input: PartialEq;
    type Error;

    fn extract(
        &self,
        external_docid: &'a str,
        doc: &'a (impl Document<'a> + Debug),
        vector: Option<&'a impl VectorDocument<'a>>,
    ) -> Result<Option<Self::Input>, Self::Error>;

    fn diff_documents(
        &self,
        external_docid: &'a str,
        old: (&'a (impl Document<'a> + Debug), Option<&'a impl VectorDocument<'a>>),
        new: (&'a (impl Document<'a> + Debug), Option<&'a impl VectorDocument<'a>>),
    ) -> Result<ExtractorDiff<Self::Input>, Self::Error> {
        let old_input = self.extract(external_docid, old.0, old.1);
        let new_input = self.extract(external_docid, new.0, new.1);
        to_diff(old_input, new_input)
    }

    fn diff_settings(
        &self,
        external_docid: &'a str,
        doc: &'a (impl Document<'a> + Debug),
        vector: Option<&'a impl VectorDocument<'a>>,
        old: &Self,
    ) -> Result<ExtractorDiff<Self::Input>, Self::Error> {
        let old_input = old.extract(external_docid, doc, vector);
        let new_input = self.extract(external_docid, doc, vector);

        to_diff(old_input, new_input)
    }
}

fn to_diff<I: PartialEq, E>(
    old_input: Result<Option<I>, E>,
    new_input: Result<Option<I>, E>,
) -> Result<ExtractorDiff<I>, E> {
    let old_input = old_input.ok().unwrap_or(None);
    let new_input = new_input?;
    Ok(match (old_input, new_input) {
        (Some(old), Some(new)) if old == new => ExtractorDiff::Unchanged,
        (None, None) => ExtractorDiff::Unchanged,
        (None, Some(input)) => ExtractorDiff::Added(input),
        (Some(_), None) => ExtractorDiff::Removed,
        (Some(_), Some(input)) => ExtractorDiff::Updated(input),
    })
}

pub enum ExtractorDiff<Input> {
    Removed,
    Added(Input),
    Updated(Input),
    Unchanged,
}

pub struct ManualExtractor<'a> {
    key: &'a str,
    doc_alloc: &'a Bump,
}

impl<'a> Extractor<'a> for ManualExtractor<'a> {
    type Input = Vec<Embedding>;
    type Error = crate::Error;

    fn extract(
        &self,
        external_docid: &'a str,
        _doc: &'a (impl Document<'a> + Debug),
        vector: Option<&'a impl VectorDocument<'a>>,
    ) -> Result<Option<Self::Input>, Self::Error> {
        let Some(vector) = vector else { return Ok(None) };

        let Some(entry) = vector.vectors_for_key(self.key)? else { return Ok(None) };
        let Some(embeddings) = entry.embeddings else { return Ok(None) };

        Ok(Some(embeddings.into_vec(self.doc_alloc, self.key).map_err(|error| {
            UserError::InvalidVectorsEmbedderConf {
                document_id: external_docid.to_string(),
                error: error.to_string(),
            }
        })?))
    }
}

pub struct DocumentTemplateExtractor<'a> {
    template: &'a Prompt,
    doc_alloc: &'a Bump,
    field_id_map: &'a RefCell<GlobalFieldsIdsMap<'a>>,
}

impl<'a> Extractor<'a> for DocumentTemplateExtractor<'a> {
    type Input = &'a str;
    type Error = RenderPromptError;

    fn extract(
        &self,
        external_docid: &'a str,
        doc: &'a (impl Document<'a> + Debug),
        _vector: Option<&'a impl VectorDocument<'a>>,
    ) -> Result<Option<&'a str>, RenderPromptError> {
        Ok(Some(self.template.render_document(
            external_docid,
            doc,
            self.field_id_map,
            self.doc_alloc,
        )?))
    }
}

pub struct RequestFragmentExtractor<'a> {
    fragment: &'a JsonTemplate,
    doc_alloc: &'a Bump,
}

impl<'a> Extractor<'a> for RequestFragmentExtractor<'a> {
    type Input = Value;
    type Error = json_template::Error;

    fn extract(
        &self,
        _external_docid: &'a str,
        doc: &'a (impl Document<'a> + Debug),
        _vector: Option<&'a impl VectorDocument<'a>>,
    ) -> Result<Option<Self::Input>, Self::Error> {
        Ok(Some(self.fragment.render_document(doc, self.doc_alloc)?))
    }
}

// 1. Document ---Extractor--> Input
// 2. Input ---Embedding Request--> EmbeddingResponse
// 3. EmbeddingResponse ---Store in vector store---> EmbeddingLocation
// 4. EmbeddingLocation ---Memorize in embedding map---> EmbeddingMap

// EmbeddingMap (extractor_id, vector_id) -> doc_id
// EmbeddingMap::push_docid(extractor_id, vector_id, doc_id)
// EmbeddingMap::remove_docid(extractor_id, doc_id)
// needed: extractor_id, vector_id, doc_id

// EmbeddingLocation (extractor_id, vector_id, doc_id)
// EmbeddingLocation::push_embedding(doc_id, embedding) -> vector_id
// EmbeddingLocation::remove_embedding(doc_id, vector_id)
// needed: extractor_id, doc_id, vector_id

// EmbeddingResponse (extractor_id, doc_id, embedding)

// EmbeddingRequest (extractor_id, doc_id, input)
// EmbeddingRequest::embed(extractor_id, doc_id, input) -> EmbeddingResponse

// Embedder::embed_index -> EmbeddingRequests

// EmbeddingRequests::embed_text(extractor_id, doc_id, text)
// EmbeddingRequests::embed_fragment(extractor_id, doc_id, fragment)

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Debug;

use bumpalo::Bump;
use serde_json::Value;

use super::json_template::{self, JsonTemplate};
use crate::prompt::error::RenderPromptError;
use crate::prompt::Prompt;
use crate::update::new::document::Document;
use crate::vector::RuntimeFragment;
use crate::GlobalFieldsIdsMap;

/// Trait for types that extract embedder inputs from a document.
///
/// An embedder input can then be sent to an embedder by using an [`super::session::EmbedSession`].
pub trait Extractor<'doc> {
    /// The embedder input that is extracted from documents by this extractor.
    ///
    /// The inputs have to be comparable for equality so that diffing is possible.
    type Input: PartialEq;

    /// The error that can happen while extracting from a document.
    type Error;

    /// Metadata associated with a document.
    type DocumentMetadata;

    /// Extract the embedder input from a document and its metadata.
    fn extract<'a, D: Document<'a> + Debug>(
        &self,
        doc: D,
        meta: &Self::DocumentMetadata,
    ) -> Result<Option<Self::Input>, Self::Error>;

    /// Unique `id` associated with this extractor.
    ///
    /// This will serve to decide where to store the vectors in the vector store.
    /// The id should be stable for a given extractor.
    fn extractor_id(&self) -> u8;

    /// The result of diffing the embedder inputs extracted from two versions of a document.
    ///
    /// # Parameters
    ///
    /// - `old`: old version of the document
    /// - `new`: new version of the document
    /// - `meta`: metadata associated to the document
    fn diff_documents<'a, OD: Document<'a> + Debug, ND: Document<'a> + Debug>(
        &self,
        old: OD,
        new: ND,
        meta: &Self::DocumentMetadata,
    ) -> Result<ExtractorDiff<Self::Input>, Self::Error>
    where
        'doc: 'a,
    {
        let old_input = self.extract(old, meta);
        let new_input = self.extract(new, meta);
        to_diff(old_input, new_input)
    }

    /// The result of diffing the embedder inputs extracted from a document by two versions of this extractor.
    ///
    /// # Parameters
    ///
    /// - `doc`: the document from which to extract the embedder inputs
    /// - `meta`: metadata associated to the document
    /// - `old`: If `Some`, the old version of this extractor. If `None`, this is equivalent to calling `ExtractorDiff::Added(self.extract(_))`.
    fn diff_settings<'a, D: Document<'a> + Debug>(
        &self,
        doc: D,
        meta: &Self::DocumentMetadata,
        old: Option<&Self>,
    ) -> Result<ExtractorDiff<Self::Input>, Self::Error> {
        let old_input = if let Some(old) = old { old.extract(&doc, meta) } else { Ok(None) };
        let new_input = self.extract(&doc, meta);

        to_diff(old_input, new_input)
    }

    /// Returns an extractor wrapping `self` and set to ignore all errors arising from extracting with this extractor.
    fn ignore_errors(self) -> IgnoreErrorExtractor<Self>
    where
        Self: Sized,
    {
        IgnoreErrorExtractor(self)
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

impl<Input> ExtractorDiff<Input> {
    pub fn into_input(self) -> Option<Input> {
        match self {
            ExtractorDiff::Removed => None,
            ExtractorDiff::Added(input) => Some(input),
            ExtractorDiff::Updated(input) => Some(input),
            ExtractorDiff::Unchanged => None,
        }
    }

    pub fn needs_change(&self) -> bool {
        match self {
            ExtractorDiff::Removed => true,
            ExtractorDiff::Added(_) => true,
            ExtractorDiff::Updated(_) => true,
            ExtractorDiff::Unchanged => false,
        }
    }

    pub fn into_list_of_changes(
        named_diffs: impl IntoIterator<Item = (String, Self)>,
    ) -> BTreeMap<String, Option<Input>> {
        named_diffs
            .into_iter()
            .filter(|(_, diff)| diff.needs_change())
            .map(|(name, diff)| (name, diff.into_input()))
            .collect()
    }
}

pub struct DocumentTemplateExtractor<'a, 'b, 'c> {
    doc_alloc: &'a Bump,
    field_id_map: &'a RefCell<GlobalFieldsIdsMap<'b>>,
    template: &'c Prompt,
}

impl<'a, 'b, 'c> DocumentTemplateExtractor<'a, 'b, 'c> {
    pub fn new(
        template: &'c Prompt,
        doc_alloc: &'a Bump,
        field_id_map: &'a RefCell<GlobalFieldsIdsMap<'b>>,
    ) -> Self {
        Self { template, doc_alloc, field_id_map }
    }
}

impl<'doc> Extractor<'doc> for DocumentTemplateExtractor<'doc, '_, '_> {
    type DocumentMetadata = &'doc str;
    type Input = &'doc str;
    type Error = RenderPromptError;

    fn extractor_id(&self) -> u8 {
        0
    }

    fn extract<'a, D: Document<'a> + Debug>(
        &self,
        doc: D,
        external_docid: &Self::DocumentMetadata,
    ) -> Result<Option<Self::Input>, Self::Error> {
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
    extractor_id: u8,
    doc_alloc: &'a Bump,
}

impl<'a> RequestFragmentExtractor<'a> {
    pub fn new(fragment: &'a RuntimeFragment, doc_alloc: &'a Bump) -> Self {
        Self { fragment: &fragment.template, extractor_id: fragment.id, doc_alloc }
    }
}

impl<'doc> Extractor<'doc> for RequestFragmentExtractor<'doc> {
    type DocumentMetadata = ();
    type Input = Value;
    type Error = json_template::Error;

    fn extractor_id(&self) -> u8 {
        self.extractor_id
    }

    fn extract<'a, D: Document<'a> + Debug>(
        &self,
        doc: D,
        _meta: &Self::DocumentMetadata,
    ) -> Result<Option<Self::Input>, Self::Error> {
        Ok(Some(self.fragment.render_document(doc, self.doc_alloc)?))
    }
}

/// Extractor that fetches URLs and includes them as virtual fields during fragment rendering.
pub struct UrlFetchingFragmentExtractor<'a> {
    fragment: &'a JsonTemplate,
    extractor_id: u8,
    doc_alloc: &'a Bump,
    url_fetcher: Option<&'a crate::vector::url_fetcher::UrlFetcher>,
    fetch_mappings: &'a [crate::vector::url_fetcher::ResolvedFetchMapping],
}

impl<'a> UrlFetchingFragmentExtractor<'a> {
    pub fn new(
        fragment: &'a super::RuntimeFragment,
        doc_alloc: &'a Bump,
        url_fetcher: Option<&'a crate::vector::url_fetcher::UrlFetcher>,
        fetch_mappings: &'a [crate::vector::url_fetcher::ResolvedFetchMapping],
    ) -> Self {
        Self {
            fragment: &fragment.template,
            extractor_id: fragment.id,
            doc_alloc,
            url_fetcher,
            fetch_mappings,
        }
    }

    /// Extract URLs from document, fetch them, and return virtual fields.
    ///
    /// Supports nested paths like:
    /// - `imageUrl` - simple top-level field
    /// - `media.image.url` - nested field
    /// - `images[0].url` - array index
    /// - `images[].url` - array wildcard (fetches all URLs, outputs as `output_0`, `output_1`, etc.)
    fn fetch_virtual_fields<'d, D: Document<'d> + Debug>(
        &self,
        doc: &D,
    ) -> std::collections::BTreeMap<String, String> {
        let mut virtual_fields = std::collections::BTreeMap::new();

        let Some(url_fetcher) = self.url_fetcher else {
            return virtual_fields;
        };

        if self.fetch_mappings.is_empty() {
            return virtual_fields;
        }

        // Build a JSON object from the document's top-level fields
        let doc_json = self.build_document_json(doc);

        for mapping in self.fetch_mappings {
            // Extract URLs using the path (supports nested paths and array wildcards)
            let urls = crate::vector::url_fetcher::extract_urls(&doc_json, &mapping.input);

            if urls.is_empty() {
                continue;
            }

            // Check if this is an array wildcard path (produces multiple outputs)
            let is_array_wildcard =
                crate::vector::url_fetcher::path_has_array_wildcard(&mapping.input);

            if is_array_wildcard && urls.len() > 1 {
                // Multiple URLs - create indexed outputs: output_0, output_1, etc.
                for (idx, url) in urls.iter().enumerate() {
                    if url.is_empty() {
                        continue;
                    }
                    match url_fetcher.fetch_as_base64(url, mapping) {
                        Ok(base64_content) => {
                            let output_name = format!("{}_{}", mapping.output, idx);
                            virtual_fields.insert(output_name, base64_content);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to fetch URL '{}' for field '{}[{}]': {}",
                                url,
                                mapping.input,
                                idx,
                                e
                            );
                        }
                    }
                }
            } else {
                // Single URL (or first URL if somehow multiple without wildcard)
                let url = &urls[0];
                if !url.is_empty() {
                    match url_fetcher.fetch_as_base64(url, mapping) {
                        Ok(base64_content) => {
                            virtual_fields.insert(mapping.output.clone(), base64_content);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to fetch URL '{}' for field '{}': {}",
                                url,
                                mapping.input,
                                e
                            );
                        }
                    }
                }
            }
        }

        virtual_fields
    }

    /// Build a JSON object from the document's top-level fields.
    fn build_document_json<'d, D: Document<'d> + Debug>(
        &self,
        doc: &D,
    ) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        for (field_name, raw_value) in doc.iter_top_level_fields().flatten() {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(raw_value.get()) {
                obj.insert(field_name.to_string(), value);
            }
        }

        serde_json::Value::Object(obj)
    }
}

impl<'doc> Extractor<'doc> for UrlFetchingFragmentExtractor<'doc> {
    type DocumentMetadata = ();
    type Input = Value;
    type Error = json_template::Error;

    fn extractor_id(&self) -> u8 {
        self.extractor_id
    }

    fn extract<'a, D: Document<'a> + Debug>(
        &self,
        doc: D,
        _meta: &Self::DocumentMetadata,
    ) -> Result<Option<Self::Input>, Self::Error> {
        // Fetch URLs and get virtual fields
        let virtual_fields = self.fetch_virtual_fields(&doc);

        // Render the fragment with virtual fields
        let virtual_fields_ref =
            if virtual_fields.is_empty() { None } else { Some(&virtual_fields) };

        Ok(Some(self.fragment.render_document_with_virtual_fields(
            doc,
            self.doc_alloc,
            virtual_fields_ref,
        )?))
    }
}

pub struct IgnoreErrorExtractor<E>(E);

impl<'doc, E> Extractor<'doc> for IgnoreErrorExtractor<E>
where
    E: Extractor<'doc>,
{
    type DocumentMetadata = E::DocumentMetadata;
    type Input = E::Input;

    type Error = Infallible;

    fn extractor_id(&self) -> u8 {
        self.0.extractor_id()
    }

    fn extract<'a, D: Document<'a> + Debug>(
        &self,
        doc: D,
        meta: &Self::DocumentMetadata,
    ) -> Result<Option<Self::Input>, Self::Error> {
        Ok(self.0.extract(doc, meta).ok().flatten())
    }
}

#[derive(Debug)]
pub enum Infallible {}

impl From<Infallible> for crate::Error {
    fn from(_: Infallible) -> Self {
        unreachable!("Infallible values cannot be built")
    }
}

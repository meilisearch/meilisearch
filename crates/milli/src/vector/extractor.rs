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

pub trait Extractor<'doc> {
    type DocumentMetadata;
    type Input: PartialEq;
    type Error;

    fn extract<'a, D: Document<'a> + Debug>(
        &self,
        doc: D,
        meta: &Self::DocumentMetadata,
    ) -> Result<Option<Self::Input>, Self::Error>;

    fn extractor_id(&self) -> u8;

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

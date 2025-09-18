use std::cell::RefCell;
use std::collections::BTreeMap;

use bumpalo::Bump;
use hashbrown::HashMap;

use super::DelAddRoaringBitmap;
use crate::constants::{RESERVED_GEOJSON_FIELD_NAME, RESERVED_GEO_FIELD_NAME};
use crate::update::new::channel::{DocumentsSender, ExtractorBbqueueSender};
use crate::update::new::document::{write_to_obkv, Document, DocumentContext, DocumentIdentifiers};
use crate::update::new::indexer::document_changes::{Extractor, IndexingContext};
use crate::update::new::indexer::settings_changes::{
    settings_change_extract, DocumentsIndentifiers, SettingsChangeExtractor,
};
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::thread_local::{FullySend, ThreadLocal};
use crate::update::new::vector_document::VectorDocument;
use crate::update::new::DocumentChange;
use crate::update::settings::SettingsDelta;
use crate::vector::settings::EmbedderAction;
use crate::vector::RuntimeEmbedders;
use crate::Result;

pub struct DocumentsExtractor<'a, 'b> {
    document_sender: DocumentsSender<'a, 'b>,
    embedders: &'a RuntimeEmbedders,
}

impl<'a, 'b> DocumentsExtractor<'a, 'b> {
    pub fn new(document_sender: DocumentsSender<'a, 'b>, embedders: &'a RuntimeEmbedders) -> Self {
        Self { document_sender, embedders }
    }
}

#[derive(Default)]
pub struct DocumentExtractorData {
    pub docids_delta: DelAddRoaringBitmap,
    pub field_distribution_delta: HashMap<String, i64>,
}

impl<'extractor> Extractor<'extractor> for DocumentsExtractor<'_, '_> {
    type Data = FullySend<RefCell<DocumentExtractorData>>;

    fn init_data(&self, _extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(FullySend(Default::default()))
    }

    fn process<'doc>(
        &self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &DocumentContext<Self::Data>,
    ) -> Result<()> {
        let mut document_buffer = bumpalo::collections::Vec::new_in(&context.doc_alloc);
        let mut document_extractor_data = context.data.0.borrow_mut_or_yield();
        let embedder_actions = &Default::default();

        for change in changes {
            let change = change?;
            // **WARNING**: the exclusive borrow on `new_fields_ids_map` needs to be taken **inside** of the `for change in changes` loop
            // Otherwise, `BorrowMutError` will occur for document changes that also need the new_fields_ids_map (e.g.: UpdateByFunction)
            let mut new_fields_ids_map = context.new_fields_ids_map.borrow_mut_or_yield();

            let external_docid = change.external_docid().to_owned();

            // document but we need to create a function that collects and compresses documents.
            match change {
                DocumentChange::Deletion(deletion) => {
                    let docid = deletion.docid();
                    let content = deletion.current(
                        &context.rtxn,
                        context.index,
                        &context.db_fields_ids_map,
                    )?;
                    let geo_iter = content
                        .geo_field()
                        .transpose()
                        .map(|res| res.map(|rv| (RESERVED_GEO_FIELD_NAME, rv)));
                    let geojson_iter = content
                        .geojson_field()
                        .transpose()
                        .map(|res| res.map(|rv| (RESERVED_GEOJSON_FIELD_NAME, rv)));
                    for res in content.iter_top_level_fields().chain(geo_iter).chain(geojson_iter) {
                        let (f, _) = res?;
                        let entry = document_extractor_data
                            .field_distribution_delta
                            .entry_ref(f)
                            .or_default();
                        *entry -= 1;
                    }
                    document_extractor_data.docids_delta.insert_del_u32(docid);
                    self.document_sender.delete(docid, external_docid).unwrap();
                }
                DocumentChange::Update(update) => {
                    let docid = update.docid();
                    let content =
                        update.current(&context.rtxn, context.index, &context.db_fields_ids_map)?;
                    let geo_iter = content
                        .geo_field()
                        .transpose()
                        .map(|res| res.map(|rv| (RESERVED_GEO_FIELD_NAME, rv)));
                    let geojson_iter = content
                        .geojson_field()
                        .transpose()
                        .map(|res| res.map(|rv| (RESERVED_GEOJSON_FIELD_NAME, rv)));
                    for res in content.iter_top_level_fields().chain(geo_iter).chain(geojson_iter) {
                        let (f, _) = res?;
                        let entry = document_extractor_data
                            .field_distribution_delta
                            .entry_ref(f)
                            .or_default();
                        *entry -= 1;
                    }
                    let content =
                        update.merged(&context.rtxn, context.index, &context.db_fields_ids_map)?;
                    let geo_iter = content
                        .geo_field()
                        .transpose()
                        .map(|res| res.map(|rv| (RESERVED_GEO_FIELD_NAME, rv)));
                    let geojson_iter = content
                        .geojson_field()
                        .transpose()
                        .map(|res| res.map(|rv| (RESERVED_GEOJSON_FIELD_NAME, rv)));
                    for res in content.iter_top_level_fields().chain(geo_iter).chain(geojson_iter) {
                        let (f, _) = res?;
                        let entry = document_extractor_data
                            .field_distribution_delta
                            .entry_ref(f)
                            .or_default();
                        *entry += 1;
                    }

                    let content =
                        update.merged(&context.rtxn, context.index, &context.db_fields_ids_map)?;
                    let vector_content = update.merged_vectors(
                        &context.rtxn,
                        context.index,
                        &context.db_fields_ids_map,
                        &context.doc_alloc,
                        self.embedders,
                    )?;
                    let content = write_to_obkv(
                        &content,
                        vector_content.as_ref(),
                        embedder_actions,
                        &mut new_fields_ids_map,
                        &mut document_buffer,
                    )?;

                    self.document_sender.uncompressed(docid, external_docid, content).unwrap();
                }
                DocumentChange::Insertion(insertion) => {
                    let docid = insertion.docid();
                    let content = insertion.inserted();
                    let geo_iter = content
                        .geo_field()
                        .transpose()
                        .map(|res| res.map(|rv| (RESERVED_GEO_FIELD_NAME, rv)));
                    let geojson_iter = content
                        .geojson_field()
                        .transpose()
                        .map(|res| res.map(|rv| (RESERVED_GEOJSON_FIELD_NAME, rv)));
                    for res in content.iter_top_level_fields().chain(geo_iter).chain(geojson_iter) {
                        let (f, _) = res?;
                        let entry = document_extractor_data
                            .field_distribution_delta
                            .entry_ref(f)
                            .or_default();
                        *entry += 1;
                    }
                    let inserted_vectors =
                        insertion.inserted_vectors(&context.doc_alloc, self.embedders)?;
                    let content = write_to_obkv(
                        &content,
                        inserted_vectors.as_ref(),
                        embedder_actions,
                        &mut new_fields_ids_map,
                        &mut document_buffer,
                    )?;
                    document_extractor_data.docids_delta.insert_add_u32(docid);
                    self.document_sender.uncompressed(docid, external_docid, content).unwrap();
                }
            }
        }

        Ok(())
    }
}

pub struct SettingsChangeDocumentExtractor<'a, 'b> {
    document_sender: DocumentsSender<'a, 'b>,
    embedder_actions: &'a BTreeMap<String, EmbedderAction>,
}

impl<'a, 'b> SettingsChangeDocumentExtractor<'a, 'b> {
    pub fn new(
        document_sender: DocumentsSender<'a, 'b>,
        embedder_actions: &'a BTreeMap<String, EmbedderAction>,
    ) -> Self {
        Self { document_sender, embedder_actions }
    }
}

impl<'extractor> SettingsChangeExtractor<'extractor> for SettingsChangeDocumentExtractor<'_, '_> {
    type Data = FullySend<()>;

    fn init_data(&self, _extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(FullySend(()))
    }

    fn process<'doc>(
        &self,
        documents: impl Iterator<Item = Result<DocumentIdentifiers<'doc>>>,
        context: &DocumentContext<Self::Data>,
    ) -> Result<()> {
        let mut document_buffer = bumpalo::collections::Vec::new_in(&context.doc_alloc);

        for document in documents {
            let document = document?;
            // **WARNING**: the exclusive borrow on `new_fields_ids_map` needs to be taken **inside** of the `for change in changes` loop
            // Otherwise, `BorrowMutError` will occur for document changes that also need the new_fields_ids_map (e.g.: UpdateByFunction)
            let mut new_fields_ids_map = context.new_fields_ids_map.borrow_mut_or_yield();

            let external_docid = document.external_document_id().to_owned();
            let content =
                document.current(&context.rtxn, context.index, &context.db_fields_ids_map)?;
            let vector_content = document.current_vectors(
                &context.rtxn,
                context.index,
                &context.db_fields_ids_map,
                &context.doc_alloc,
            )?;

            // if the document doesn't need to be updated, we skip it
            if !must_update_document(&vector_content, self.embedder_actions)? {
                continue;
            }

            let content = write_to_obkv(
                &content,
                Some(&vector_content),
                self.embedder_actions,
                &mut new_fields_ids_map,
                &mut document_buffer,
            )?;

            self.document_sender.uncompressed(document.docid(), external_docid, content).unwrap();
        }

        Ok(())
    }
}

/// Modify the database documents based on the settings changes.
///
/// This function extracts the documents from the database,
/// modifies them by adding or removing vector fields based on embedder actions,
/// and then updates the database.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::documents::extract")]
pub fn update_database_documents<'indexer, MSP, SD>(
    documents: &'indexer DocumentsIndentifiers<'indexer>,
    indexing_context: IndexingContext<MSP>,
    extractor_sender: &ExtractorBbqueueSender,
    settings_delta: &SD,
    extractor_allocs: &mut ThreadLocal<FullySend<Bump>>,
) -> Result<()>
where
    MSP: Fn() -> bool + Sync,
    SD: SettingsDelta,
{
    if !must_update_database(settings_delta) {
        return Ok(());
    }

    let document_sender = extractor_sender.documents();
    let document_extractor =
        SettingsChangeDocumentExtractor::new(document_sender, settings_delta.embedder_actions());
    let datastore = ThreadLocal::with_capacity(rayon::current_num_threads());

    settings_change_extract(
        documents,
        &document_extractor,
        indexing_context,
        extractor_allocs,
        &datastore,
        crate::update::new::steps::IndexingStep::ExtractingDocuments,
    )?;

    Ok(())
}

fn must_update_database<SD: SettingsDelta>(settings_delta: &SD) -> bool {
    settings_delta.embedder_actions().iter().any(|(name, action)| {
        if action.reindex().is_some() {
            // if action has a reindex, we need to update the documents database if the embedder is a new one
            settings_delta.old_embedders().get(name).is_none()
        } else {
            // if action has a write_back, we need to update the documents database
            action.write_back().is_some()
        }
    })
}

fn must_update_document<'s, 'a>(
    vector_document: &'s impl VectorDocument<'s>,
    embedder_actions: &'a BTreeMap<String, EmbedderAction>,
) -> Result<bool>
where
    's: 'a,
{
    // Check if any vector needs to be written back for the document
    for (name, action) in embedder_actions {
        // if the vector entry is not found, we don't need to update the document
        let Some(vector_entry) = vector_document.vectors_for_key(name)? else {
            continue;
        };

        // if the vector entry is user provided, we need to update the document by writing back vectors.
        let write_back = action.write_back().is_some() && !vector_entry.regenerate;
        // if the vector entry is a new embedder, we need to update the document removing the vectors from the document.
        let new_embedder = action.reindex().is_some() && !vector_entry.has_configured_embedder;

        if write_back || new_embedder {
            return Ok(true);
        }
    }

    Ok(false)
}

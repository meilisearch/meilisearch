use std::borrow::Cow;
use std::collections::btree_map::Entry as BEntry;
use std::collections::hash_map::Entry as HEntry;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek};

use fxhash::FxHashMap;
use itertools::Itertools;
use obkv::{KvReader, KvReaderU16, KvWriter};
use roaring::RoaringBitmap;
use serde_json::Value;
use smartstring::SmartString;

use super::helpers::{
    create_sorter, create_writer, keep_first, obkvs_keep_last_addition_merge_deletions,
    obkvs_merge_additions_and_deletions, sorter_into_reader, MergeFn,
};
use super::{IndexDocumentsMethod, IndexerConfig};
use crate::documents::{DocumentsBatchIndex, EnrichedDocument, EnrichedDocumentsBatchReader};
use crate::error::{Error, InternalError, UserError};
use crate::index::{db_name, main_key};
use crate::update::del_add::{
    del_add_from_two_obkvs, into_del_add_obkv, DelAdd, DelAddOperation, KvReaderDelAdd,
};
use crate::update::index_documents::GrenadParameters;
use crate::update::settings::{InnerIndexSettings, InnerIndexSettingsDiff};
use crate::update::{AvailableDocumentsIds, UpdateIndexingStep};
use crate::{FieldDistribution, FieldId, FieldIdMapMissingEntry, FieldsIdsMap, Index, Result};

pub struct TransformOutput {
    pub primary_key: String,
    pub settings_diff: InnerIndexSettingsDiff,
    pub field_distribution: FieldDistribution,
    pub documents_count: usize,
    pub original_documents: File,
    pub flattened_documents: File,
}

/// Extract the external ids, deduplicate and compute the new internal documents ids
/// and fields ids, writing all the documents under their internal ids into a final file.
///
/// Outputs the new `FieldsIdsMap`, the new `UsersIdsDocumentsIds` map, the new documents ids,
/// the replaced documents ids, the number of documents in this update and the file
/// containing all those documents.
pub struct Transform<'a, 'i> {
    pub index: &'i Index,
    fields_ids_map: FieldsIdsMap,

    indexer_settings: &'a IndexerConfig,
    pub autogenerate_docids: bool,
    pub index_documents_method: IndexDocumentsMethod,
    available_documents_ids: AvailableDocumentsIds,

    // Both grenad follows the same format:
    // key | value
    // u32 | 1 byte for the Operation byte, the rest is the obkv of the document stored
    original_sorter: grenad::Sorter<MergeFn>,
    flattened_sorter: grenad::Sorter<MergeFn>,

    replaced_documents_ids: RoaringBitmap,
    new_documents_ids: RoaringBitmap,
    // To increase the cache locality and decrease the heap usage we use compact smartstring.
    new_external_documents_ids_builder: FxHashMap<SmartString<smartstring::Compact>, u64>,
    documents_count: usize,
}

/// This enum is specific to the grenad sorter stored in the transform.
/// It's used as the first byte of the grenads and tells you if the document id was an addition or a deletion.
#[repr(u8)]
pub enum Operation {
    Addition,
    Deletion,
}

/// Create a mapping between the field ids found in the document batch and the one that were
/// already present in the index.
///
/// If new fields are present in the addition, they are added to the index field ids map.
fn create_fields_mapping(
    index_field_map: &mut FieldsIdsMap,
    batch_field_map: &DocumentsBatchIndex,
) -> Result<HashMap<FieldId, FieldId>> {
    batch_field_map
        .iter()
        // we sort by id here to ensure a deterministic mapping of the fields, that preserves
        // the original ordering.
        .sorted_by_key(|(&id, _)| id)
        .map(|(field, name)| match index_field_map.id(name) {
            Some(id) => Ok((*field, id)),
            None => index_field_map
                .insert(name)
                .ok_or(Error::UserError(UserError::AttributeLimitReached))
                .map(|id| (*field, id)),
        })
        .collect()
}

impl<'a, 'i> Transform<'a, 'i> {
    pub fn new(
        wtxn: &mut heed::RwTxn,
        index: &'i Index,
        indexer_settings: &'a IndexerConfig,
        index_documents_method: IndexDocumentsMethod,
        autogenerate_docids: bool,
    ) -> Result<Self> {
        // We must choose the appropriate merge function for when two or more documents
        // with the same user id must be merged or fully replaced in the same batch.
        let merge_function = match index_documents_method {
            IndexDocumentsMethod::ReplaceDocuments => obkvs_keep_last_addition_merge_deletions,
            IndexDocumentsMethod::UpdateDocuments => obkvs_merge_additions_and_deletions,
        };

        // We initialize the sorter with the user indexing settings.
        let original_sorter = create_sorter(
            grenad::SortAlgorithm::Stable,
            merge_function,
            indexer_settings.chunk_compression_type,
            indexer_settings.chunk_compression_level,
            indexer_settings.max_nb_chunks,
            indexer_settings.max_memory.map(|mem| mem / 2),
        );

        // We initialize the sorter with the user indexing settings.
        let flattened_sorter = create_sorter(
            grenad::SortAlgorithm::Stable,
            merge_function,
            indexer_settings.chunk_compression_type,
            indexer_settings.chunk_compression_level,
            indexer_settings.max_nb_chunks,
            indexer_settings.max_memory.map(|mem| mem / 2),
        );
        let documents_ids = index.documents_ids(wtxn)?;

        Ok(Transform {
            index,
            fields_ids_map: index.fields_ids_map(wtxn)?,
            indexer_settings,
            autogenerate_docids,
            available_documents_ids: AvailableDocumentsIds::from_documents_ids(&documents_ids),
            original_sorter,
            flattened_sorter,
            index_documents_method,
            replaced_documents_ids: RoaringBitmap::new(),
            new_documents_ids: RoaringBitmap::new(),
            new_external_documents_ids_builder: FxHashMap::default(),
            documents_count: 0,
        })
    }

    #[tracing::instrument(level = "trace", skip_all, target = "indexing::documents")]
    pub fn read_documents<R, FP, FA>(
        &mut self,
        reader: EnrichedDocumentsBatchReader<R>,
        wtxn: &mut heed::RwTxn,
        progress_callback: FP,
        should_abort: FA,
    ) -> Result<usize>
    where
        R: Read + Seek,
        FP: Fn(UpdateIndexingStep) + Sync,
        FA: Fn() -> bool + Sync,
    {
        puffin::profile_function!();

        let (mut cursor, fields_index) = reader.into_cursor_and_fields_index();
        let external_documents_ids = self.index.external_documents_ids();
        let mapping = create_fields_mapping(&mut self.fields_ids_map, &fields_index)?;

        let primary_key = cursor.primary_key().to_string();
        let primary_key_id =
            self.fields_ids_map.insert(&primary_key).ok_or(UserError::AttributeLimitReached)?;

        let mut obkv_buffer = Vec::new();
        let mut document_sorter_value_buffer = Vec::new();
        let mut document_sorter_key_buffer = Vec::new();
        let mut documents_count = 0;
        let mut docid_buffer: Vec<u8> = Vec::new();
        let mut field_buffer: Vec<(u16, Cow<[u8]>)> = Vec::new();
        while let Some(enriched_document) = cursor.next_enriched_document()? {
            let EnrichedDocument { document, document_id } = enriched_document;

            if should_abort() {
                return Err(Error::InternalError(InternalError::AbortedIndexation));
            }

            // drop_and_reuse is called instead of .clear() to communicate to the compiler that field_buffer
            // does not keep references from the cursor between loop iterations
            let mut field_buffer_cache = drop_and_reuse(field_buffer);
            if self.indexer_settings.log_every_n.map_or(false, |len| documents_count % len == 0) {
                progress_callback(UpdateIndexingStep::RemapDocumentAddition {
                    documents_seen: documents_count,
                });
            }

            // When the document id has been auto-generated by the `enrich_documents_batch`
            // we must insert this document id into the remaped document.
            let external_id = document_id.value();
            if document_id.is_generated() {
                serde_json::to_writer(&mut docid_buffer, external_id)
                    .map_err(InternalError::SerdeJson)?;
                field_buffer_cache.push((primary_key_id, Cow::from(&docid_buffer)));
            }

            for (k, v) in document.iter() {
                let mapped_id =
                    *mapping.get(&k).ok_or(InternalError::FieldIdMappingMissingEntry { key: k })?;
                field_buffer_cache.push((mapped_id, Cow::from(v)));
            }

            // Insertion in a obkv need to be done with keys ordered. For now they are ordered
            // according to the document addition key order, so we sort it according to the
            // fieldids map keys order.
            field_buffer_cache.sort_unstable_by(|(f1, _), (f2, _)| f1.cmp(f2));

            // Build the new obkv document.
            let mut writer = KvWriter::new(&mut obkv_buffer);
            for (k, v) in field_buffer_cache.iter() {
                writer.insert(*k, v)?;
            }

            let mut original_docid = None;
            let docid = match self.new_external_documents_ids_builder.entry((*external_id).into()) {
                HEntry::Occupied(entry) => *entry.get() as u32,
                HEntry::Vacant(entry) => {
                    let docid = match external_documents_ids.get(wtxn, entry.key())? {
                        Some(docid) => {
                            // If it was already in the list of replaced documents it means it was deleted
                            // by the remove_document method. We should starts as if it never existed.
                            if self.replaced_documents_ids.insert(docid) {
                                original_docid = Some(docid);
                            }

                            docid
                        }
                        None => self
                            .available_documents_ids
                            .next()
                            .ok_or(UserError::DocumentLimitReached)?,
                    };
                    entry.insert(docid as u64);
                    docid
                }
            };

            let mut skip_insertion = false;
            if let Some(original_docid) = original_docid {
                let original_key = original_docid;
                let base_obkv = self
                    .index
                    .documents
                    .remap_data_type::<heed::types::Bytes>()
                    .get(wtxn, &original_key)?
                    .ok_or(InternalError::DatabaseMissingEntry {
                        db_name: db_name::DOCUMENTS,
                        key: None,
                    })?;

                // we check if the two documents are exactly equal. If it's the case we can skip this document entirely
                if base_obkv == obkv_buffer {
                    // we're not replacing anything
                    self.replaced_documents_ids.remove(original_docid);
                    // and we need to put back the original id as it was before
                    self.new_external_documents_ids_builder.remove(external_id);
                    skip_insertion = true;
                } else {
                    // we associate the base document with the new key, everything will get merged later.
                    let deladd_operation = match self.index_documents_method {
                        IndexDocumentsMethod::UpdateDocuments => {
                            DelAddOperation::DeletionAndAddition
                        }
                        IndexDocumentsMethod::ReplaceDocuments => DelAddOperation::Deletion,
                    };
                    document_sorter_key_buffer.clear();
                    document_sorter_key_buffer.extend_from_slice(&docid.to_be_bytes());
                    document_sorter_key_buffer.extend_from_slice(external_id.as_bytes());
                    document_sorter_value_buffer.clear();
                    document_sorter_value_buffer.push(Operation::Addition as u8);
                    into_del_add_obkv(
                        KvReaderU16::new(base_obkv),
                        deladd_operation,
                        &mut document_sorter_value_buffer,
                    )?;
                    self.original_sorter
                        .insert(&document_sorter_key_buffer, &document_sorter_value_buffer)?;
                    let base_obkv = KvReader::new(base_obkv);
                    if let Some(flattened_obkv) =
                        Self::flatten_from_fields_ids_map(&base_obkv, &mut self.fields_ids_map)?
                    {
                        // we recreate our buffer with the flattened documents
                        document_sorter_value_buffer.clear();
                        document_sorter_value_buffer.push(Operation::Addition as u8);
                        into_del_add_obkv(
                            KvReaderU16::new(&flattened_obkv),
                            deladd_operation,
                            &mut document_sorter_value_buffer,
                        )?;
                    }
                    self.flattened_sorter
                        .insert(docid.to_be_bytes(), &document_sorter_value_buffer)?;
                }
            }

            if !skip_insertion {
                self.new_documents_ids.insert(docid);

                document_sorter_key_buffer.clear();
                document_sorter_key_buffer.extend_from_slice(&docid.to_be_bytes());
                document_sorter_key_buffer.extend_from_slice(external_id.as_bytes());
                document_sorter_value_buffer.clear();
                document_sorter_value_buffer.push(Operation::Addition as u8);
                into_del_add_obkv(
                    KvReaderU16::new(&obkv_buffer),
                    DelAddOperation::Addition,
                    &mut document_sorter_value_buffer,
                )?;
                // We use the extracted/generated user id as the key for this document.
                self.original_sorter
                    .insert(&document_sorter_key_buffer, &document_sorter_value_buffer)?;

                let flattened_obkv = KvReader::new(&obkv_buffer);
                if let Some(obkv) =
                    Self::flatten_from_fields_ids_map(&flattened_obkv, &mut self.fields_ids_map)?
                {
                    document_sorter_value_buffer.clear();
                    document_sorter_value_buffer.push(Operation::Addition as u8);
                    into_del_add_obkv(
                        KvReaderU16::new(&obkv),
                        DelAddOperation::Addition,
                        &mut document_sorter_value_buffer,
                    )?
                }
                self.flattened_sorter.insert(docid.to_be_bytes(), &document_sorter_value_buffer)?;
            }
            documents_count += 1;

            progress_callback(UpdateIndexingStep::RemapDocumentAddition {
                documents_seen: documents_count,
            });

            field_buffer = drop_and_reuse(field_buffer_cache);
            docid_buffer.clear();
            obkv_buffer.clear();
        }

        progress_callback(UpdateIndexingStep::RemapDocumentAddition {
            documents_seen: documents_count,
        });

        self.index.put_fields_ids_map(wtxn, &self.fields_ids_map)?;
        self.index.put_primary_key(wtxn, &primary_key)?;
        self.documents_count += documents_count;
        // Now that we have a valid sorter that contains the user id and the obkv we
        // give it to the last transforming function which returns the TransformOutput.
        Ok(documents_count)
    }

    /// The counter part of `read_documents` that removes documents either from the transform or the database.
    /// It can be called before, after or in between two calls of the `read_documents`.
    ///
    /// It needs to update all the internal datastructure in the transform.
    /// - If the document is coming from the database -> it's marked as a to_delete document
    /// - If the document to remove was inserted by the `read_documents` method before AND was present in the db,
    ///   it's marked as `to_delete` + added into the grenad to ensure we don't reinsert it.
    /// - If the document to remove was inserted by the `read_documents` method before but was NOT present in the db,
    ///   it's added into the grenad to ensure we don't insert it + removed from the list of new documents ids.
    /// - If the document to remove was not present in either the db or the transform we do nothing.
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::documents")]
    pub fn remove_documents<FA>(
        &mut self,
        mut to_remove: Vec<String>,
        wtxn: &mut heed::RwTxn,
        should_abort: FA,
    ) -> Result<usize>
    where
        FA: Fn() -> bool + Sync,
    {
        puffin::profile_function!();

        // there may be duplicates in the documents to remove.
        to_remove.sort_unstable();
        to_remove.dedup();

        let external_documents_ids = self.index.external_documents_ids();

        let mut documents_deleted = 0;
        let mut document_sorter_value_buffer = Vec::new();
        let mut document_sorter_key_buffer = Vec::new();
        for to_remove in to_remove {
            if should_abort() {
                return Err(Error::InternalError(InternalError::AbortedIndexation));
            }

            // Check if the document has been added in the current indexing process.
            let deleted_from_current =
                match self.new_external_documents_ids_builder.entry((*to_remove).into()) {
                    // if the document was added in a previous iteration of the transform we make it as deleted in the sorters.
                    HEntry::Occupied(entry) => {
                        let docid = *entry.get() as u32;
                        // Key is the concatenation of the internal docid and the external one.
                        document_sorter_key_buffer.clear();
                        document_sorter_key_buffer.extend_from_slice(&docid.to_be_bytes());
                        document_sorter_key_buffer.extend_from_slice(to_remove.as_bytes());
                        document_sorter_value_buffer.clear();
                        document_sorter_value_buffer.push(Operation::Deletion as u8);
                        obkv::KvWriterU16::new(&mut document_sorter_value_buffer).finish().unwrap();
                        self.original_sorter
                            .insert(&document_sorter_key_buffer, &document_sorter_value_buffer)?;
                        self.flattened_sorter
                            .insert(docid.to_be_bytes(), &document_sorter_value_buffer)?;

                        // we must NOT update the list of replaced_documents_ids
                        // Either:
                        // 1. It's already in it and there is nothing to do
                        // 2. It wasn't in it because the document was created by a previous batch and since
                        //    we're removing it there is nothing to do.
                        self.new_documents_ids.remove(docid);
                        entry.remove_entry();
                        true
                    }
                    HEntry::Vacant(_) => false,
                };

            // If the document was already in the db we mark it as a `to_delete` document.
            // Then we push the document in sorters in deletion mode.
            let deleted_from_db = match external_documents_ids.get(wtxn, &to_remove)? {
                Some(docid) => {
                    self.remove_document_from_db(
                        docid,
                        to_remove,
                        wtxn,
                        &mut document_sorter_key_buffer,
                        &mut document_sorter_value_buffer,
                    )?;
                    true
                }
                None => false,
            };

            // increase counter only if the document existed somewhere before.
            if deleted_from_current || deleted_from_db {
                documents_deleted += 1;
            }
        }

        Ok(documents_deleted)
    }

    /// Removes documents from db using their internal document ids.
    ///
    /// # Warning
    ///
    /// This function is dangerous and will only work correctly if:
    ///
    /// - All the passed ids currently exist in the database
    /// - No batching using the standards `remove_documents` and `add_documents` took place
    ///
    /// TODO: make it impossible to call `remove_documents` or `add_documents` on an instance that calls this function.
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::details")]
    pub fn remove_documents_from_db_no_batch<FA>(
        &mut self,
        to_remove: &RoaringBitmap,
        wtxn: &mut heed::RwTxn,
        should_abort: FA,
    ) -> Result<usize>
    where
        FA: Fn() -> bool + Sync,
    {
        puffin::profile_function!();

        let mut documents_deleted = 0;
        let mut document_sorter_value_buffer = Vec::new();
        let mut document_sorter_key_buffer = Vec::new();
        let external_ids = self.index.external_id_of(wtxn, to_remove.iter())?;

        for (internal_docid, external_docid) in to_remove.iter().zip(external_ids) {
            let external_docid = external_docid?;
            if should_abort() {
                return Err(Error::InternalError(InternalError::AbortedIndexation));
            }
            self.remove_document_from_db(
                internal_docid,
                external_docid,
                wtxn,
                &mut document_sorter_key_buffer,
                &mut document_sorter_value_buffer,
            )?;

            documents_deleted += 1;
        }

        Ok(documents_deleted)
    }

    fn remove_document_from_db(
        &mut self,
        internal_docid: u32,
        external_docid: String,
        txn: &heed::RoTxn,
        document_sorter_key_buffer: &mut Vec<u8>,
        document_sorter_value_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        self.replaced_documents_ids.insert(internal_docid);

        // fetch the obkv document
        let original_key = internal_docid;
        let base_obkv = self
            .index
            .documents
            .remap_data_type::<heed::types::Bytes>()
            .get(txn, &original_key)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::DOCUMENTS,
                key: None,
            })?;

        // Key is the concatenation of the internal docid and the external one.
        document_sorter_key_buffer.clear();
        document_sorter_key_buffer.extend_from_slice(&internal_docid.to_be_bytes());
        document_sorter_key_buffer.extend_from_slice(external_docid.as_bytes());
        // push it as to delete in the original_sorter
        document_sorter_value_buffer.clear();
        document_sorter_value_buffer.push(Operation::Deletion as u8);
        into_del_add_obkv(
            KvReaderU16::new(base_obkv),
            DelAddOperation::Deletion,
            document_sorter_value_buffer,
        )?;
        self.original_sorter.insert(&document_sorter_key_buffer, &document_sorter_value_buffer)?;

        // flatten it and push it as to delete in the flattened_sorter
        let flattened_obkv = KvReader::new(base_obkv);
        if let Some(obkv) =
            Self::flatten_from_fields_ids_map(&flattened_obkv, &mut self.fields_ids_map)?
        {
            // we recreate our buffer with the flattened documents
            document_sorter_value_buffer.clear();
            document_sorter_value_buffer.push(Operation::Deletion as u8);
            into_del_add_obkv(
                KvReaderU16::new(&obkv),
                DelAddOperation::Deletion,
                document_sorter_value_buffer,
            )?;
        }
        self.flattened_sorter
            .insert(internal_docid.to_be_bytes(), &document_sorter_value_buffer)?;
        Ok(())
    }

    // Flatten a document from the fields ids map contained in self and insert the new
    // created fields. Returns `None` if the document doesn't need to be flattened.
    #[tracing::instrument(
        level = "trace",
        skip(obkv, fields_ids_map),
        target = "indexing::transform"
    )]
    fn flatten_from_fields_ids_map(
        obkv: &KvReader<FieldId>,
        fields_ids_map: &mut FieldsIdsMap,
    ) -> Result<Option<Vec<u8>>> {
        if obkv
            .iter()
            .all(|(_, value)| !json_depth_checker::should_flatten_from_unchecked_slice(value))
        {
            return Ok(None);
        }

        // store the keys and values the original obkv + the flattened json
        // We first extract all the key+value out of the obkv. If a value is not nested
        // we keep a reference on its value. If the value is nested we'll get its value
        // as an owned `Vec<u8>` after flattening it.
        let mut key_value: Vec<(FieldId, Cow<[u8]>)> = Vec::new();

        // the object we're going to use to store the fields that need to be flattened.
        let mut doc = serde_json::Map::new();

        // we recreate a json containing only the fields that needs to be flattened.
        // all the raw values get inserted directly in the `key_value` vec.
        for (key, value) in obkv.iter() {
            if json_depth_checker::should_flatten_from_unchecked_slice(value) {
                let key = fields_ids_map.name(key).ok_or(FieldIdMapMissingEntry::FieldId {
                    field_id: key,
                    process: "Flatten from fields ids map.",
                })?;

                let value = serde_json::from_slice::<Value>(value)
                    .map_err(crate::error::InternalError::SerdeJson)?;
                doc.insert(key.to_string(), value);
            } else {
                key_value.push((key, value.into()));
            }
        }

        let flattened = flatten_serde_json::flatten(&doc);

        // Once we have the flattened version we insert all the new generated fields_ids
        // (if any) in the fields ids map and serialize the value.
        for (key, value) in flattened.into_iter() {
            let fid = fields_ids_map.insert(&key).ok_or(UserError::AttributeLimitReached)?;
            let value = serde_json::to_vec(&value).map_err(InternalError::SerdeJson)?;
            key_value.push((fid, value.into()));
        }

        // we sort the key. If there was a conflict between the obkv and the new generated value the
        // keys will be consecutive.
        key_value.sort_unstable_by_key(|(key, _)| *key);

        let mut buffer = Vec::new();
        Self::create_obkv_from_key_value(&mut key_value, &mut buffer)?;
        Ok(Some(buffer))
    }

    /// Generate an obkv from a slice of key / value sorted by key.
    fn create_obkv_from_key_value(
        key_value: &mut [(FieldId, Cow<[u8]>)],
        output_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        debug_assert!(
            key_value.windows(2).all(|vec| vec[0].0 <= vec[1].0),
            "The slice of key / value pair must be sorted."
        );

        output_buffer.clear();
        let mut writer = KvWriter::new(output_buffer);

        let mut skip_next_value = false;
        for things in key_value.windows(2) {
            if skip_next_value {
                skip_next_value = false;
                continue;
            }
            let (key1, value1) = &things[0];
            let (key2, value2) = &things[1];

            // now we're going to look for conflicts between the keys. For example the following documents would cause a conflict:
            // { "doggo.name": "jean", "doggo": { "name": "paul" } }
            // we should find a first "doggo.name" from the obkv and a second one from the flattening.
            // but we must generate the following document:
            // { "doggo.name": ["jean", "paul"] }
            // thus we're going to merge the value from the obkv and the flattened document in a single array and skip the next
            // iteration.
            if key1 == key2 {
                skip_next_value = true;

                let value1 = serde_json::from_slice(value1)
                    .map_err(crate::error::InternalError::SerdeJson)?;
                let value2 = serde_json::from_slice(value2)
                    .map_err(crate::error::InternalError::SerdeJson)?;
                let value = match (value1, value2) {
                    (Value::Array(mut left), Value::Array(mut right)) => {
                        left.append(&mut right);
                        Value::Array(left)
                    }
                    (Value::Array(mut array), value) | (value, Value::Array(mut array)) => {
                        array.push(value);
                        Value::Array(array)
                    }
                    (left, right) => Value::Array(vec![left, right]),
                };

                let value = serde_json::to_vec(&value).map_err(InternalError::SerdeJson)?;
                writer.insert(*key1, value)?;
            } else {
                writer.insert(*key1, value1)?;
            }
        }

        if !skip_next_value {
            // the unwrap is safe here, we know there was at least one value in the document
            let (key, value) = key_value.last().unwrap();
            writer.insert(*key, value)?;
        }

        Ok(())
    }

    /// Generate the `TransformOutput` based on the given sorter that can be generated from any
    /// format like CSV, JSON or JSON stream. This sorter must contain a key that is the document
    /// id for the user side and the value must be an obkv where keys are valid fields ids.
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::transform")]
    pub(crate) fn output_from_sorter<F>(
        self,
        wtxn: &mut heed::RwTxn,
        progress_callback: F,
    ) -> Result<TransformOutput>
    where
        F: Fn(UpdateIndexingStep) + Sync,
    {
        puffin::profile_function!();

        let primary_key = self
            .index
            .primary_key(wtxn)?
            .ok_or(Error::InternalError(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::PRIMARY_KEY_KEY),
            }))?
            .to_string();

        // We create a final writer to write the new documents in order from the sorter.
        let mut writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );

        // To compute the field distribution we need to;
        // 1. Remove all the deleted documents from the field distribution
        // 2. Add all the new documents to the field distribution
        let mut field_distribution = self.index.field_distribution(wtxn)?;

        // Here we are going to do the document count + field distribution + `write_into_stream_writer`
        let mut iter = self.original_sorter.into_stream_merger_iter()?;
        // used only for the callback
        let mut documents_count = 0;

        while let Some((key, val)) = iter.next()? {
            // skip first byte corresponding to the operation type (Deletion or Addition).
            let val = &val[1..];

            // send a callback to show at which step we are
            documents_count += 1;
            progress_callback(UpdateIndexingStep::ComputeIdsAndMergeDocuments {
                documents_seen: documents_count,
                total_documents: self.documents_count,
            });

            for (key, value) in KvReader::new(val) {
                let reader = KvReaderDelAdd::new(value);
                match (reader.get(DelAdd::Deletion), reader.get(DelAdd::Addition)) {
                    (None, None) => {}
                    (None, Some(_)) => {
                        // New field
                        let name = self.fields_ids_map.name(key).ok_or(
                            FieldIdMapMissingEntry::FieldId {
                                field_id: key,
                                process: "Computing field distribution in transform.",
                            },
                        )?;
                        *field_distribution.entry(name.to_string()).or_insert(0) += 1;
                    }
                    (Some(_), None) => {
                        // Field removed
                        let name = self.fields_ids_map.name(key).ok_or(
                            FieldIdMapMissingEntry::FieldId {
                                field_id: key,
                                process: "Computing field distribution in transform.",
                            },
                        )?;
                        match field_distribution.entry(name.to_string()) {
                            BEntry::Vacant(_) => { /* Bug? trying to remove a non-existing field */
                            }
                            BEntry::Occupied(mut entry) => {
                                // attempt to remove one
                                match entry.get_mut().checked_sub(1) {
                                    Some(0) => {
                                        entry.remove();
                                    }
                                    Some(new_val) => {
                                        *entry.get_mut() = new_val;
                                    }
                                    None => {
                                        unreachable!("Attempting to remove a field that wasn't in the field distribution")
                                    }
                                }
                            }
                        }
                    }
                    (Some(_), Some(_)) => {
                        // Value change, no field distribution change
                    }
                }
            }
            writer.insert(key, val)?;
        }

        let mut original_documents = writer.into_inner()?;
        // We then extract the file and reset the seek to be able to read it again.
        original_documents.rewind()?;

        // We create a final writer to write the new documents in order from the sorter.
        let mut writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );

        // Once we have written all the documents into the final sorter, we write the nested documents
        // into this writer.
        // We get rids of the `Operation` byte and skip the deleted documents as well.
        let mut iter = self.flattened_sorter.into_stream_merger_iter()?;
        while let Some((key, val)) = iter.next()? {
            // skip first byte corresponding to the operation type (Deletion or Addition).
            let val = &val[1..];
            writer.insert(key, val)?;
        }
        let mut flattened_documents = writer.into_inner()?;
        flattened_documents.rewind()?;

        let mut new_external_documents_ids_builder: Vec<_> =
            self.new_external_documents_ids_builder.into_iter().collect();

        new_external_documents_ids_builder
            .sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut fst_new_external_documents_ids_builder = fst::MapBuilder::memory();
        new_external_documents_ids_builder.into_iter().try_for_each(|(key, value)| {
            fst_new_external_documents_ids_builder.insert(key, value)
        })?;

        let old_inner_settings = InnerIndexSettings::from_index(self.index, wtxn)?;
        let mut new_inner_settings = old_inner_settings.clone();
        new_inner_settings.fields_ids_map = self.fields_ids_map;
        let settings_diff = InnerIndexSettingsDiff {
            old: old_inner_settings,
            new: new_inner_settings,
            embedding_configs_updated: false,
            settings_update_only: false,
        };

        Ok(TransformOutput {
            primary_key,
            settings_diff,
            field_distribution,
            documents_count: self.documents_count,
            original_documents: original_documents.into_inner().map_err(|err| err.into_error())?,
            flattened_documents: flattened_documents
                .into_inner()
                .map_err(|err| err.into_error())?,
        })
    }

    fn rebind_existing_document(
        old_obkv: KvReader<FieldId>,
        settings_diff: &InnerIndexSettingsDiff,
        original_obkv_buffer: &mut Vec<u8>,
        flattened_obkv_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        let mut old_fields_ids_map = settings_diff.old.fields_ids_map.clone();
        let mut new_fields_ids_map = settings_diff.new.fields_ids_map.clone();
        let mut obkv_writer = KvWriter::<_, FieldId>::memory();
        // We iterate over the new `FieldsIdsMap` ids in order and construct the new obkv.
        for (id, name) in new_fields_ids_map.iter() {
            if let Some(val) = old_fields_ids_map.id(name).and_then(|id| old_obkv.get(id)) {
                obkv_writer.insert(id, val)?;
            }
        }
        let data = obkv_writer.into_inner()?;
        let new_obkv = KvReader::<FieldId>::new(&data);

        // take the non-flattened version if flatten_from_fields_ids_map returns None.
        let old_flattened = Self::flatten_from_fields_ids_map(&old_obkv, &mut old_fields_ids_map)?;
        let old_flattened =
            old_flattened.as_deref().map_or_else(|| old_obkv, KvReader::<FieldId>::new);
        let new_flattened = Self::flatten_from_fields_ids_map(&new_obkv, &mut new_fields_ids_map)?;
        let new_flattened =
            new_flattened.as_deref().map_or_else(|| new_obkv, KvReader::<FieldId>::new);

        original_obkv_buffer.clear();
        flattened_obkv_buffer.clear();

        del_add_from_two_obkvs(&old_obkv, &new_obkv, original_obkv_buffer)?;
        del_add_from_two_obkvs(&old_flattened, &new_flattened, flattened_obkv_buffer)?;

        Ok(())
    }

    /// Clear all databases. Returns a `TransformOutput` with a file that contains the documents
    /// of the index with the attributes reordered accordingly to the `FieldsIdsMap` given as argument.
    ///
    // TODO this can be done in parallel by using the rayon `ThreadPool`.
    pub fn prepare_for_documents_reindexing(
        self,
        wtxn: &mut heed::RwTxn<'i>,
        settings_diff: InnerIndexSettingsDiff,
    ) -> Result<TransformOutput> {
        // There already has been a document addition, the primary key should be set by now.
        let primary_key = self
            .index
            .primary_key(wtxn)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::PRIMARY_KEY_KEY),
            })?
            .to_string();
        let field_distribution = self.index.field_distribution(wtxn)?;

        let documents_ids = self.index.documents_ids(wtxn)?;
        let documents_count = documents_ids.len() as usize;

        // We initialize the sorter with the user indexing settings.
        let mut original_sorter = create_sorter(
            grenad::SortAlgorithm::Stable,
            keep_first,
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            self.indexer_settings.max_nb_chunks,
            self.indexer_settings.max_memory.map(|mem| mem / 2),
        );

        // We initialize the sorter with the user indexing settings.
        let mut flattened_sorter = create_sorter(
            grenad::SortAlgorithm::Stable,
            keep_first,
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            self.indexer_settings.max_nb_chunks,
            self.indexer_settings.max_memory.map(|mem| mem / 2),
        );

        let mut original_obkv_buffer = Vec::new();
        let mut flattened_obkv_buffer = Vec::new();
        let mut document_sorter_key_buffer = Vec::new();
        for result in self.index.external_documents_ids().iter(wtxn)? {
            let (external_id, docid) = result?;
            let old_obkv = self.index.documents.get(wtxn, &docid)?.ok_or(
                InternalError::DatabaseMissingEntry { db_name: db_name::DOCUMENTS, key: None },
            )?;

            Self::rebind_existing_document(
                old_obkv,
                &settings_diff,
                &mut original_obkv_buffer,
                &mut flattened_obkv_buffer,
            )?;

            document_sorter_key_buffer.clear();
            document_sorter_key_buffer.extend_from_slice(&docid.to_be_bytes());
            document_sorter_key_buffer.extend_from_slice(external_id.as_bytes());
            original_sorter.insert(&document_sorter_key_buffer, &original_obkv_buffer)?;
            flattened_sorter.insert(docid.to_be_bytes(), &flattened_obkv_buffer)?;
        }

        let grenad_params = GrenadParameters {
            chunk_compression_type: self.indexer_settings.chunk_compression_type,
            chunk_compression_level: self.indexer_settings.chunk_compression_level,
            max_memory: self.indexer_settings.max_memory,
            max_nb_chunks: self.indexer_settings.max_nb_chunks, // default value, may be chosen.
        };

        // Once we have written all the documents, we merge everything into a Reader.
        let original_documents = sorter_into_reader(original_sorter, grenad_params)?;

        let flattened_documents = sorter_into_reader(flattened_sorter, grenad_params)?;

        Ok(TransformOutput {
            primary_key,
            field_distribution,
            settings_diff,
            documents_count,
            original_documents: original_documents.into_inner().into_inner(),
            flattened_documents: flattened_documents.into_inner().into_inner(),
        })
    }
}

/// Drops all the value of type `U` in vec, and reuses the allocation to create a `Vec<T>`.
///
/// The size and alignment of T and U must match.
fn drop_and_reuse<U, T>(mut vec: Vec<U>) -> Vec<T> {
    debug_assert_eq!(std::mem::align_of::<U>(), std::mem::align_of::<T>());
    debug_assert_eq!(std::mem::size_of::<U>(), std::mem::size_of::<T>());
    vec.clear();
    debug_assert!(vec.is_empty());
    vec.into_iter().map(|_| unreachable!()).collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn merge_obkvs() {
        let mut additive_doc_0 = Vec::new();
        let mut deletive_doc_0 = Vec::new();
        let mut del_add_doc_0 = Vec::new();
        let mut kv_writer = KvWriter::memory();
        kv_writer.insert(0_u8, [0]).unwrap();
        let buffer = kv_writer.into_inner().unwrap();
        into_del_add_obkv(
            KvReaderU16::new(&buffer),
            DelAddOperation::Addition,
            &mut additive_doc_0,
        )
        .unwrap();
        additive_doc_0.insert(0, Operation::Addition as u8);
        into_del_add_obkv(
            KvReaderU16::new(&buffer),
            DelAddOperation::Deletion,
            &mut deletive_doc_0,
        )
        .unwrap();
        deletive_doc_0.insert(0, Operation::Deletion as u8);
        into_del_add_obkv(
            KvReaderU16::new(&buffer),
            DelAddOperation::DeletionAndAddition,
            &mut del_add_doc_0,
        )
        .unwrap();
        del_add_doc_0.insert(0, Operation::Addition as u8);

        let mut additive_doc_1 = Vec::new();
        let mut kv_writer = KvWriter::memory();
        kv_writer.insert(1_u8, [1]).unwrap();
        let buffer = kv_writer.into_inner().unwrap();
        into_del_add_obkv(
            KvReaderU16::new(&buffer),
            DelAddOperation::Addition,
            &mut additive_doc_1,
        )
        .unwrap();
        additive_doc_1.insert(0, Operation::Addition as u8);

        let mut additive_doc_0_1 = Vec::new();
        let mut kv_writer = KvWriter::memory();
        kv_writer.insert(0_u8, [0]).unwrap();
        kv_writer.insert(1_u8, [1]).unwrap();
        let buffer = kv_writer.into_inner().unwrap();
        into_del_add_obkv(
            KvReaderU16::new(&buffer),
            DelAddOperation::Addition,
            &mut additive_doc_0_1,
        )
        .unwrap();
        additive_doc_0_1.insert(0, Operation::Addition as u8);

        let ret = obkvs_merge_additions_and_deletions(&[], &[Cow::from(additive_doc_0.as_slice())])
            .unwrap();
        assert_eq!(*ret, additive_doc_0);

        let ret = obkvs_merge_additions_and_deletions(
            &[],
            &[Cow::from(deletive_doc_0.as_slice()), Cow::from(additive_doc_0.as_slice())],
        )
        .unwrap();
        assert_eq!(*ret, del_add_doc_0);

        let ret = obkvs_merge_additions_and_deletions(
            &[],
            &[Cow::from(additive_doc_0.as_slice()), Cow::from(deletive_doc_0.as_slice())],
        )
        .unwrap();
        assert_eq!(*ret, deletive_doc_0);

        let ret = obkvs_merge_additions_and_deletions(
            &[],
            &[
                Cow::from(additive_doc_1.as_slice()),
                Cow::from(deletive_doc_0.as_slice()),
                Cow::from(additive_doc_0.as_slice()),
            ],
        )
        .unwrap();
        assert_eq!(*ret, del_add_doc_0);

        let ret = obkvs_merge_additions_and_deletions(
            &[],
            &[Cow::from(additive_doc_1.as_slice()), Cow::from(additive_doc_0.as_slice())],
        )
        .unwrap();
        assert_eq!(*ret, additive_doc_0_1);

        let ret = obkvs_keep_last_addition_merge_deletions(
            &[],
            &[Cow::from(additive_doc_1.as_slice()), Cow::from(additive_doc_0.as_slice())],
        )
        .unwrap();
        assert_eq!(*ret, additive_doc_0);

        let ret = obkvs_keep_last_addition_merge_deletions(
            &[],
            &[
                Cow::from(deletive_doc_0.as_slice()),
                Cow::from(additive_doc_1.as_slice()),
                Cow::from(additive_doc_0.as_slice()),
            ],
        )
        .unwrap();
        assert_eq!(*ret, del_add_doc_0);
    }
}

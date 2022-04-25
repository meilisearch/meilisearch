use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use byteorder::ReadBytesExt;
use fxhash::FxHashMap;
use heed::RoTxn;
use itertools::Itertools;
use obkv::{KvReader, KvWriter};
use roaring::RoaringBitmap;
use serde_json::{Map, Value};
use smartstring::SmartString;

use super::helpers::{create_sorter, create_writer, keep_latest_obkv, merge_obkvs, MergeFn};
use super::{IndexDocumentsMethod, IndexerConfig};
use crate::documents::{DocumentBatchReader, DocumentsBatchIndex};
use crate::error::{Error, InternalError, UserError};
use crate::index::db_name;
use crate::update::{AvailableDocumentsIds, UpdateIndexingStep};
use crate::{
    ExternalDocumentsIds, FieldDistribution, FieldId, FieldIdMapMissingEntry, FieldsIdsMap, Index,
    Result, BEU32,
};

const DEFAULT_PRIMARY_KEY_NAME: &str = "id";

pub struct TransformOutput {
    pub primary_key: String,
    pub fields_ids_map: FieldsIdsMap,
    pub field_distribution: FieldDistribution,
    pub external_documents_ids: ExternalDocumentsIds<'static>,
    pub new_documents_ids: RoaringBitmap,
    pub replaced_documents_ids: RoaringBitmap,
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

    original_sorter: grenad::Sorter<MergeFn>,
    flattened_sorter: grenad::Sorter<MergeFn>,
    replaced_documents_ids: RoaringBitmap,
    new_documents_ids: RoaringBitmap,
    // To increase the cache locality and the heap usage we use smartstring.
    new_external_documents_ids_builder: FxHashMap<SmartString<smartstring::Compact>, u64>,
    documents_count: usize,
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
        .map(|(field, name)| match index_field_map.id(&name) {
            Some(id) => Ok((*field, id)),
            None => index_field_map
                .insert(&name)
                .ok_or(Error::UserError(UserError::AttributeLimitReached))
                .map(|id| (*field, id)),
        })
        .collect()
}

/// Look for a key containing the [DEFAULT_PRIMARY_KEY_NAME] in the fields.
/// It doesn't look in the subfield because we don't want to enable the
/// primary key inference on nested objects.
fn find_primary_key(index: &DocumentsBatchIndex) -> Option<&str> {
    index
        .iter()
        .sorted_by_key(|(k, _)| *k)
        .map(|(_, v)| v)
        .find(|v| v.to_lowercase().contains(DEFAULT_PRIMARY_KEY_NAME))
        .map(String::as_str)
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
            IndexDocumentsMethod::ReplaceDocuments => keep_latest_obkv,
            IndexDocumentsMethod::UpdateDocuments => merge_obkvs,
        };

        // We initialize the sorter with the user indexing settings.
        let original_sorter = create_sorter(
            merge_function,
            indexer_settings.chunk_compression_type,
            indexer_settings.chunk_compression_level,
            indexer_settings.max_nb_chunks,
            indexer_settings.max_memory.map(|mem| mem / 2),
        );

        // We initialize the sorter with the user indexing settings.
        let flattened_sorter = create_sorter(
            merge_function,
            indexer_settings.chunk_compression_type,
            indexer_settings.chunk_compression_level,
            indexer_settings.max_nb_chunks,
            indexer_settings.max_memory.map(|mem| mem / 2),
        );

        Ok(Transform {
            index,
            fields_ids_map: index.fields_ids_map(wtxn)?,
            indexer_settings,
            autogenerate_docids,
            original_sorter,
            flattened_sorter,
            index_documents_method,
            replaced_documents_ids: RoaringBitmap::new(),
            new_documents_ids: RoaringBitmap::new(),
            new_external_documents_ids_builder: FxHashMap::default(),
            documents_count: 0,
        })
    }

    pub fn read_documents<R, F>(
        &mut self,
        mut reader: DocumentBatchReader<R>,
        wtxn: &mut heed::RwTxn,
        progress_callback: F,
    ) -> Result<usize>
    where
        R: Read + Seek,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let fields_index = reader.index();
        let external_documents_ids = self.index.external_documents_ids(wtxn)?;
        let documents_ids = self.index.documents_ids(wtxn)?;
        let mut available_documents_ids = AvailableDocumentsIds::from_documents_ids(&documents_ids);

        let mapping = create_fields_mapping(&mut self.fields_ids_map, fields_index)?;

        let alternative_name = self
            .index
            .primary_key(wtxn)?
            .or_else(|| find_primary_key(fields_index))
            .map(String::from);

        let (primary_key_id, primary_key_name) = compute_primary_key_pair(
            self.index.primary_key(wtxn)?,
            &mut self.fields_ids_map,
            alternative_name,
            self.autogenerate_docids,
        )?;

        let primary_key_id_nested = primary_key_name.contains('.');

        let mut flattened_document = None;
        let mut obkv_buffer = Vec::new();
        let mut flattened_obkv_buffer = Vec::new();
        let mut documents_count = 0;
        let mut external_id_buffer = Vec::new();
        let mut field_buffer: Vec<(u16, Cow<[u8]>)> = Vec::new();
        while let Some((addition_index, document)) = reader.next_document_with_index()? {
            let mut field_buffer_cache = drop_and_reuse(field_buffer);
            if self.indexer_settings.log_every_n.map_or(false, |len| documents_count % len == 0) {
                progress_callback(UpdateIndexingStep::RemapDocumentAddition {
                    documents_seen: documents_count,
                });
            }

            for (k, v) in document.iter() {
                let mapped_id =
                    *mapping.get(&k).ok_or(InternalError::FieldIdMappingMissingEntry { key: k })?;
                field_buffer_cache.push((mapped_id, Cow::from(v)));
            }

            // We need to make sure that every document has a primary key. After we have remapped
            // all the fields in the document, we try to find the primary key value. If we can find
            // it, transform it into a string and validate it, and then update it in the
            // document. If none is found, and we were told to generate missing document ids, then
            // we create the missing field, and update the new document.
            let mut uuid_buffer = [0; uuid::adapter::Hyphenated::LENGTH];
            let external_id = if primary_key_id_nested {
                let mut field_buffer_cache = field_buffer_cache.clone();
                self.flatten_from_field_mapping(
                    &mapping,
                    &document,
                    &mut flattened_obkv_buffer,
                    &mut field_buffer_cache,
                )?;
                flattened_document = Some(&flattened_obkv_buffer);
                let document = KvReader::new(&flattened_obkv_buffer);

                update_primary_key(
                    document,
                    &addition_index,
                    primary_key_id,
                    &primary_key_name,
                    &mut uuid_buffer,
                    &mut field_buffer_cache,
                    &mut external_id_buffer,
                    self.autogenerate_docids,
                )?
            } else {
                update_primary_key(
                    document,
                    &addition_index,
                    primary_key_id,
                    &primary_key_name,
                    &mut uuid_buffer,
                    &mut field_buffer_cache,
                    &mut external_id_buffer,
                    self.autogenerate_docids,
                )?
            };

            // Insertion in a obkv need to be done with keys ordered. For now they are ordered
            // according to the document addition key order, so we sort it according to the
            // fieldids map keys order.
            field_buffer_cache.sort_unstable_by(|(f1, _), (f2, _)| f1.cmp(&f2));

            // Build the new obkv document.
            let mut writer = obkv::KvWriter::new(&mut obkv_buffer);
            for (k, v) in field_buffer_cache.iter() {
                writer.insert(*k, v)?;
            }

            let (docid, should_insert_original_document) =
                match external_documents_ids.get(&*external_id) {
                    // if the document is in the db but has already been inserted
                    // (ie: already exists in the list of replaced documents ids),
                    // we should not add the original document a second time.
                    Some(docid) => (docid, !self.replaced_documents_ids.contains(docid)),
                    None => {
                        // if the document has already been inserted in this
                        // batch we need to get its docid
                        match self.new_external_documents_ids_builder.entry(external_id.into()) {
                            Entry::Occupied(entry) => (*entry.get() as u32, false),
                            // if the document has never been encountered we give it a new docid
                            // and push this new docid to the external documents ids builder
                            Entry::Vacant(entry) => {
                                let new_docid = available_documents_ids
                                    .next()
                                    .ok_or(UserError::DocumentLimitReached)?;
                                entry.insert(new_docid as u64);
                                (new_docid, false)
                            }
                        }
                    }
                };

            if should_insert_original_document {
                self.replaced_documents_ids.insert(docid);

                let key = BEU32::new(docid);
                let base_obkv = self
                    .index
                    .documents
                    .remap_data_type::<heed::types::ByteSlice>()
                    .get(wtxn, &key)?
                    .ok_or(InternalError::DatabaseMissingEntry {
                        db_name: db_name::DOCUMENTS,
                        key: None,
                    })?;

                self.original_sorter.insert(&docid.to_be_bytes(), base_obkv)?;
                match self.flatten_from_fields_ids_map(KvReader::new(&base_obkv))? {
                    Some(buffer) => self.flattened_sorter.insert(docid.to_be_bytes(), &buffer)?,
                    None => self.flattened_sorter.insert(docid.to_be_bytes(), base_obkv)?,
                }
            } else {
                self.new_documents_ids.insert(docid);
            }

            // We use the extracted/generated user id as the key for this document.
            self.original_sorter.insert(&docid.to_be_bytes(), obkv_buffer.clone())?;
            documents_count += 1;

            if let Some(flatten) = flattened_document {
                self.flattened_sorter.insert(docid.to_be_bytes(), &flatten)?;
            } else {
                match self.flatten_from_fields_ids_map(KvReader::new(&obkv_buffer))? {
                    Some(buffer) => self.flattened_sorter.insert(docid.to_be_bytes(), &buffer)?,
                    None => {
                        self.flattened_sorter.insert(docid.to_be_bytes(), obkv_buffer.clone())?
                    }
                }
            }

            progress_callback(UpdateIndexingStep::RemapDocumentAddition {
                documents_seen: documents_count,
            });

            field_buffer = drop_and_reuse(field_buffer_cache);
            external_id_buffer.clear();
            obkv_buffer.clear();
        }

        progress_callback(UpdateIndexingStep::RemapDocumentAddition {
            documents_seen: documents_count,
        });

        self.index.put_fields_ids_map(wtxn, &self.fields_ids_map)?;
        self.index.put_primary_key(wtxn, &primary_key_name)?;
        self.documents_count += documents_count;
        // Now that we have a valid sorter that contains the user id and the obkv we
        // give it to the last transforming function which returns the TransformOutput.
        Ok(documents_count)
    }

    // Flatten a document from the fields ids map contained in self and insert the new
    // created fields. Returns `None` if the document doesn't need to be flattened.
    fn flatten_from_fields_ids_map(&mut self, obkv: KvReader<FieldId>) -> Result<Option<Vec<u8>>> {
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
                let key = self.fields_ids_map.name(key).ok_or(FieldIdMapMissingEntry::FieldId {
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
            let fid = self.fields_ids_map.insert(&key).ok_or(UserError::AttributeLimitReached)?;
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

    // Flatten a document from a field mapping generated by [create_fields_mapping]
    fn flatten_from_field_mapping(
        &mut self,
        mapping: &HashMap<FieldId, FieldId>,
        obkv: &KvReader<FieldId>,
        output_buffer: &mut Vec<u8>,
        field_buffer_cache: &mut Vec<(u16, Cow<[u8]>)>,
    ) -> Result<()> {
        // store the keys and values of the json + the original obkv
        let mut key_value: Vec<(FieldId, Cow<[u8]>)> = Vec::new();

        // if the primary_key is nested we need to flatten the document before being able to do anything
        let mut doc = serde_json::Map::new();

        // we recreate a json containing only the fields that needs to be flattened.
        // all the raw values get inserted directly in the `key_value` vec.
        for (key, value) in obkv.iter() {
            if json_depth_checker::should_flatten_from_unchecked_slice(value) {
                let key =
                    mapping.get(&key).ok_or(InternalError::FieldIdMappingMissingEntry { key })?;
                let key =
                    self.fields_ids_map.name(*key).ok_or(FieldIdMapMissingEntry::FieldId {
                        field_id: *key,
                        process: "Flatten from field mapping.",
                    })?;
                let value = serde_json::from_slice::<serde_json::Value>(value)
                    .map_err(InternalError::SerdeJson)?;
                doc.insert(key.to_string(), value);
            } else {
                key_value.push((key, value.into()));
            }
        }

        let flattened = flatten_serde_json::flatten(&doc);

        // Once we have the flattened version we insert all the new generated fields_ids
        // (if any) in the fields ids map and serialize the value.
        for (key, value) in flattened.into_iter() {
            let fid = self.fields_ids_map.insert(&key).ok_or(UserError::AttributeLimitReached)?;
            let value = serde_json::to_vec(&value).map_err(InternalError::SerdeJson)?;
            key_value.push((fid, value.clone().into()));

            if field_buffer_cache.iter().find(|(id, _)| *id == fid).is_none() {
                field_buffer_cache.push((fid, value.into()));
            }
        }

        // we sort the key. If there was a conflict between the obkv and the new generated value the
        // keys will be consecutive.
        key_value.sort_unstable_by_key(|(key, _)| *key);

        Self::create_obkv_from_key_value(&mut key_value, output_buffer)?;
        Ok(())
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
    pub(crate) fn output_from_sorter<F>(
        self,
        wtxn: &mut heed::RwTxn,
        progress_callback: F,
    ) -> Result<TransformOutput>
    where
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let primary_key = self
            .index
            .primary_key(&wtxn)?
            .ok_or(Error::UserError(UserError::MissingPrimaryKey))?
            .to_string();

        let mut external_documents_ids = self.index.external_documents_ids(wtxn)?;

        // We create a final writer to write the new documents in order from the sorter.
        let mut writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );

        // Once we have all the documents in the sorter, we write the documents
        // in the writer. We also generate the field distribution.
        let mut field_distribution = self.index.field_distribution(wtxn)?;
        let mut iter = self.original_sorter.into_stream_merger_iter()?;
        // used only for the callback
        let mut documents_count = 0;

        while let Some((key, val)) = iter.next()? {
            // send a callback to show at which step we are
            documents_count += 1;
            progress_callback(UpdateIndexingStep::ComputeIdsAndMergeDocuments {
                documents_seen: documents_count,
                total_documents: self.documents_count,
            });

            let u32_key = key.clone().read_u32::<byteorder::BigEndian>()?;
            // if the document was already in the db we remove all of its field
            // from the field distribution.
            if self.replaced_documents_ids.contains(u32_key) {
                let obkv = self.index.documents.get(wtxn, &BEU32::new(u32_key))?.ok_or(
                    InternalError::DatabaseMissingEntry { db_name: db_name::DOCUMENTS, key: None },
                )?;

                for (key, _) in obkv.iter() {
                    let name =
                        self.fields_ids_map.name(key).ok_or(FieldIdMapMissingEntry::FieldId {
                            field_id: key,
                            process: "Computing field distribution in transform.",
                        })?;
                    // We checked that the document was in the db earlier. If we can't find it it means
                    // there is an inconsistency between the field distribution and the field id map.
                    let field = field_distribution.get_mut(name).ok_or(
                        FieldIdMapMissingEntry::FieldId {
                            field_id: key,
                            process: "Accessing field distribution in transform.",
                        },
                    )?;
                    *field -= 1;
                    if *field == 0 {
                        // since we were able to get the field right before it's safe to unwrap here
                        field_distribution.remove(name).unwrap();
                    }
                }
            }

            // We increment all the field of the current document in the field distribution.
            let obkv = KvReader::new(val);

            for (key, _) in obkv.iter() {
                let name =
                    self.fields_ids_map.name(key).ok_or(FieldIdMapMissingEntry::FieldId {
                        field_id: key,
                        process: "Computing field distribution in transform.",
                    })?;
                *field_distribution.entry(name.to_string()).or_insert(0) += 1;
            }
            writer.insert(key, val)?;
        }

        let mut original_documents = writer.into_inner()?;
        // We then extract the file and reset the seek to be able to read it again.
        original_documents.seek(SeekFrom::Start(0))?;

        // We create a final writer to write the new documents in order from the sorter.
        let mut writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );
        // Once we have written all the documents into the final sorter, we write the documents
        // into this writer, extract the file and reset the seek to be able to read it again.
        self.flattened_sorter.write_into_stream_writer(&mut writer)?;
        let mut flattened_documents = writer.into_inner()?;
        flattened_documents.seek(SeekFrom::Start(0))?;

        let mut new_external_documents_ids_builder: Vec<_> =
            self.new_external_documents_ids_builder.into_iter().collect();

        new_external_documents_ids_builder
            .sort_unstable_by(|(left, _), (right, _)| left.cmp(&right));
        let mut fst_new_external_documents_ids_builder = fst::MapBuilder::memory();
        new_external_documents_ids_builder.into_iter().try_for_each(|(key, value)| {
            fst_new_external_documents_ids_builder.insert(key, value)
        })?;
        let new_external_documents_ids = fst_new_external_documents_ids_builder.into_map();
        external_documents_ids.insert_ids(&new_external_documents_ids)?;

        Ok(TransformOutput {
            primary_key,
            fields_ids_map: self.fields_ids_map,
            field_distribution,
            external_documents_ids: external_documents_ids.into_static(),
            new_documents_ids: self.new_documents_ids,
            replaced_documents_ids: self.replaced_documents_ids,
            documents_count: self.documents_count,
            original_documents,
            flattened_documents,
        })
    }

    /// Returns a `TransformOutput` with a file that contains the documents of the index
    /// with the attributes reordered accordingly to the `FieldsIdsMap` given as argument.
    // TODO this can be done in parallel by using the rayon `ThreadPool`.
    pub fn remap_index_documents(
        self,
        wtxn: &mut heed::RwTxn,
        old_fields_ids_map: FieldsIdsMap,
        mut new_fields_ids_map: FieldsIdsMap,
    ) -> Result<TransformOutput> {
        // There already has been a document addition, the primary key should be set by now.
        let primary_key =
            self.index.primary_key(wtxn)?.ok_or(UserError::MissingPrimaryKey)?.to_string();
        let field_distribution = self.index.field_distribution(wtxn)?;
        let external_documents_ids = self.index.external_documents_ids(wtxn)?;
        let documents_ids = self.index.documents_ids(wtxn)?;
        let documents_count = documents_ids.len() as usize;

        // We create a final writer to write the new documents in order from the sorter.
        let mut original_writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );

        // We create a final writer to write the new documents in order from the sorter.
        let mut flattened_writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );

        let mut obkv_buffer = Vec::new();
        for result in self.index.documents.iter(wtxn)? {
            let (docid, obkv) = result?;
            let docid = docid.get();

            obkv_buffer.clear();
            let mut obkv_writer = obkv::KvWriter::<_, FieldId>::new(&mut obkv_buffer);

            // We iterate over the new `FieldsIdsMap` ids in order and construct the new obkv.
            for (id, name) in new_fields_ids_map.iter() {
                if let Some(val) = old_fields_ids_map.id(name).and_then(|id| obkv.get(id)) {
                    obkv_writer.insert(id, val)?;
                }
            }

            let buffer = obkv_writer.into_inner()?;
            original_writer.insert(docid.to_be_bytes(), &buffer)?;

            // Once we have the document. We're going to flatten it
            // and insert it in the flattened sorter.
            let mut doc = serde_json::Map::new();

            let reader = obkv::KvReader::new(buffer);
            for (k, v) in reader.iter() {
                let key = new_fields_ids_map.name(k).ok_or(FieldIdMapMissingEntry::FieldId {
                    field_id: k,
                    process: "Accessing field distribution in transform.",
                })?;
                let value = serde_json::from_slice::<serde_json::Value>(v)
                    .map_err(InternalError::SerdeJson)?;
                doc.insert(key.to_string(), value);
            }

            let flattened = flatten_serde_json::flatten(&doc);

            // Once we have the flattened version we can convert it back to obkv and
            // insert all the new generated fields_ids (if any) in the fields ids map.
            let mut buffer: Vec<u8> = Vec::new();
            let mut writer = KvWriter::new(&mut buffer);
            let mut flattened: Vec<_> = flattened.into_iter().collect();
            // we reorder the field to get all the known field first
            flattened.sort_unstable_by_key(|(key, _)| {
                new_fields_ids_map.id(&key).unwrap_or(FieldId::MAX)
            });

            for (key, value) in flattened {
                let fid =
                    new_fields_ids_map.insert(&key).ok_or(UserError::AttributeLimitReached)?;
                let value = serde_json::to_vec(&value).map_err(InternalError::SerdeJson)?;
                writer.insert(fid, &value)?;
            }
            flattened_writer.insert(docid.to_be_bytes(), &buffer)?;
        }

        // Once we have written all the documents, we extract
        // the file and reset the seek to be able to read it again.
        let mut original_documents = original_writer.into_inner()?;
        original_documents.seek(SeekFrom::Start(0))?;

        let mut flattened_documents = flattened_writer.into_inner()?;
        flattened_documents.seek(SeekFrom::Start(0))?;

        Ok(TransformOutput {
            primary_key,
            fields_ids_map: new_fields_ids_map,
            field_distribution,
            external_documents_ids: external_documents_ids.into_static(),
            new_documents_ids: documents_ids,
            replaced_documents_ids: RoaringBitmap::default(),
            documents_count,
            original_documents,
            flattened_documents,
        })
    }
}

/// Given an optional primary key and an optional alternative name, returns the (field_id, attr_name)
/// for the primary key according to the following rules:
/// - if primary_key is `Some`, returns the id and the name, else
/// - if alternative_name is Some, adds alternative to the fields_ids_map, and returns the pair, else
/// - if autogenerate_docids is true, insert the default id value in the field ids map ("id") and
/// returns the pair, else
/// - returns an error.
fn compute_primary_key_pair(
    primary_key: Option<&str>,
    fields_ids_map: &mut FieldsIdsMap,
    alternative_name: Option<String>,
    autogenerate_docids: bool,
) -> Result<(FieldId, String)> {
    match primary_key {
        Some(primary_key) => {
            let id = fields_ids_map.insert(primary_key).ok_or(UserError::AttributeLimitReached)?;
            Ok((id, primary_key.to_string()))
        }
        None => {
            let name = match alternative_name {
                Some(key) => key,
                None => {
                    if !autogenerate_docids {
                        // If there is no primary key in the current document batch, we must
                        // return an error and not automatically generate any document id.
                        return Err(UserError::MissingPrimaryKey.into());
                    }
                    DEFAULT_PRIMARY_KEY_NAME.to_string()
                }
            };
            let id = fields_ids_map.insert(&name).ok_or(UserError::AttributeLimitReached)?;
            Ok((id, name))
        }
    }
}

fn validate_document_id(document_id: &str) -> Option<&str> {
    let document_id = document_id.trim();
    Some(document_id).filter(|id| {
        !id.is_empty()
            && id.chars().all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_'))
    })
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

fn update_primary_key<'a>(
    document: KvReader<'a, FieldId>,
    addition_index: &DocumentsBatchIndex,
    primary_key_id: FieldId,
    primary_key_name: &str,
    uuid_buffer: &'a mut [u8; uuid::adapter::Hyphenated::LENGTH],
    field_buffer_cache: &mut Vec<(u16, Cow<'a, [u8]>)>,
    mut external_id_buffer: &'a mut Vec<u8>,
    autogenerate_docids: bool,
) -> Result<Cow<'a, str>> {
    match field_buffer_cache.iter_mut().find(|(id, _)| *id == primary_key_id) {
        Some((_, bytes)) => {
            let value = match serde_json::from_slice(bytes).map_err(InternalError::SerdeJson)? {
                Value::String(string) => match validate_document_id(&string) {
                    Some(s) if s.len() == string.len() => string,
                    Some(s) => s.to_string(),
                    None => {
                        return Err(UserError::InvalidDocumentId {
                            document_id: Value::String(string),
                        }
                        .into())
                    }
                },
                Value::Number(number) => number.to_string(),
                content => {
                    return Err(UserError::InvalidDocumentId { document_id: content.clone() }.into())
                }
            };
            serde_json::to_writer(external_id_buffer, &value).map_err(InternalError::SerdeJson)?;
            Ok(Cow::Owned(value))
        }
        None if autogenerate_docids => {
            let uuid = uuid::Uuid::new_v4().to_hyphenated().encode_lower(uuid_buffer);
            serde_json::to_writer(&mut external_id_buffer, &uuid)
                .map_err(InternalError::SerdeJson)?;
            field_buffer_cache.push((primary_key_id, external_id_buffer.as_slice().into()));
            Ok(Cow::Borrowed(&*uuid))
        }
        None => {
            let mut json = Map::new();
            for (key, value) in document.iter() {
                let key = addition_index.name(key).cloned();
                let value = serde_json::from_slice::<Value>(&value).ok();

                if let Some((k, v)) = key.zip(value) {
                    json.insert(k, v);
                }
            }

            Err(UserError::MissingDocumentId {
                primary_key: primary_key_name.to_string(),
                document: json,
            })?
        }
    }
}

impl TransformOutput {
    // find and insert the new field ids
    pub fn compute_real_facets(&self, rtxn: &RoTxn, index: &Index) -> Result<HashSet<String>> {
        let user_defined_facets = index.user_defined_faceted_fields(rtxn)?;

        Ok(self
            .fields_ids_map
            .names()
            .filter(|&field| crate::is_faceted(field, &user_defined_facets))
            .map(|field| field.to_string())
            .collect())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod compute_primary_key {
        use big_s::S;

        use super::{compute_primary_key_pair, FieldsIdsMap};

        #[test]
        fn should_return_primary_key_if_is_some() {
            let mut fields_map = FieldsIdsMap::new();
            fields_map.insert("toto").unwrap();
            let result = compute_primary_key_pair(
                Some("toto"),
                &mut fields_map,
                Some("tata".to_string()),
                false,
            );
            assert_eq!(result.unwrap(), (0, "toto".to_string()));
            assert_eq!(fields_map.len(), 1);

            // and with nested fields
            let mut fields_map = FieldsIdsMap::new();
            fields_map.insert("toto.tata").unwrap();
            let result = compute_primary_key_pair(
                Some("toto.tata"),
                &mut fields_map,
                Some(S("titi")),
                false,
            );
            assert_eq!(result.unwrap(), (0, "toto.tata".to_string()));
            assert_eq!(fields_map.len(), 1);
        }

        #[test]
        fn should_return_alternative_if_primary_is_none() {
            let mut fields_map = FieldsIdsMap::new();
            let result =
                compute_primary_key_pair(None, &mut fields_map, Some("tata".to_string()), false);
            assert_eq!(result.unwrap(), (0, S("tata")));
            assert_eq!(fields_map.len(), 1);
        }

        #[test]
        fn should_return_default_if_both_are_none() {
            let mut fields_map = FieldsIdsMap::new();
            let result = compute_primary_key_pair(None, &mut fields_map, None, true);
            assert_eq!(result.unwrap(), (0, S("id")));
            assert_eq!(fields_map.len(), 1);
        }

        #[test]
        fn should_return_err_if_both_are_none_and_recompute_is_false() {
            let mut fields_map = FieldsIdsMap::new();
            let result = compute_primary_key_pair(None, &mut fields_map, None, false);
            assert!(result.is_err());
            assert_eq!(fields_map.len(), 0);
        }
    }

    mod primary_key_inference {
        use big_s::S;
        use bimap::BiHashMap;

        use crate::documents::DocumentsBatchIndex;
        use crate::update::index_documents::transform::find_primary_key;

        #[test]
        fn primary_key_infered_on_first_field() {
            // We run the test multiple times to change the order in which the fields are iterated upon.
            for _ in 1..50 {
                let mut map = BiHashMap::new();
                map.insert(1, S("fakeId"));
                map.insert(2, S("fakeId"));
                map.insert(3, S("fakeId"));
                map.insert(4, S("fakeId"));
                map.insert(0, S("realId"));

                assert_eq!(find_primary_key(&DocumentsBatchIndex(map)), Some("realId"));
            }
        }
    }
}

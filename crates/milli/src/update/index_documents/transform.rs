use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;

use either::Either;
use fxhash::FxHashMap;
use itertools::Itertools;
use obkv::{KvReader, KvWriter};
use roaring::RoaringBitmap;
use serde_json::Value;
use smartstring::SmartString;

use super::helpers::{
    create_sorter, sorter_into_reader, EitherObkvMerge, ObkvsKeepLastAdditionMergeDeletions,
    ObkvsMergeAdditionsAndDeletions,
};
use super::{IndexDocumentsMethod, IndexerConfig, KeepFirst};
use crate::documents::DocumentsBatchIndex;
use crate::error::{Error, InternalError, UserError};
use crate::index::{db_name, main_key};
use crate::update::del_add::{
    into_del_add_obkv, into_del_add_obkv_conditional_operation, DelAddOperation,
};
use crate::update::index_documents::GrenadParameters;
use crate::update::settings::InnerIndexSettingsDiff;
use crate::update::AvailableIds;
use crate::vector::parsed_vectors::{ExplicitVectors, VectorOrArrayOfVectors};
use crate::vector::settings::WriteBackToDocuments;
use crate::vector::ArroyWrapper;
use crate::{
    is_faceted_by, FieldDistribution, FieldId, FieldIdMapMissingEntry, FieldsIdsMap, Index, Result,
};

pub struct TransformOutput {
    pub primary_key: String,
    pub settings_diff: InnerIndexSettingsDiff,
    pub field_distribution: FieldDistribution,
    pub documents_count: usize,
    pub original_documents: Option<File>,
    pub flattened_documents: Option<File>,
}

/// Extract the external ids, deduplicate and compute the new internal documents ids
/// and fields ids, writing all the documents under their internal ids into a final file.
///
/// Outputs the new `FieldsIdsMap`, the new `UsersIdsDocumentsIds` map, the new documents ids,
/// the replaced documents ids, the number of documents in this update and the file
/// containing all those documents.
pub struct Transform<'a, 'i> {
    pub index: &'i Index,
    indexer_settings: &'a IndexerConfig,
    pub index_documents_method: IndexDocumentsMethod,
}

/// This enum is specific to the grenad sorter stored in the transform.
/// It's used as the first byte of the grenads and tells you if the document id was an addition or a deletion.
#[repr(u8)]
pub enum Operation {
    Addition,
    Deletion,
}

impl<'a, 'i> Transform<'a, 'i> {
    pub fn new(
        wtxn: &mut heed::RwTxn<'_>,
        index: &'i Index,
        indexer_settings: &'a IndexerConfig,
        index_documents_method: IndexDocumentsMethod,
        _autogenerate_docids: bool,
    ) -> Result<Self> {
        use IndexDocumentsMethod::{ReplaceDocuments, UpdateDocuments};

        // We must choose the appropriate merge function for when two or more documents
        // with the same user id must be merged or fully replaced in the same batch.
        let merge_function = match index_documents_method {
            ReplaceDocuments => Either::Left(ObkvsKeepLastAdditionMergeDeletions),
            UpdateDocuments => Either::Right(ObkvsMergeAdditionsAndDeletions),
        };

        // We initialize the sorter with the user indexing settings.
        let original_sorter = create_sorter(
            grenad::SortAlgorithm::Stable,
            merge_function,
            indexer_settings.chunk_compression_type,
            indexer_settings.chunk_compression_level,
            indexer_settings.max_nb_chunks,
            indexer_settings.max_memory.map(|mem| mem / 2),
            true,
        );

        // We initialize the sorter with the user indexing settings.
        let flattened_sorter = create_sorter(
            grenad::SortAlgorithm::Stable,
            merge_function,
            indexer_settings.chunk_compression_type,
            indexer_settings.chunk_compression_level,
            indexer_settings.max_nb_chunks,
            indexer_settings.max_memory.map(|mem| mem / 2),
            true,
        );
        let documents_ids = index.documents_ids(wtxn)?;

        Ok(Transform { index, indexer_settings, index_documents_method })
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
        let mut key_value: Vec<(FieldId, Cow<'_, [u8]>)> = Vec::new();

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
        key_value: &mut [(FieldId, Cow<'_, [u8]>)],
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

    /// Rebind the field_ids of the provided document to their values
    /// based on the field_ids_maps difference between the old and the new settings,
    /// then fill the provided buffers with delta documents using KvWritterDelAdd.
    #[allow(clippy::too_many_arguments)] // need the vectors + fid, feel free to create a struct xo xo
    fn rebind_existing_document(
        old_obkv: &KvReader<FieldId>,
        settings_diff: &InnerIndexSettingsDiff,
        modified_faceted_fields: &HashSet<String>,
        mut injected_vectors: serde_json::Map<String, serde_json::Value>,
        old_vectors_fid: Option<FieldId>,
        original_obkv_buffer: Option<&mut Vec<u8>>,
        flattened_obkv_buffer: Option<&mut Vec<u8>>,
    ) -> Result<()> {
        // Always keep the primary key.
        let is_primary_key = |id: FieldId| -> bool { settings_diff.primary_key_id == Some(id) };

        // If only a faceted field has been added, keep only this field.
        let must_reindex_facets = settings_diff.reindex_facets();
        let necessary_faceted_field = |id: FieldId| -> bool {
            let field_name = settings_diff.new.fields_ids_map.name(id).unwrap();
            must_reindex_facets
                && modified_faceted_fields
                    .iter()
                    .any(|long| is_faceted_by(long, field_name) || is_faceted_by(field_name, long))
        };

        // Alway provide all fields when vectors are involved because
        // we need the fields for the prompt/templating.
        let reindex_vectors = settings_diff.reindex_vectors();

        // The operations that we must perform on the different fields.
        let mut operations = HashMap::new();
        let mut error_seen = false;

        let mut obkv_writer = KvWriter::<_, FieldId>::memory();
        'write_fid: for (id, val) in old_obkv.iter() {
            if !injected_vectors.is_empty() {
                'inject_vectors: {
                    let Some(vectors_fid) = old_vectors_fid else { break 'inject_vectors };

                    if id < vectors_fid {
                        break 'inject_vectors;
                    }

                    let mut existing_vectors = if id == vectors_fid {
                        let existing_vectors: std::result::Result<
                            serde_json::Map<String, serde_json::Value>,
                            serde_json::Error,
                        > = serde_json::from_slice(val);

                        match existing_vectors {
                            Ok(existing_vectors) => existing_vectors,
                            Err(error) => {
                                if !error_seen {
                                    tracing::error!(%error, "Unexpected `_vectors` field that is not a map. Treating as an empty map");
                                    error_seen = true;
                                }
                                Default::default()
                            }
                        }
                    } else {
                        Default::default()
                    };

                    existing_vectors.append(&mut injected_vectors);

                    operations.insert(vectors_fid, DelAddOperation::DeletionAndAddition);
                    obkv_writer
                        .insert(vectors_fid, serde_json::to_vec(&existing_vectors).unwrap())?;
                    if id == vectors_fid {
                        continue 'write_fid;
                    }
                }
            }

            if is_primary_key(id) || necessary_faceted_field(id) || reindex_vectors {
                operations.insert(id, DelAddOperation::DeletionAndAddition);
                obkv_writer.insert(id, val)?;
            } else if let Some(operation) = settings_diff.reindex_searchable_id(id) {
                operations.insert(id, operation);
                obkv_writer.insert(id, val)?;
            }
        }
        if !injected_vectors.is_empty() {
            'inject_vectors: {
                let Some(vectors_fid) = old_vectors_fid else { break 'inject_vectors };

                operations.insert(vectors_fid, DelAddOperation::DeletionAndAddition);
                obkv_writer.insert(vectors_fid, serde_json::to_vec(&injected_vectors).unwrap())?;
            }
        }

        let data = obkv_writer.into_inner()?;
        let obkv = KvReader::<FieldId>::from_slice(&data);

        if let Some(original_obkv_buffer) = original_obkv_buffer {
            original_obkv_buffer.clear();
            into_del_add_obkv(obkv, DelAddOperation::DeletionAndAddition, original_obkv_buffer)?;
        }

        if let Some(flattened_obkv_buffer) = flattened_obkv_buffer {
            // take the non-flattened version if flatten_from_fields_ids_map returns None.
            let mut fields_ids_map = settings_diff.new.fields_ids_map.clone();
            let flattened = Self::flatten_from_fields_ids_map(obkv, &mut fields_ids_map)?;
            let flattened = flattened.as_deref().map_or(obkv, KvReader::from_slice);

            flattened_obkv_buffer.clear();
            into_del_add_obkv_conditional_operation(flattened, flattened_obkv_buffer, |id| {
                operations.get(&id).copied().unwrap_or(DelAddOperation::DeletionAndAddition)
            })?;
        }

        Ok(())
    }

    /// Clear all databases. Returns a `TransformOutput` with a file that contains the documents
    /// of the index with the attributes reordered accordingly to the `FieldsIdsMap` given as argument.
    ///
    // TODO this can be done in parallel by using the rayon `ThreadPool`.
    #[tracing::instrument(
        level = "trace"
        skip(self, wtxn, settings_diff),
        target = "indexing::documents"
    )]
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
        let mut original_sorter = if settings_diff.reindex_vectors() {
            Some(create_sorter(
                grenad::SortAlgorithm::Stable,
                KeepFirst,
                self.indexer_settings.chunk_compression_type,
                self.indexer_settings.chunk_compression_level,
                self.indexer_settings.max_nb_chunks,
                self.indexer_settings.max_memory.map(|mem| mem / 2),
                true,
            ))
        } else {
            None
        };

        let readers: BTreeMap<&str, (ArroyWrapper, &RoaringBitmap)> = settings_diff
            .embedding_config_updates
            .iter()
            .filter_map(|(name, action)| {
                if let Some(WriteBackToDocuments { embedder_id, user_provided }) =
                    action.write_back()
                {
                    let reader = ArroyWrapper::new(
                        self.index.vector_arroy,
                        *embedder_id,
                        action.was_quantized,
                    );
                    Some((name.as_str(), (reader, user_provided)))
                } else {
                    None
                }
            })
            .collect();

        let old_vectors_fid = settings_diff
            .old
            .fields_ids_map
            .id(crate::vector::parsed_vectors::RESERVED_VECTORS_FIELD_NAME);

        // We initialize the sorter with the user indexing settings.
        let mut flattened_sorter =
            if settings_diff.reindex_searchable() || settings_diff.reindex_facets() {
                Some(create_sorter(
                    grenad::SortAlgorithm::Stable,
                    KeepFirst,
                    self.indexer_settings.chunk_compression_type,
                    self.indexer_settings.chunk_compression_level,
                    self.indexer_settings.max_nb_chunks,
                    self.indexer_settings.max_memory.map(|mem| mem / 2),
                    true,
                ))
            } else {
                None
            };

        if original_sorter.is_some() || flattened_sorter.is_some() {
            let modified_faceted_fields = settings_diff.modified_faceted_fields();
            let mut original_obkv_buffer = Vec::new();
            let mut flattened_obkv_buffer = Vec::new();
            let mut document_sorter_key_buffer = Vec::new();
            for result in self.index.external_documents_ids().iter(wtxn)? {
                let (external_id, docid) = result?;
                let old_obkv = self.index.documents.get(wtxn, &docid)?.ok_or(
                    InternalError::DatabaseMissingEntry { db_name: db_name::DOCUMENTS, key: None },
                )?;

                let injected_vectors: std::result::Result<
                    serde_json::Map<String, serde_json::Value>,
                    arroy::Error,
                > = readers
                    .iter()
                    .filter_map(|(name, (reader, user_provided))| {
                        if !user_provided.contains(docid) {
                            return None;
                        }
                        match reader.item_vectors(wtxn, docid) {
                            Ok(vectors) if vectors.is_empty() => None,
                            Ok(vectors) => Some(Ok((
                                name.to_string(),
                                serde_json::to_value(ExplicitVectors {
                                    embeddings: Some(
                                        VectorOrArrayOfVectors::from_array_of_vectors(vectors),
                                    ),
                                    regenerate: false,
                                })
                                .unwrap(),
                            ))),
                            Err(e) => Some(Err(e)),
                        }
                    })
                    .collect();

                let injected_vectors = injected_vectors?;

                Self::rebind_existing_document(
                    old_obkv,
                    &settings_diff,
                    &modified_faceted_fields,
                    injected_vectors,
                    old_vectors_fid,
                    Some(&mut original_obkv_buffer).filter(|_| original_sorter.is_some()),
                    Some(&mut flattened_obkv_buffer).filter(|_| flattened_sorter.is_some()),
                )?;

                if let Some(original_sorter) = original_sorter.as_mut() {
                    document_sorter_key_buffer.clear();
                    document_sorter_key_buffer.extend_from_slice(&docid.to_be_bytes());
                    document_sorter_key_buffer.extend_from_slice(external_id.as_bytes());
                    original_sorter.insert(&document_sorter_key_buffer, &original_obkv_buffer)?;
                }
                if let Some(flattened_sorter) = flattened_sorter.as_mut() {
                    flattened_sorter.insert(docid.to_be_bytes(), &flattened_obkv_buffer)?;
                }
            }
        }

        // delete all vectors from the embedders that need removal
        for (_, (reader, _)) in readers {
            let dimensions = reader.dimensions(wtxn)?;
            reader.clear(wtxn, dimensions)?;
        }

        let grenad_params = GrenadParameters {
            chunk_compression_type: self.indexer_settings.chunk_compression_type,
            chunk_compression_level: self.indexer_settings.chunk_compression_level,
            max_memory: self.indexer_settings.max_memory,
            max_nb_chunks: self.indexer_settings.max_nb_chunks, // default value, may be chosen.
        };

        // Once we have written all the documents, we merge everything into a Reader.
        let flattened_documents = match flattened_sorter {
            Some(flattened_sorter) => Some(sorter_into_reader(flattened_sorter, grenad_params)?),
            None => None,
        };
        let original_documents = match original_sorter {
            Some(original_sorter) => Some(sorter_into_reader(original_sorter, grenad_params)?),
            None => None,
        };

        Ok(TransformOutput {
            primary_key,
            field_distribution,
            settings_diff,
            documents_count,
            original_documents: original_documents.map(|od| od.into_inner().into_inner()),
            flattened_documents: flattened_documents.map(|fd| fd.into_inner().into_inner()),
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
    use grenad::MergeFunction;
    use obkv::KvReaderU16;

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
            KvReaderU16::from_slice(&buffer),
            DelAddOperation::Addition,
            &mut additive_doc_0,
        )
        .unwrap();
        additive_doc_0.insert(0, Operation::Addition as u8);
        into_del_add_obkv(
            KvReaderU16::from_slice(&buffer),
            DelAddOperation::Deletion,
            &mut deletive_doc_0,
        )
        .unwrap();
        deletive_doc_0.insert(0, Operation::Deletion as u8);
        into_del_add_obkv(
            KvReaderU16::from_slice(&buffer),
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
            KvReaderU16::from_slice(&buffer),
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
            KvReaderU16::from_slice(&buffer),
            DelAddOperation::Addition,
            &mut additive_doc_0_1,
        )
        .unwrap();
        additive_doc_0_1.insert(0, Operation::Addition as u8);

        let ret = MergeFunction::merge(
            &ObkvsMergeAdditionsAndDeletions,
            &[],
            &[Cow::from(additive_doc_0.as_slice())],
        )
        .unwrap();
        assert_eq!(*ret, additive_doc_0);

        let ret = MergeFunction::merge(
            &ObkvsMergeAdditionsAndDeletions,
            &[],
            &[Cow::from(deletive_doc_0.as_slice()), Cow::from(additive_doc_0.as_slice())],
        )
        .unwrap();
        assert_eq!(*ret, del_add_doc_0);

        let ret = MergeFunction::merge(
            &ObkvsMergeAdditionsAndDeletions,
            &[],
            &[Cow::from(additive_doc_0.as_slice()), Cow::from(deletive_doc_0.as_slice())],
        )
        .unwrap();
        assert_eq!(*ret, deletive_doc_0);

        let ret = MergeFunction::merge(
            &ObkvsMergeAdditionsAndDeletions,
            &[],
            &[
                Cow::from(additive_doc_1.as_slice()),
                Cow::from(deletive_doc_0.as_slice()),
                Cow::from(additive_doc_0.as_slice()),
            ],
        )
        .unwrap();
        assert_eq!(*ret, del_add_doc_0);

        let ret = MergeFunction::merge(
            &ObkvsMergeAdditionsAndDeletions,
            &[],
            &[Cow::from(additive_doc_1.as_slice()), Cow::from(additive_doc_0.as_slice())],
        )
        .unwrap();
        assert_eq!(*ret, additive_doc_0_1);

        let ret = MergeFunction::merge(
            &ObkvsKeepLastAdditionMergeDeletions,
            &[],
            &[Cow::from(additive_doc_1.as_slice()), Cow::from(additive_doc_0.as_slice())],
        )
        .unwrap();
        assert_eq!(*ret, additive_doc_0);

        let ret = MergeFunction::merge(
            &ObkvsKeepLastAdditionMergeDeletions,
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

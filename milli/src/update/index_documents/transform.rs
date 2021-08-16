use std::borrow::Cow;
use std::collections::btree_map::Entry;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::iter::Peekable;
use std::result::Result as StdResult;
use std::time::Instant;

use grenad::CompressionType;
use log::info;
use roaring::RoaringBitmap;
use serde_json::{Map, Value};

use super::helpers::{
    create_sorter, create_writer, keep_latest_obkv, merge_obkvs, merge_two_obkvs, MergeFn,
};
use super::IndexDocumentsMethod;
use crate::error::{InternalError, UserError};
use crate::index::db_name;
use crate::update::{AvailableDocumentsIds, UpdateIndexingStep};
use crate::{ExternalDocumentsIds, FieldDistribution, FieldId, FieldsIdsMap, Index, Result, BEU32};

const DEFAULT_PRIMARY_KEY_NAME: &str = "id";

pub struct TransformOutput {
    pub primary_key: String,
    pub fields_ids_map: FieldsIdsMap,
    pub field_distribution: FieldDistribution,
    pub external_documents_ids: ExternalDocumentsIds<'static>,
    pub new_documents_ids: RoaringBitmap,
    pub replaced_documents_ids: RoaringBitmap,
    pub documents_count: usize,
    pub documents_file: File,
}

/// Extract the external ids, deduplicate and compute the new internal documents ids
/// and fields ids, writing all the documents under their internal ids into a final file.
///
/// Outputs the new `FieldsIdsMap`, the new `UsersIdsDocumentsIds` map, the new documents ids,
/// the replaced documents ids, the number of documents in this update and the file
/// containing all those documents.
pub struct Transform<'t, 'i> {
    pub rtxn: &'t heed::RoTxn<'i>,
    pub index: &'i Index,
    pub log_every_n: Option<usize>,
    pub chunk_compression_type: CompressionType,
    pub chunk_compression_level: Option<u32>,
    pub max_nb_chunks: Option<usize>,
    pub max_memory: Option<usize>,
    pub index_documents_method: IndexDocumentsMethod,
    pub autogenerate_docids: bool,
}

fn is_primary_key(field: impl AsRef<str>) -> bool {
    field.as_ref().to_lowercase().contains(DEFAULT_PRIMARY_KEY_NAME)
}

impl Transform<'_, '_> {
    pub fn output_from_json<R, F>(self, reader: R, progress_callback: F) -> Result<TransformOutput>
    where
        R: Read,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        self.output_from_generic_json(reader, false, progress_callback)
    }

    pub fn output_from_json_stream<R, F>(
        self,
        reader: R,
        progress_callback: F,
    ) -> Result<TransformOutput>
    where
        R: Read,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        self.output_from_generic_json(reader, true, progress_callback)
    }

    fn output_from_generic_json<R, F>(
        self,
        reader: R,
        is_stream: bool,
        progress_callback: F,
    ) -> Result<TransformOutput>
    where
        R: Read,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let mut fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let external_documents_ids = self.index.external_documents_ids(self.rtxn).unwrap();

        // Deserialize the whole batch of documents in memory.
        let mut documents: Peekable<
            Box<dyn Iterator<Item = serde_json::Result<Map<String, Value>>>>,
        > = if is_stream {
            let iter = serde_json::Deserializer::from_reader(reader).into_iter();
            let iter = Box::new(iter) as Box<dyn Iterator<Item = _>>;
            iter.peekable()
        } else {
            let vec: Vec<_> = serde_json::from_reader(reader).map_err(UserError::SerdeJson)?;
            let iter = vec.into_iter().map(Ok);
            let iter = Box::new(iter) as Box<dyn Iterator<Item = _>>;
            iter.peekable()
        };

        // We extract the primary key from the first document in
        // the batch if it hasn't already been defined in the index
        let first = match documents.peek().map(StdResult::as_ref).transpose() {
            Ok(first) => first,
            Err(_) => {
                let error = documents.next().unwrap().unwrap_err();
                return Err(UserError::SerdeJson(error).into());
            }
        };

        let alternative_name =
            first.and_then(|doc| doc.keys().find(|f| is_primary_key(f)).cloned());
        let (primary_key_id, primary_key) = compute_primary_key_pair(
            self.index.primary_key(self.rtxn)?,
            &mut fields_ids_map,
            alternative_name,
            self.autogenerate_docids,
        )?;

        if documents.peek().is_none() {
            return Ok(TransformOutput {
                primary_key,
                fields_ids_map,
                field_distribution: self.index.field_distribution(self.rtxn)?,
                external_documents_ids: ExternalDocumentsIds::default(),
                new_documents_ids: RoaringBitmap::new(),
                replaced_documents_ids: RoaringBitmap::new(),
                documents_count: 0,
                documents_file: tempfile::tempfile()?,
            });
        }

        // We must choose the appropriate merge function for when two or more documents
        // with the same user id must be merged or fully replaced in the same batch.
        let merge_function = match self.index_documents_method {
            IndexDocumentsMethod::ReplaceDocuments => keep_latest_obkv,
            IndexDocumentsMethod::UpdateDocuments => merge_obkvs,
        };

        // We initialize the sorter with the user indexing settings.
        let mut sorter = create_sorter(
            merge_function,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        let mut json_buffer = Vec::new();
        let mut obkv_buffer = Vec::new();
        let mut uuid_buffer = [0; uuid::adapter::Hyphenated::LENGTH];
        let mut documents_count = 0;

        for result in documents {
            let document = result.map_err(UserError::SerdeJson)?;

            if self.log_every_n.map_or(false, |len| documents_count % len == 0) {
                progress_callback(UpdateIndexingStep::TransformFromUserIntoGenericFormat {
                    documents_seen: documents_count,
                });
            }

            obkv_buffer.clear();
            let mut writer = obkv::KvWriter::<_, FieldId>::new(&mut obkv_buffer);

            // We prepare the fields ids map with the documents keys.
            for (key, _value) in &document {
                fields_ids_map.insert(&key).ok_or(UserError::AttributeLimitReached)?;
            }

            // We retrieve the user id from the document based on the primary key name,
            // if the document id isn't present we generate a uuid.
            let external_id = match document.get(&primary_key) {
                Some(value) => match value {
                    Value::String(string) => Cow::Borrowed(string.as_str()),
                    Value::Number(number) => Cow::Owned(number.to_string()),
                    content => {
                        return Err(
                            UserError::InvalidDocumentId { document_id: content.clone() }.into()
                        )
                    }
                },
                None => {
                    if !self.autogenerate_docids {
                        return Err(UserError::MissingDocumentId { document }.into());
                    }
                    let uuid = uuid::Uuid::new_v4().to_hyphenated().encode_lower(&mut uuid_buffer);
                    Cow::Borrowed(uuid)
                }
            };

            // We iterate in the fields ids ordered.
            for (field_id, name) in fields_ids_map.iter() {
                json_buffer.clear();

                // We try to extract the value from the document and if we don't find anything
                // and this should be the document id we return the one we generated.
                if let Some(value) = document.get(name) {
                    // We serialize the attribute values.
                    serde_json::to_writer(&mut json_buffer, value)
                        .map_err(InternalError::SerdeJson)?;
                    writer.insert(field_id, &json_buffer)?;
                }
                // We validate the document id [a-zA-Z0-9\-_].
                if field_id == primary_key_id && validate_document_id(&external_id).is_none() {
                    return Err(UserError::InvalidDocumentId {
                        document_id: Value::from(external_id),
                    }
                    .into());
                }
            }

            // We use the extracted/generated user id as the key for this document.
            sorter.insert(external_id.as_bytes(), &obkv_buffer)?;
            documents_count += 1;
        }

        progress_callback(UpdateIndexingStep::TransformFromUserIntoGenericFormat {
            documents_seen: documents_count,
        });

        // Now that we have a valid sorter that contains the user id and the obkv we
        // give it to the last transforming function which returns the TransformOutput.
        self.output_from_sorter(
            sorter,
            primary_key,
            fields_ids_map,
            documents_count,
            external_documents_ids,
            progress_callback,
        )
    }

    pub fn output_from_csv<R, F>(self, reader: R, progress_callback: F) -> Result<TransformOutput>
    where
        R: Read,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let mut fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let external_documents_ids = self.index.external_documents_ids(self.rtxn).unwrap();

        let mut csv = csv::Reader::from_reader(reader);
        let headers = csv.headers().map_err(UserError::Csv)?;

        let mut fields_ids = Vec::new();
        // Generate the new fields ids based on the current fields ids and this CSV headers.
        for (i, header) in headers.iter().enumerate() {
            let id = fields_ids_map.insert(header).ok_or(UserError::AttributeLimitReached)?;
            fields_ids.push((id, i));
        }

        // Extract the position of the primary key in the current headers, None if not found.
        let primary_key_pos = match self.index.primary_key(self.rtxn)? {
            Some(primary_key) => {
                // The primary key is known so we must find the position in the CSV headers.
                headers.iter().position(|h| h == primary_key)
            }
            None => headers.iter().position(is_primary_key),
        };

        // Returns the field id in the fields ids map, create an "id" field
        // in case it is not in the current headers.
        let alternative_name = primary_key_pos.map(|pos| headers[pos].to_string());
        let (primary_key_id, primary_key_name) = compute_primary_key_pair(
            self.index.primary_key(self.rtxn)?,
            &mut fields_ids_map,
            alternative_name,
            self.autogenerate_docids,
        )?;

        // The primary key field is not present in the header, so we need to create it.
        if primary_key_pos.is_none() {
            fields_ids.push((primary_key_id, usize::max_value()));
        }

        // We sort the fields ids by the fields ids map id, this way we are sure to iterate over
        // the records fields in the fields ids map order and correctly generate the obkv.
        fields_ids.sort_unstable_by_key(|(field_id, _)| *field_id);

        // We initialize the sorter with the user indexing settings.
        let mut sorter = create_sorter(
            keep_latest_obkv,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        // We write into the sorter to merge and deduplicate the documents
        // based on the external ids.
        let mut json_buffer = Vec::new();
        let mut obkv_buffer = Vec::new();
        let mut uuid_buffer = [0; uuid::adapter::Hyphenated::LENGTH];
        let mut documents_count = 0;

        let mut record = csv::StringRecord::new();
        while csv.read_record(&mut record).map_err(UserError::Csv)? {
            obkv_buffer.clear();
            let mut writer = obkv::KvWriter::<_, FieldId>::new(&mut obkv_buffer);

            if self.log_every_n.map_or(false, |len| documents_count % len == 0) {
                progress_callback(UpdateIndexingStep::TransformFromUserIntoGenericFormat {
                    documents_seen: documents_count,
                });
            }

            // We extract the user id if we know where it is or generate an UUID V4 otherwise.
            let external_id = match primary_key_pos {
                Some(pos) => {
                    let external_id = &record[pos];
                    // We validate the document id [a-zA-Z0-9\-_].
                    match validate_document_id(&external_id) {
                        Some(valid) => valid,
                        None => {
                            return Err(UserError::InvalidDocumentId {
                                document_id: Value::from(external_id),
                            }
                            .into())
                        }
                    }
                }
                None => uuid::Uuid::new_v4().to_hyphenated().encode_lower(&mut uuid_buffer),
            };

            // When the primary_key_field_id is found in the fields ids list
            // we return the generated document id instead of the record field.
            let iter = fields_ids.iter().map(|(fi, i)| {
                let field = if *fi == primary_key_id { external_id } else { &record[*i] };
                (fi, field)
            });

            // We retrieve the field id based on the fields ids map fields ids order.
            for (field_id, field) in iter {
                // We serialize the attribute values as JSON strings.
                json_buffer.clear();
                serde_json::to_writer(&mut json_buffer, &field)
                    .map_err(InternalError::SerdeJson)?;
                writer.insert(*field_id, &json_buffer)?;
            }

            // We use the extracted/generated user id as the key for this document.
            sorter.insert(external_id, &obkv_buffer)?;
            documents_count += 1;
        }

        progress_callback(UpdateIndexingStep::TransformFromUserIntoGenericFormat {
            documents_seen: documents_count,
        });

        // Now that we have a valid sorter that contains the user id and the obkv we
        // give it to the last transforming function which returns the TransformOutput.
        self.output_from_sorter(
            sorter,
            primary_key_name,
            fields_ids_map,
            documents_count,
            external_documents_ids,
            progress_callback,
        )
    }

    /// Generate the `TransformOutput` based on the given sorter that can be generated from any
    /// format like CSV, JSON or JSON stream. This sorter must contain a key that is the document
    /// id for the user side and the value must be an obkv where keys are valid fields ids.
    fn output_from_sorter<F>(
        self,
        sorter: grenad::Sorter<MergeFn>,
        primary_key: String,
        fields_ids_map: FieldsIdsMap,
        approximate_number_of_documents: usize,
        mut external_documents_ids: ExternalDocumentsIds<'_>,
        progress_callback: F,
    ) -> Result<TransformOutput>
    where
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let documents_ids = self.index.documents_ids(self.rtxn)?;
        let mut field_distribution = self.index.field_distribution(self.rtxn)?;
        let mut available_documents_ids = AvailableDocumentsIds::from_documents_ids(&documents_ids);

        // Once we have sort and deduplicated the documents we write them into a final file.
        let mut final_sorter = create_sorter(
            |_id, obkvs| {
                if obkvs.len() == 1 {
                    Ok(obkvs[0].clone())
                } else {
                    Err(InternalError::IndexingMergingKeys { process: "documents" }.into())
                }
            },
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );
        let mut new_external_documents_ids_builder = fst::MapBuilder::memory();
        let mut replaced_documents_ids = RoaringBitmap::new();
        let mut new_documents_ids = RoaringBitmap::new();
        let mut obkv_buffer = Vec::new();

        // While we write into final file we get or generate the internal documents ids.
        let mut documents_count = 0;
        let mut iter = sorter.into_merger_iter()?;
        while let Some((external_id, update_obkv)) = iter.next()? {
            if self.log_every_n.map_or(false, |len| documents_count % len == 0) {
                progress_callback(UpdateIndexingStep::ComputeIdsAndMergeDocuments {
                    documents_seen: documents_count,
                    total_documents: approximate_number_of_documents,
                });
            }

            let (docid, obkv) = match external_documents_ids.get(external_id) {
                Some(docid) => {
                    // If we find the user id in the current external documents ids map
                    // we use it and insert it in the list of replaced documents.
                    replaced_documents_ids.insert(docid);

                    let key = BEU32::new(docid);
                    let base_obkv = self.index.documents.get(&self.rtxn, &key)?.ok_or(
                        InternalError::DatabaseMissingEntry {
                            db_name: db_name::DOCUMENTS,
                            key: None,
                        },
                    )?;

                    // we remove all the fields that were already counted
                    for (field_id, _) in base_obkv.iter() {
                        let field_name = fields_ids_map.name(field_id).unwrap();
                        if let Entry::Occupied(mut entry) =
                            field_distribution.entry(field_name.to_string())
                        {
                            match entry.get().checked_sub(1) {
                                Some(0) | None => entry.remove(),
                                Some(count) => entry.insert(count),
                            };
                        }
                    }

                    // Depending on the update indexing method we will merge
                    // the document update with the current document or not.
                    match self.index_documents_method {
                        IndexDocumentsMethod::ReplaceDocuments => (docid, update_obkv),
                        IndexDocumentsMethod::UpdateDocuments => {
                            let update_obkv = obkv::KvReader::new(update_obkv);
                            merge_two_obkvs(base_obkv, update_obkv, &mut obkv_buffer);
                            (docid, obkv_buffer.as_slice())
                        }
                    }
                }
                None => {
                    // If this user id is new we add it to the external documents ids map
                    // for new ids and into the list of new documents.
                    let new_docid =
                        available_documents_ids.next().ok_or(UserError::DocumentLimitReached)?;
                    new_external_documents_ids_builder.insert(external_id, new_docid as u64)?;
                    new_documents_ids.insert(new_docid);
                    (new_docid, update_obkv)
                }
            };

            // We insert the document under the documents ids map into the final file.
            final_sorter.insert(docid.to_be_bytes(), obkv)?;
            documents_count += 1;

            let reader = obkv::KvReader::new(obkv);
            for (field_id, _) in reader.iter() {
                let field_name = fields_ids_map.name(field_id).unwrap();
                *field_distribution.entry(field_name.to_string()).or_default() += 1;
            }
        }

        progress_callback(UpdateIndexingStep::ComputeIdsAndMergeDocuments {
            documents_seen: documents_count,
            total_documents: documents_count,
        });

        // We create a final writer to write the new documents in order from the sorter.
        let file = tempfile::tempfile()?;
        let mut writer =
            create_writer(self.chunk_compression_type, self.chunk_compression_level, file)?;

        // Once we have written all the documents into the final sorter, we write the documents
        // into this writer, extract the file and reset the seek to be able to read it again.
        final_sorter.write_into(&mut writer)?;
        let mut documents_file = writer.into_inner()?;
        documents_file.seek(SeekFrom::Start(0))?;

        let before_docids_merging = Instant::now();
        // We merge the new external ids with existing external documents ids.
        let new_external_documents_ids = new_external_documents_ids_builder.into_map();
        external_documents_ids.insert_ids(&new_external_documents_ids)?;

        info!("Documents external merging took {:.02?}", before_docids_merging.elapsed());

        Ok(TransformOutput {
            primary_key,
            fields_ids_map,
            field_distribution,
            external_documents_ids: external_documents_ids.into_static(),
            new_documents_ids,
            replaced_documents_ids,
            documents_count,
            documents_file,
        })
    }

    /// Returns a `TransformOutput` with a file that contains the documents of the index
    /// with the attributes reordered accordingly to the `FieldsIdsMap` given as argument.
    // TODO this can be done in parallel by using the rayon `ThreadPool`.
    pub fn remap_index_documents(
        self,
        primary_key: String,
        old_fields_ids_map: FieldsIdsMap,
        new_fields_ids_map: FieldsIdsMap,
    ) -> Result<TransformOutput> {
        let field_distribution = self.index.field_distribution(self.rtxn)?;
        let external_documents_ids = self.index.external_documents_ids(self.rtxn)?;
        let documents_ids = self.index.documents_ids(self.rtxn)?;
        let documents_count = documents_ids.len() as usize;

        // We create a final writer to write the new documents in order from the sorter.
        let file = tempfile::tempfile()?;
        let mut writer =
            create_writer(self.chunk_compression_type, self.chunk_compression_level, file)?;

        let mut obkv_buffer = Vec::new();
        for result in self.index.documents.iter(self.rtxn)? {
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
            writer.insert(docid.to_be_bytes(), buffer)?;
        }

        // Once we have written all the documents, we extract
        // the file and reset the seek to be able to read it again.
        let mut documents_file = writer.into_inner()?;
        documents_file.seek(SeekFrom::Start(0))?;

        Ok(TransformOutput {
            primary_key,
            fields_ids_map: new_fields_ids_map,
            field_distribution,
            external_documents_ids: external_documents_ids.into_static(),
            new_documents_ids: documents_ids,
            replaced_documents_ids: RoaringBitmap::default(),
            documents_count,
            documents_file,
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

#[cfg(test)]
mod test {
    use super::*;

    mod compute_primary_key {
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
        }

        #[test]
        fn should_return_alternative_if_primary_is_none() {
            let mut fields_map = FieldsIdsMap::new();
            let result =
                compute_primary_key_pair(None, &mut fields_map, Some("tata".to_string()), false);
            assert_eq!(result.unwrap(), (0, "tata".to_string()));
            assert_eq!(fields_map.len(), 1);
        }

        #[test]
        fn should_return_default_if_both_are_none() {
            let mut fields_map = FieldsIdsMap::new();
            let result = compute_primary_key_pair(None, &mut fields_map, None, true);
            assert_eq!(result.unwrap(), (0, "id".to_string()));
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
}

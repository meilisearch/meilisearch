use std::collections::btree_map::Entry;

use fst::IntoStreamer;
use heed::types::{ByteSlice, Str};
use heed::{BytesDecode, BytesEncode, Database};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use super::ClearDocuments;
use crate::error::{InternalError, SerializationError, UserError};
use crate::heed_codec::facet::{
    FacetLevelValueU32Codec, FacetStringLevelZeroValueCodec, FacetStringZeroBoundsValueCodec,
};
use crate::heed_codec::CboRoaringBitmapCodec;
use crate::index::{db_name, main_key};
use crate::{
    DocumentId, ExternalDocumentsIds, FieldId, FieldIdMapMissingEntry, Index, Result,
    RoaringBitmapCodec, SmallString32, BEU32,
};

const DELETE_DOCUMENTS_THRESHOLD: u64 = 100_000;

pub struct DeleteDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    external_documents_ids: ExternalDocumentsIds<'static>,
    to_delete_docids: RoaringBitmap,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentDeletionResult {
    pub deleted_documents: u64,
    pub remaining_documents: u64,
}

impl<'t, 'u, 'i> DeleteDocuments<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> Result<DeleteDocuments<'t, 'u, 'i>> {
        let external_documents_ids = index.external_documents_ids(wtxn)?.into_static();

        Ok(DeleteDocuments {
            wtxn,
            index,
            external_documents_ids,
            to_delete_docids: RoaringBitmap::new(),
        })
    }

    pub fn delete_document(&mut self, docid: u32) {
        self.to_delete_docids.insert(docid);
    }

    pub fn delete_documents(&mut self, docids: &RoaringBitmap) {
        self.to_delete_docids |= docids;
    }

    pub fn delete_external_id(&mut self, external_id: &str) -> Option<u32> {
        let docid = self.external_documents_ids.get(external_id)?;
        self.delete_document(docid);
        Some(docid)
    }

    pub fn execute(mut self) -> Result<DocumentDeletionResult> {
        self.index.set_updated_at(self.wtxn, &OffsetDateTime::now_utc())?;
        // We retrieve the current documents ids that are in the database.
        let mut documents_ids = self.index.documents_ids(self.wtxn)?;
        let mut soft_deleted_docids = self.index.soft_deleted_documents_ids(self.wtxn)?;
        let current_documents_ids_len = documents_ids.len();

        // We can and must stop removing documents in a database that is empty.
        if documents_ids.is_empty() {
            // but if there was still documents to delete we clear the database entirely
            if !soft_deleted_docids.is_empty() {
                ClearDocuments::new(self.wtxn, self.index).execute()?;
            }
            return Ok(DocumentDeletionResult { deleted_documents: 0, remaining_documents: 0 });
        }

        // We remove the documents ids that we want to delete
        // from the documents in the database and write them back.
        documents_ids -= &self.to_delete_docids;
        self.index.put_documents_ids(self.wtxn, &documents_ids)?;

        // We can execute a ClearDocuments operation when the number of documents
        // to delete is exactly the number of documents in the database.
        if current_documents_ids_len == self.to_delete_docids.len() {
            let remaining_documents = ClearDocuments::new(self.wtxn, self.index).execute()?;
            return Ok(DocumentDeletionResult {
                deleted_documents: current_documents_ids_len,
                remaining_documents,
            });
        }

        let fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
        let mut field_distribution = self.index.field_distribution(self.wtxn)?;

        // we update the field distribution
        for docid in self.to_delete_docids.iter() {
            let key = BEU32::new(docid);
            let document =
                self.index.documents.get(self.wtxn, &key)?.ok_or(
                    InternalError::DatabaseMissingEntry { db_name: "documents", key: None },
                )?;
            for (fid, _value) in document.iter() {
                let field_name =
                    fields_ids_map.name(fid).ok_or(FieldIdMapMissingEntry::FieldId {
                        field_id: fid,
                        process: "delete documents",
                    })?;
                if let Entry::Occupied(mut entry) = field_distribution.entry(field_name.to_string())
                {
                    match entry.get().checked_sub(1) {
                        Some(0) | None => entry.remove(),
                        Some(count) => entry.insert(count),
                    };
                }
            }
        }

        self.index.put_field_distribution(self.wtxn, &field_distribution)?;

        soft_deleted_docids |= &self.to_delete_docids;

        // if we have less documents to delete than the threshold we simply save them in
        // the `soft_deleted_documents_ids` bitmap and early exit.
        if soft_deleted_docids.len() < DELETE_DOCUMENTS_THRESHOLD {
            self.index.put_soft_deleted_documents_ids(self.wtxn, &soft_deleted_docids)?;
            return Ok(DocumentDeletionResult {
                deleted_documents: self.to_delete_docids.len(),
                remaining_documents: documents_ids.len(),
            });
        }

        // There is more than documents to delete than the threshold we needs to delete them all
        self.to_delete_docids = soft_deleted_docids;
        // and we can reset the soft deleted bitmap
        self.index.put_soft_deleted_documents_ids(self.wtxn, &RoaringBitmap::new())?;

        let primary_key = self.index.primary_key(self.wtxn)?.ok_or_else(|| {
            InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::PRIMARY_KEY_KEY),
            }
        })?;

        // Since we already checked if the DB was empty, if we can't find the primary key, then
        // something is wrong, and we must return an error.
        let id_field = match fields_ids_map.id(primary_key) {
            Some(field) => field,
            None => return Err(UserError::MissingPrimaryKey.into()),
        };

        let Index {
            env: _env,
            main: _main,
            word_docids,
            exact_word_docids,
            word_prefix_docids,
            exact_word_prefix_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            field_id_word_count_docids,
            word_prefix_pair_proximity_docids,
            word_position_docids,
            word_prefix_position_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            documents,
        } = self.index;

        // Retrieve the words and the external documents ids contained in the documents.
        let mut words = Vec::new();
        let mut external_ids = Vec::new();
        for docid in &self.to_delete_docids {
            // We create an iterator to be able to get the content and delete the document
            // content itself. It's faster to acquire a cursor to get and delete,
            // as we avoid traversing the LMDB B-Tree two times but only once.
            let key = BEU32::new(docid);
            let mut iter = documents.range_mut(self.wtxn, &(key..=key))?;
            if let Some((_key, obkv)) = iter.next().transpose()? {
                if let Some(content) = obkv.get(id_field) {
                    let external_id = match serde_json::from_slice(content).unwrap() {
                        Value::String(string) => SmallString32::from(string.as_str()),
                        Value::Number(number) => SmallString32::from(number.to_string()),
                        document_id => {
                            return Err(UserError::InvalidDocumentId { document_id }.into())
                        }
                    };
                    external_ids.push(external_id);
                }
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.del_current()? };
            }
            drop(iter);

            // We iterate through the words positions of the document id,
            // retrieve the word and delete the positions.
            let mut iter = docid_word_positions.prefix_iter_mut(self.wtxn, &(docid, ""))?;
            while let Some(result) = iter.next() {
                let ((_docid, word), _positions) = result?;
                // This boolean will indicate if we must remove this word from the words FST.
                words.push((SmallString32::from(word), false));
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.del_current()? };
            }
        }

        // We create the FST map of the external ids that we must delete.
        external_ids.sort_unstable();
        let external_ids_to_delete = fst::Set::from_iter(external_ids)?;

        // We acquire the current external documents ids map...
        let mut new_external_documents_ids = self.index.external_documents_ids(self.wtxn)?;
        // ...and remove the to-delete external ids.
        new_external_documents_ids.delete_ids(external_ids_to_delete)?;

        // We write the new external ids into the main database.
        let new_external_documents_ids = new_external_documents_ids.into_static();
        self.index.put_external_documents_ids(self.wtxn, &new_external_documents_ids)?;

        // Maybe we can improve the get performance of the words
        // if we sort the words first, keeping the LMDB pages in cache.
        words.sort_unstable();

        // We iterate over the words and delete the documents ids
        // from the word docids database.
        for (word, must_remove) in &mut words {
            remove_from_word_docids(
                self.wtxn,
                word_docids,
                word.as_str(),
                must_remove,
                &self.to_delete_docids,
            )?;

            remove_from_word_docids(
                self.wtxn,
                exact_word_docids,
                word.as_str(),
                must_remove,
                &self.to_delete_docids,
            )?;
        }

        // We construct an FST set that contains the words to delete from the words FST.
        let words_to_delete =
            words.iter().filter_map(
                |(word, must_remove)| {
                    if *must_remove {
                        Some(word.as_str())
                    } else {
                        None
                    }
                },
            );
        let words_to_delete = fst::Set::from_iter(words_to_delete)?;

        let new_words_fst = {
            // We retrieve the current words FST from the database.
            let words_fst = self.index.words_fst(self.wtxn)?;
            let difference = words_fst.op().add(&words_to_delete).difference();

            // We stream the new external ids that does no more contains the to-delete external ids.
            let mut new_words_fst_builder = fst::SetBuilder::memory();
            new_words_fst_builder.extend_stream(difference.into_stream())?;

            // We create an words FST set from the above builder.
            new_words_fst_builder.into_set()
        };

        // We write the new words FST into the main database.
        self.index.put_words_fst(self.wtxn, &new_words_fst)?;

        let prefixes_to_delete =
            remove_from_word_prefix_docids(self.wtxn, word_prefix_docids, &self.to_delete_docids)?;

        let exact_prefix_to_delete = remove_from_word_prefix_docids(
            self.wtxn,
            exact_word_prefix_docids,
            &self.to_delete_docids,
        )?;

        let all_prefixes_to_delete = prefixes_to_delete.op().add(&exact_prefix_to_delete).union();

        // We compute the new prefix FST and write it only if there is a change.
        if !prefixes_to_delete.is_empty() || !exact_prefix_to_delete.is_empty() {
            let new_words_prefixes_fst = {
                // We retrieve the current words prefixes FST from the database.
                let words_prefixes_fst = self.index.words_prefixes_fst(self.wtxn)?;
                let difference =
                    words_prefixes_fst.op().add(all_prefixes_to_delete.into_stream()).difference();

                // We stream the new external ids that does no more contains the to-delete external ids.
                let mut new_words_prefixes_fst_builder = fst::SetBuilder::memory();
                new_words_prefixes_fst_builder.extend_stream(difference.into_stream())?;

                // We create an words FST set from the above builder.
                new_words_prefixes_fst_builder.into_set()
            };

            // We write the new words prefixes FST into the main database.
            self.index.put_words_prefixes_fst(self.wtxn, &new_words_prefixes_fst)?;
        }

        // We delete the documents ids from the word prefix pair proximity database docids
        // and remove the empty pairs too.
        let db = word_prefix_pair_proximity_docids.remap_key_type::<ByteSlice>();
        let mut iter = db.iter_mut(self.wtxn)?;
        while let Some(result) = iter.next() {
            let (key, mut docids) = result?;
            let previous_len = docids.len();
            docids -= &self.to_delete_docids;
            if docids.is_empty() {
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.del_current()? };
            } else if docids.len() != previous_len {
                let key = key.to_owned();
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.put_current(&key, &docids)? };
            }
        }

        drop(iter);

        // We delete the documents ids that are under the pairs of words,
        // it is faster and use no memory to iterate over all the words pairs than
        // to compute the cartesian product of every words of the deleted documents.
        let mut iter =
            word_pair_proximity_docids.remap_key_type::<ByteSlice>().iter_mut(self.wtxn)?;
        while let Some(result) = iter.next() {
            let (bytes, mut docids) = result?;
            let previous_len = docids.len();
            docids -= &self.to_delete_docids;
            if docids.is_empty() {
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.del_current()? };
            } else if docids.len() != previous_len {
                let bytes = bytes.to_owned();
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.put_current(&bytes, &docids)? };
            }
        }

        drop(iter);

        // We delete the documents ids that are under the word level position docids.
        let mut iter = word_position_docids.iter_mut(self.wtxn)?.remap_key_type::<ByteSlice>();
        while let Some(result) = iter.next() {
            let (bytes, mut docids) = result?;
            let previous_len = docids.len();
            docids -= &self.to_delete_docids;
            if docids.is_empty() {
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.del_current()? };
            } else if docids.len() != previous_len {
                let bytes = bytes.to_owned();
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.put_current(&bytes, &docids)? };
            }
        }

        drop(iter);

        // We delete the documents ids that are under the word prefix level position docids.
        let mut iter =
            word_prefix_position_docids.iter_mut(self.wtxn)?.remap_key_type::<ByteSlice>();
        while let Some(result) = iter.next() {
            let (bytes, mut docids) = result?;
            let previous_len = docids.len();
            docids -= &self.to_delete_docids;
            if docids.is_empty() {
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.del_current()? };
            } else if docids.len() != previous_len {
                let bytes = bytes.to_owned();
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.put_current(&bytes, &docids)? };
            }
        }

        drop(iter);

        // Remove the documents ids from the field id word count database.
        let mut iter = field_id_word_count_docids.iter_mut(self.wtxn)?;
        while let Some((key, mut docids)) = iter.next().transpose()? {
            let previous_len = docids.len();
            docids -= &self.to_delete_docids;
            if docids.is_empty() {
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.del_current()? };
            } else if docids.len() != previous_len {
                let key = key.to_owned();
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.put_current(&key, &docids)? };
            }
        }

        drop(iter);

        if let Some(mut rtree) = self.index.geo_rtree(self.wtxn)? {
            let mut geo_faceted_doc_ids = self.index.geo_faceted_documents_ids(self.wtxn)?;

            let (points_to_remove, docids_to_remove): (Vec<_>, RoaringBitmap) = rtree
                .iter()
                .filter(|&point| self.to_delete_docids.contains(point.data.0))
                .cloned()
                .map(|point| (point, point.data.0))
                .unzip();
            points_to_remove.iter().for_each(|point| {
                rtree.remove(&point);
            });
            geo_faceted_doc_ids -= docids_to_remove;

            self.index.put_geo_rtree(self.wtxn, &rtree)?;
            self.index.put_geo_faceted_documents_ids(self.wtxn, &geo_faceted_doc_ids)?;
        }

        // We delete the documents ids that are under the facet field id values.
        remove_docids_from_facet_field_id_number_docids(
            self.wtxn,
            facet_id_f64_docids,
            &self.to_delete_docids,
        )?;

        remove_docids_from_facet_field_id_string_docids(
            self.wtxn,
            facet_id_string_docids,
            &self.to_delete_docids,
        )?;

        // Remove the documents ids from the faceted documents ids.
        for field_id in self.index.faceted_fields_ids(self.wtxn)? {
            // Remove docids from the number faceted documents ids
            let mut docids = self.index.number_faceted_documents_ids(self.wtxn, field_id)?;
            docids -= &self.to_delete_docids;
            self.index.put_number_faceted_documents_ids(self.wtxn, field_id, &docids)?;

            remove_docids_from_field_id_docid_facet_value(
                self.wtxn,
                field_id_docid_facet_f64s,
                field_id,
                &self.to_delete_docids,
                |(_fid, docid, _value)| docid,
            )?;

            // Remove docids from the string faceted documents ids
            let mut docids = self.index.string_faceted_documents_ids(self.wtxn, field_id)?;
            docids -= &self.to_delete_docids;
            self.index.put_string_faceted_documents_ids(self.wtxn, field_id, &docids)?;

            remove_docids_from_field_id_docid_facet_value(
                self.wtxn,
                field_id_docid_facet_strings,
                field_id,
                &self.to_delete_docids,
                |(_fid, docid, _value)| docid,
            )?;
        }

        Ok(DocumentDeletionResult {
            deleted_documents: self.to_delete_docids.len(),
            remaining_documents: documents_ids.len(),
        })
    }
}

fn remove_from_word_prefix_docids(
    txn: &mut heed::RwTxn,
    db: &Database<Str, RoaringBitmapCodec>,
    to_remove: &RoaringBitmap,
) -> Result<fst::Set<Vec<u8>>> {
    let mut prefixes_to_delete = fst::SetBuilder::memory();

    // We iterate over the word prefix docids database and remove the deleted documents ids
    // from every docids lists. We register the empty prefixes in an fst Set for futur deletion.
    let mut iter = db.iter_mut(txn)?;
    while let Some(result) = iter.next() {
        let (prefix, mut docids) = result?;
        let prefix = prefix.to_owned();
        let previous_len = docids.len();
        docids -= to_remove;
        if docids.is_empty() {
            // safety: we don't keep references from inside the LMDB database.
            unsafe { iter.del_current()? };
            prefixes_to_delete.insert(prefix)?;
        } else if docids.len() != previous_len {
            // safety: we don't keep references from inside the LMDB database.
            unsafe { iter.put_current(&prefix, &docids)? };
        }
    }

    Ok(prefixes_to_delete.into_set())
}

fn remove_from_word_docids(
    txn: &mut heed::RwTxn,
    db: &heed::Database<Str, RoaringBitmapCodec>,
    word: &str,
    must_remove: &mut bool,
    to_remove: &RoaringBitmap,
) -> Result<()> {
    // We create an iterator to be able to get the content and delete the word docids.
    // It's faster to acquire a cursor to get and delete or put, as we avoid traversing
    // the LMDB B-Tree two times but only once.
    let mut iter = db.prefix_iter_mut(txn, &word)?;
    if let Some((key, mut docids)) = iter.next().transpose()? {
        if key == word {
            let previous_len = docids.len();
            docids -= to_remove;
            if docids.is_empty() {
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.del_current()? };
                *must_remove = true;
            } else if docids.len() != previous_len {
                let key = key.to_owned();
                // safety: we don't keep references from inside the LMDB database.
                unsafe { iter.put_current(&key, &docids)? };
            }
        }
    }

    Ok(())
}

fn remove_docids_from_field_id_docid_facet_value<'a, C, K, F, DC, V>(
    wtxn: &'a mut heed::RwTxn,
    db: &heed::Database<C, DC>,
    field_id: FieldId,
    to_remove: &RoaringBitmap,
    convert: F,
) -> heed::Result<()>
where
    C: heed::BytesDecode<'a, DItem = K>,
    DC: heed::BytesDecode<'a, DItem = V>,
    F: Fn(K) -> DocumentId,
{
    let mut iter = db
        .remap_key_type::<ByteSlice>()
        .prefix_iter_mut(wtxn, &field_id.to_be_bytes())?
        .remap_key_type::<C>();

    while let Some(result) = iter.next() {
        let (key, _) = result?;
        if to_remove.contains(convert(key)) {
            // safety: we don't keep references from inside the LMDB database.
            unsafe { iter.del_current()? };
        }
    }

    Ok(())
}

fn remove_docids_from_facet_field_id_string_docids<'a, C, D>(
    wtxn: &'a mut heed::RwTxn,
    db: &heed::Database<C, D>,
    to_remove: &RoaringBitmap,
) -> crate::Result<()> {
    let db_name = Some(crate::index::db_name::FACET_ID_STRING_DOCIDS);
    let mut iter = db.remap_types::<ByteSlice, ByteSlice>().iter_mut(wtxn)?;
    while let Some(result) = iter.next() {
        let (key, val) = result?;
        match FacetLevelValueU32Codec::bytes_decode(key) {
            Some(_) => {
                // If we are able to parse this key it means it is a facet string group
                // level key. We must then parse the value using the appropriate codec.
                let (group, mut docids) =
                    FacetStringZeroBoundsValueCodec::<CboRoaringBitmapCodec>::bytes_decode(val)
                        .ok_or_else(|| SerializationError::Decoding { db_name })?;

                let previous_len = docids.len();
                docids -= to_remove;
                if docids.is_empty() {
                    // safety: we don't keep references from inside the LMDB database.
                    unsafe { iter.del_current()? };
                } else if docids.len() != previous_len {
                    let key = key.to_owned();
                    let val = &(group, docids);
                    let value_bytes =
                        FacetStringZeroBoundsValueCodec::<CboRoaringBitmapCodec>::bytes_encode(val)
                            .ok_or_else(|| SerializationError::Encoding { db_name })?;

                    // safety: we don't keep references from inside the LMDB database.
                    unsafe { iter.put_current(&key, &value_bytes)? };
                }
            }
            None => {
                // The key corresponds to a level zero facet string.
                let (original_value, mut docids) =
                    FacetStringLevelZeroValueCodec::bytes_decode(val)
                        .ok_or_else(|| SerializationError::Decoding { db_name })?;

                let previous_len = docids.len();
                docids -= to_remove;
                if docids.is_empty() {
                    // safety: we don't keep references from inside the LMDB database.
                    unsafe { iter.del_current()? };
                } else if docids.len() != previous_len {
                    let key = key.to_owned();
                    let val = &(original_value, docids);
                    let value_bytes = FacetStringLevelZeroValueCodec::bytes_encode(val)
                        .ok_or_else(|| SerializationError::Encoding { db_name })?;

                    // safety: we don't keep references from inside the LMDB database.
                    unsafe { iter.put_current(&key, &value_bytes)? };
                }
            }
        }
    }

    Ok(())
}

fn remove_docids_from_facet_field_id_number_docids<'a, C>(
    wtxn: &'a mut heed::RwTxn,
    db: &heed::Database<C, CboRoaringBitmapCodec>,
    to_remove: &RoaringBitmap,
) -> heed::Result<()>
where
    C: heed::BytesDecode<'a> + heed::BytesEncode<'a>,
{
    let mut iter = db.remap_key_type::<ByteSlice>().iter_mut(wtxn)?;
    while let Some(result) = iter.next() {
        let (bytes, mut docids) = result?;
        let previous_len = docids.len();
        docids -= to_remove;
        if docids.is_empty() {
            // safety: we don't keep references from inside the LMDB database.
            unsafe { iter.del_current()? };
        } else if docids.len() != previous_len {
            let bytes = bytes.to_owned();
            // safety: we don't keep references from inside the LMDB database.
            unsafe { iter.put_current(&bytes, &docids)? };
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use heed::{EnvOpenOptions, RwTxn};
    use maplit::hashset;

    use super::*;
    use crate::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
    use crate::Filter;

    fn insert_documents<'t, R: std::io::Read + std::io::Seek>(
        wtxn: &mut RwTxn<'t, '_>,
        index: &'t Index,
        documents: crate::documents::DocumentBatchReader<R>,
    ) {
        let config = IndexerConfig::default();
        let indexing_config = IndexDocumentsConfig::default();
        let mut builder =
            IndexDocuments::new(wtxn, &index, &config, indexing_config, |_| ()).unwrap();
        builder.add_documents(documents).unwrap();
        builder.execute().unwrap();
    }

    fn delete_documents<'t>(
        wtxn: &mut RwTxn<'t, '_>,
        index: &'t Index,
        external_ids: &[&str],
    ) -> Vec<u32> {
        let external_document_ids = index.external_documents_ids(&wtxn).unwrap();
        let ids_to_delete: Vec<u32> = external_ids
            .iter()
            .map(|id| external_document_ids.get(id.as_bytes()).unwrap())
            .collect();

        // Delete some documents.
        let mut builder = DeleteDocuments::new(wtxn, index).unwrap();
        external_ids.iter().for_each(|id| drop(builder.delete_external_id(id)));
        builder.execute().unwrap();

        ids_to_delete
    }

    #[test]
    fn delete_documents_with_numbers_as_primary_key() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let content = documents!([
            { "id": 0, "name": "kevin", "object": { "key1": "value1", "key2": "value2" } },
            { "id": 1, "name": "kevina", "array": ["I", "am", "fine"] },
            { "id": 2, "name": "benoit", "array_of_object": [{ "wow": "amazing" }] }
        ]);
        let config = IndexerConfig::default();
        let indexing_config = IndexDocumentsConfig::default();
        let mut builder =
            IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ()).unwrap();
        builder.add_documents(content).unwrap();
        builder.execute().unwrap();

        // delete those documents, ids are synchronous therefore 0, 1, and 2.
        let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
        builder.delete_document(0);
        builder.delete_document(1);
        builder.delete_document(2);
        builder.execute().unwrap();

        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        assert!(index.field_distribution(&rtxn).unwrap().is_empty());
    }

    #[test]
    fn delete_documents_with_strange_primary_key() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let content = documents!([
            { "mysuperid": 0, "name": "kevin" },
            { "mysuperid": 1, "name": "kevina" },
            { "mysuperid": 2, "name": "benoit" }
        ]);

        let config = IndexerConfig::default();
        let indexing_config = IndexDocumentsConfig::default();
        let mut builder =
            IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ()).unwrap();
        builder.add_documents(content).unwrap();
        builder.execute().unwrap();

        // Delete not all of the documents but some of them.
        let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
        builder.delete_external_id("0");
        builder.delete_external_id("1");
        builder.execute().unwrap();

        wtxn.commit().unwrap();
    }

    #[test]
    fn filtered_placeholder_search_should_not_return_deleted_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let config = IndexerConfig::default();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_primary_key(S("docid"));
        builder.set_filterable_fields(hashset! { S("label") });
        builder.execute(|_| ()).unwrap();

        let content = documents!([
            { "docid": "1_4",  "label": "sign" },
            { "docid": "1_5",  "label": "letter" },
            { "docid": "1_7",  "label": "abstract,cartoon,design,pattern" },
            { "docid": "1_36", "label": "drawing,painting,pattern" },
            { "docid": "1_37", "label": "art,drawing,outdoor" },
            { "docid": "1_38", "label": "aquarium,art,drawing" },
            { "docid": "1_39", "label": "abstract" },
            { "docid": "1_40", "label": "cartoon" },
            { "docid": "1_41", "label": "art,drawing" },
            { "docid": "1_42", "label": "art,pattern" },
            { "docid": "1_43", "label": "abstract,art,drawing,pattern" },
            { "docid": "1_44", "label": "drawing" },
            { "docid": "1_45", "label": "art" },
            { "docid": "1_46", "label": "abstract,colorfulness,pattern" },
            { "docid": "1_47", "label": "abstract,pattern" },
            { "docid": "1_52", "label": "abstract,cartoon" },
            { "docid": "1_57", "label": "abstract,drawing,pattern" },
            { "docid": "1_58", "label": "abstract,art,cartoon" },
            { "docid": "1_68", "label": "design" },
            { "docid": "1_69", "label": "geometry" }
        ]);

        insert_documents(&mut wtxn, &index, content);
        delete_documents(&mut wtxn, &index, &["1_4"]);

        // Placeholder search with filter
        let filter = Filter::from_str("label = sign").unwrap().unwrap();
        let results = index.search(&wtxn).filter(filter).execute().unwrap();
        assert!(results.documents_ids.is_empty());

        wtxn.commit().unwrap();
    }

    #[test]
    fn placeholder_search_should_not_return_deleted_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let config = IndexerConfig::default();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_primary_key(S("docid"));
        builder.execute(|_| ()).unwrap();

        let content = documents!([
            { "docid": "1_4",  "label": "sign" },
            { "docid": "1_5",  "label": "letter" },
            { "docid": "1_7",  "label": "abstract,cartoon,design,pattern" },
            { "docid": "1_36", "label": "drawing,painting,pattern" },
            { "docid": "1_37", "label": "art,drawing,outdoor" },
            { "docid": "1_38", "label": "aquarium,art,drawing" },
            { "docid": "1_39", "label": "abstract" },
            { "docid": "1_40", "label": "cartoon" },
            { "docid": "1_41", "label": "art,drawing" },
            { "docid": "1_42", "label": "art,pattern" },
            { "docid": "1_43", "label": "abstract,art,drawing,pattern" },
            { "docid": "1_44", "label": "drawing" },
            { "docid": "1_45", "label": "art" },
            { "docid": "1_46", "label": "abstract,colorfulness,pattern" },
            { "docid": "1_47", "label": "abstract,pattern" },
            { "docid": "1_52", "label": "abstract,cartoon" },
            { "docid": "1_57", "label": "abstract,drawing,pattern" },
            { "docid": "1_58", "label": "abstract,art,cartoon" },
            { "docid": "1_68", "label": "design" },
            { "docid": "1_69", "label": "geometry" }
        ]);

        insert_documents(&mut wtxn, &index, content);
        let deleted_internal_ids = delete_documents(&mut wtxn, &index, &["1_4"]);

        // Placeholder search
        let results = index.search(&wtxn).execute().unwrap();
        assert!(!results.documents_ids.is_empty());
        for id in results.documents_ids.iter() {
            assert!(
                !deleted_internal_ids.contains(&id),
                "The document {} was supposed to be deleted",
                id
            );
        }

        wtxn.commit().unwrap();
    }

    #[test]
    fn search_should_not_return_deleted_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let config = IndexerConfig::default();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_primary_key(S("docid"));
        builder.execute(|_| ()).unwrap();

        let content = documents!([
            {"docid": "1_4", "label": "sign"},
            {"docid": "1_5", "label": "letter"},
            {"docid": "1_7", "label": "abstract,cartoon,design,pattern"},
            {"docid": "1_36","label": "drawing,painting,pattern"},
            {"docid": "1_37","label": "art,drawing,outdoor"},
            {"docid": "1_38","label": "aquarium,art,drawing"},
            {"docid": "1_39","label": "abstract"},
            {"docid": "1_40","label": "cartoon"},
            {"docid": "1_41","label": "art,drawing"},
            {"docid": "1_42","label": "art,pattern"},
            {"docid": "1_43","label": "abstract,art,drawing,pattern"},
            {"docid": "1_44","label": "drawing"},
            {"docid": "1_45","label": "art"},
            {"docid": "1_46","label": "abstract,colorfulness,pattern"},
            {"docid": "1_47","label": "abstract,pattern"},
            {"docid": "1_52","label": "abstract,cartoon"},
            {"docid": "1_57","label": "abstract,drawing,pattern"},
            {"docid": "1_58","label": "abstract,art,cartoon"},
            {"docid": "1_68","label": "design"},
            {"docid": "1_69","label": "geometry"}
        ]);

        insert_documents(&mut wtxn, &index, content);
        let deleted_internal_ids = delete_documents(&mut wtxn, &index, &["1_7", "1_52"]);

        // search for abstract
        let results = index.search(&wtxn).query("abstract").execute().unwrap();
        assert!(!results.documents_ids.is_empty());
        for id in results.documents_ids.iter() {
            assert!(
                !deleted_internal_ids.contains(&id),
                "The document {} was supposed to be deleted",
                id
            );
        }

        wtxn.commit().unwrap();
    }

    #[test]
    fn geo_filtered_placeholder_search_should_not_return_deleted_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let config = IndexerConfig::default();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_primary_key(S("id"));
        builder.set_filterable_fields(hashset!(S("_geo")));
        builder.set_sortable_fields(hashset!(S("_geo")));
        builder.execute(|_| ()).unwrap();

        let content = documents!([
            { "id": "1",  "city": "Lille",             "_geo": { "lat": 50.6299, "lng": 3.0569 } },
            { "id": "2",  "city": "Mons-en-Barœul",    "_geo": { "lat": 50.6415, "lng": 3.1106 } },
            { "id": "3",  "city": "Hellemmes",         "_geo": { "lat": 50.6312, "lng": 3.1106 } },
            { "id": "4",  "city": "Villeneuve-d'Ascq", "_geo": { "lat": 50.6224, "lng": 3.1476 } },
            { "id": "5",  "city": "Hem",               "_geo": { "lat": 50.6552, "lng": 3.1897 } },
            { "id": "6",  "city": "Roubaix",           "_geo": { "lat": 50.6924, "lng": 3.1763 } },
            { "id": "7",  "city": "Tourcoing",         "_geo": { "lat": 50.7263, "lng": 3.1541 } },
            { "id": "8",  "city": "Mouscron",          "_geo": { "lat": 50.7453, "lng": 3.2206 } },
            { "id": "9",  "city": "Tournai",           "_geo": { "lat": 50.6053, "lng": 3.3758 } },
            { "id": "10", "city": "Ghent",             "_geo": { "lat": 51.0537, "lng": 3.6957 } },
            { "id": "11", "city": "Brussels",          "_geo": { "lat": 50.8466, "lng": 4.3370 } },
            { "id": "12", "city": "Charleroi",         "_geo": { "lat": 50.4095, "lng": 4.4347 } },
            { "id": "13", "city": "Mons",              "_geo": { "lat": 50.4502, "lng": 3.9623 } },
            { "id": "14", "city": "Valenciennes",      "_geo": { "lat": 50.3518, "lng": 3.5326 } },
            { "id": "15", "city": "Arras",             "_geo": { "lat": 50.2844, "lng": 2.7637 } },
            { "id": "16", "city": "Cambrai",           "_geo": { "lat": 50.1793, "lng": 3.2189 } },
            { "id": "17", "city": "Bapaume",           "_geo": { "lat": 50.1112, "lng": 2.8547 } },
            { "id": "18", "city": "Amiens",            "_geo": { "lat": 49.9314, "lng": 2.2710 } },
            { "id": "19", "city": "Compiègne",         "_geo": { "lat": 49.4449, "lng": 2.7913 } },
            { "id": "20", "city": "Paris",             "_geo": { "lat": 48.9021, "lng": 2.3708 } }
        ]);
        let external_ids_to_delete = ["5", "6", "7", "12", "17", "19"];

        insert_documents(&mut wtxn, &index, content);
        let deleted_internal_ids = delete_documents(&mut wtxn, &index, &external_ids_to_delete);

        // Placeholder search with geo filter
        let filter = Filter::from_str("_geoRadius(50.6924, 3.1763, 20000)").unwrap().unwrap();
        let results = index.search(&wtxn).filter(filter).execute().unwrap();
        assert!(!results.documents_ids.is_empty());
        for id in results.documents_ids.iter() {
            assert!(
                !deleted_internal_ids.contains(&id),
                "The document {} was supposed to be deleted",
                id
            );
        }

        wtxn.commit().unwrap();
    }

    #[test]
    fn get_documents_should_not_return_deleted_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let config = IndexerConfig::default();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_primary_key(S("docid"));
        builder.execute(|_| ()).unwrap();

        let content = documents!([
            { "docid": "1_4",  "label": "sign" },
            { "docid": "1_5",  "label": "letter" },
            { "docid": "1_7",  "label": "abstract,cartoon,design,pattern" },
            { "docid": "1_36", "label": "drawing,painting,pattern" },
            { "docid": "1_37", "label": "art,drawing,outdoor" },
            { "docid": "1_38", "label": "aquarium,art,drawing" },
            { "docid": "1_39", "label": "abstract" },
            { "docid": "1_40", "label": "cartoon" },
            { "docid": "1_41", "label": "art,drawing" },
            { "docid": "1_42", "label": "art,pattern" },
            { "docid": "1_43", "label": "abstract,art,drawing,pattern" },
            { "docid": "1_44", "label": "drawing" },
            { "docid": "1_45", "label": "art" },
            { "docid": "1_46", "label": "abstract,colorfulness,pattern" },
            { "docid": "1_47", "label": "abstract,pattern" },
            { "docid": "1_52", "label": "abstract,cartoon" },
            { "docid": "1_57", "label": "abstract,drawing,pattern" },
            { "docid": "1_58", "label": "abstract,art,cartoon" },
            { "docid": "1_68", "label": "design" },
            { "docid": "1_69", "label": "geometry" }
        ]);

        insert_documents(&mut wtxn, &index, content);
        let deleted_external_ids = ["1_7", "1_52"];
        let deleted_internal_ids = delete_documents(&mut wtxn, &index, &deleted_external_ids);

        // list all documents
        let results = index.all_documents(&wtxn).unwrap();
        for result in results {
            let (id, _) = result.unwrap();
            assert!(
                !deleted_internal_ids.contains(&id),
                "The document {} was supposed to be deleted",
                id
            );
        }

        // list internal document ids
        let results = index.documents_ids(&wtxn).unwrap();
        for id in results {
            assert!(
                !deleted_internal_ids.contains(&id),
                "The document {} was supposed to be deleted",
                id
            );
        }

        // get internal docids from deleted external document ids
        let results = index.external_documents_ids(&wtxn).unwrap();
        for id in deleted_external_ids {
            assert!(results.get(id).is_none(), "The document {} was supposed to be deleted", id);
        }

        wtxn.commit().unwrap();
    }

    #[test]
    fn stats_should_not_return_deleted_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let config = IndexerConfig::default();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_primary_key(S("docid"));
        builder.execute(|_| ()).unwrap();

        let content = documents!([
            { "docid": "1_4",  "label": "sign"},
            { "docid": "1_5",  "label": "letter"},
            { "docid": "1_7",  "label": "abstract,cartoon,design,pattern", "title": "Mickey Mouse"},
            { "docid": "1_36", "label": "drawing,painting,pattern"},
            { "docid": "1_37", "label": "art,drawing,outdoor"},
            { "docid": "1_38", "label": "aquarium,art,drawing",            "title": "Nemo"},
            { "docid": "1_39", "label": "abstract"},
            { "docid": "1_40", "label": "cartoon"},
            { "docid": "1_41", "label": "art,drawing"},
            { "docid": "1_42", "label": "art,pattern"},
            { "docid": "1_43", "label": "abstract,art,drawing,pattern",    "number": 32i32},
            { "docid": "1_44", "label": "drawing",                         "number": 44i32},
            { "docid": "1_45", "label": "art"},
            { "docid": "1_46", "label": "abstract,colorfulness,pattern"},
            { "docid": "1_47", "label": "abstract,pattern"},
            { "docid": "1_52", "label": "abstract,cartoon"},
            { "docid": "1_57", "label": "abstract,drawing,pattern"},
            { "docid": "1_58", "label": "abstract,art,cartoon"},
            { "docid": "1_68", "label": "design"},
            { "docid": "1_69", "label": "geometry"}
        ]);

        insert_documents(&mut wtxn, &index, content);
        delete_documents(&mut wtxn, &index, &["1_7", "1_52"]);

        // count internal documents
        let results = index.number_of_documents(&wtxn).unwrap();
        assert_eq!(18, results);

        // count field distribution
        let results = index.field_distribution(&wtxn).unwrap();
        assert_eq!(Some(&18), results.get("label"));
        assert_eq!(Some(&1), results.get("title"));
        assert_eq!(Some(&2), results.get("number"));

        wtxn.commit().unwrap();
    }
}

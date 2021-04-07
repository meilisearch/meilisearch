use std::collections::HashMap;
use std::collections::hash_map::Entry;

use anyhow::anyhow;
use chrono::Utc;
use fst::IntoStreamer;
use heed::types::ByteSlice;
use roaring::RoaringBitmap;
use serde_json::Value;

use crate::facet::FacetType;
use crate::{Index, BEU32, SmallString32, ExternalDocumentsIds};
use crate::heed_codec::facet::{FieldDocIdFacetStringCodec, FieldDocIdFacetF64Codec};
use super::ClearDocuments;

pub struct DeleteDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    external_documents_ids: ExternalDocumentsIds<'static>,
    documents_ids: RoaringBitmap,
    update_id: u64,
}

impl<'t, 'u, 'i> DeleteDocuments<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
        update_id: u64,
    ) -> anyhow::Result<DeleteDocuments<'t, 'u, 'i>>
    {
        let external_documents_ids = index
            .external_documents_ids(wtxn)?
            .into_static();

        Ok(DeleteDocuments {
            wtxn,
            index,
            external_documents_ids,
            documents_ids: RoaringBitmap::new(),
            update_id,
        })
    }

    pub fn delete_document(&mut self, docid: u32) {
        self.documents_ids.insert(docid);
    }

    pub fn delete_documents(&mut self, docids: &RoaringBitmap) {
        self.documents_ids.union_with(docids);
    }

    pub fn delete_external_id(&mut self, external_id: &str) -> Option<u32> {
        let docid = self.external_documents_ids.get(external_id)?;
        self.delete_document(docid);
        Some(docid)
    }

    pub fn execute(self) -> anyhow::Result<u64> {
        self.index.set_updated_at(self.wtxn, &Utc::now())?;
        // We retrieve the current documents ids that are in the database.
        let mut documents_ids = self.index.documents_ids(self.wtxn)?;

        // We can and must stop removing documents in a database that is empty.
        if documents_ids.is_empty() {
            return Ok(0);
        }

        // We remove the documents ids that we want to delete
        // from the documents in the database and write them back.
        let current_documents_ids_len = documents_ids.len();
        documents_ids.difference_with(&self.documents_ids);
        self.index.put_documents_ids(self.wtxn, &documents_ids)?;

        // We can execute a ClearDocuments operation when the number of documents
        // to delete is exactly the number of documents in the database.
        if current_documents_ids_len == self.documents_ids.len() {
            return ClearDocuments::new(self.wtxn, self.index, self.update_id).execute();
        }

        let fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
        let id_field = fields_ids_map.id("id").expect(r#"the field "id" to be present"#);

        let Index {
            env: _env,
            main: _main,
            word_docids,
            word_prefix_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            word_prefix_pair_proximity_docids,
            facet_field_id_value_docids,
            field_id_docid_facet_values,
            documents,
        } = self.index;

        // Number of fields for each document that has been deleted.
        let mut fields_ids_distribution_diff = HashMap::new();

        // Retrieve the words and the external documents ids contained in the documents.
        let mut words = Vec::new();
        let mut external_ids = Vec::new();
        for docid in &self.documents_ids {
            // We create an iterator to be able to get the content and delete the document
            // content itself. It's faster to acquire a cursor to get and delete,
            // as we avoid traversing the LMDB B-Tree two times but only once.
            let key = BEU32::new(docid);
            let mut iter = documents.range_mut(self.wtxn, &(key..=key))?;
            if let Some((_key, obkv)) = iter.next().transpose()? {
                for (field_id, _) in obkv.iter() {
                    *fields_ids_distribution_diff.entry(field_id).or_default() += 1;
                }

                if let Some(content) = obkv.get(id_field) {
                    let external_id = match serde_json::from_slice(content).unwrap() {
                        Value::String(string) => SmallString32::from(string.as_str()),
                        Value::Number(number) => SmallString32::from(number.to_string()),
                        _ => return Err(anyhow!("documents ids must be either strings or numbers")),
                    };
                    external_ids.push(external_id);
                }
                iter.del_current()?;
            }
            drop(iter);

            // We iterate through the words positions of the document id,
            // retrieve the word and delete the positions.
            let mut iter = docid_word_positions.prefix_iter_mut(self.wtxn, &(docid, ""))?;
            while let Some(result) = iter.next() {
                let ((_docid, word), _positions) = result?;
                // This boolean will indicate if we must remove this word from the words FST.
                words.push((SmallString32::from(word), false));
                iter.del_current()?;
            }
        }

        let mut fields_distribution = self.index.fields_distribution(self.wtxn)?;

        // We use pre-calculated number of fields occurrences that needs to be deleted
        // to reflect deleted documents.
        // If all field occurrences are removed, delete the entry from distribution.
        // Otherwise, insert new number of occurrences (current_count - count_diff).
        for (field_id, count_diff) in fields_ids_distribution_diff {
            let field_name = fields_ids_map.name(field_id).unwrap();
            if let Entry::Occupied(mut entry) = fields_distribution.entry(field_name.to_string()) {
                match entry.get().checked_sub(count_diff) {
                    Some(0) | None => entry.remove(),
                    Some(count) => entry.insert(count)
                };
            }
        }

        self.index.put_fields_distribution(self.wtxn, &fields_distribution)?;

        // We create the FST map of the external ids that we must delete.
        external_ids.sort_unstable();
        let external_ids_to_delete = fst::Set::from_iter(external_ids.iter().map(AsRef::as_ref))?;

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
            // We create an iterator to be able to get the content and delete the word docids.
            // It's faster to acquire a cursor to get and delete or put, as we avoid traversing
            // the LMDB B-Tree two times but only once.
            let mut iter = word_docids.prefix_iter_mut(self.wtxn, &word)?;
            if let Some((key, mut docids)) = iter.next().transpose()? {
                if key == word.as_ref() {
                    let previous_len = docids.len();
                    docids.difference_with(&self.documents_ids);
                    if docids.is_empty() {
                        iter.del_current()?;
                        *must_remove = true;
                    } else if docids.len() != previous_len {
                        iter.put_current(key, &docids)?;
                    }
                }
            }
        }

        // We construct an FST set that contains the words to delete from the words FST.
        let words_to_delete = words.iter().filter_map(|(word, must_remove)| {
            if *must_remove { Some(word.as_ref()) } else { None }
        });
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

        // We iterate over the word prefix docids database and remove the deleted documents ids
        // from every docids lists. We register the empty prefixes in an fst Set for futur deletion.
        let mut prefixes_to_delete = fst::SetBuilder::memory();
        let mut iter = word_prefix_docids.iter_mut(self.wtxn)?;
        while let Some(result) = iter.next() {
            let (prefix, mut docids) = result?;
            let previous_len = docids.len();
            docids.difference_with(&self.documents_ids);
            if docids.is_empty() {
                iter.del_current()?;
                prefixes_to_delete.insert(prefix)?;
            } else if docids.len() != previous_len {
                iter.put_current(prefix, &docids)?;
            }
        }

        drop(iter);

        // We compute the new prefix FST and write it only if there is a change.
        let prefixes_to_delete = prefixes_to_delete.into_set();
        if !prefixes_to_delete.is_empty() {
            let new_words_prefixes_fst = {
                // We retrieve the current words prefixes FST from the database.
                let words_prefixes_fst = self.index.words_prefixes_fst(self.wtxn)?;
                let difference = words_prefixes_fst.op().add(&prefixes_to_delete).difference();

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
            docids.difference_with(&self.documents_ids);
            if docids.is_empty() {
                iter.del_current()?;
            } else if docids.len() != previous_len {
                iter.put_current(key, &docids)?;
            }
        }

        drop(iter);

        // We delete the documents ids that are under the pairs of words,
        // it is faster and use no memory to iterate over all the words pairs than
        // to compute the cartesian product of every words of the deleted documents.
        let mut iter = word_pair_proximity_docids.remap_key_type::<ByteSlice>().iter_mut(self.wtxn)?;
        while let Some(result) = iter.next() {
            let (bytes, mut docids) = result?;
            let previous_len = docids.len();
            docids.difference_with(&self.documents_ids);
            if docids.is_empty() {
                iter.del_current()?;
            } else if docids.len() != previous_len {
                iter.put_current(bytes, &docids)?;
            }
        }

        drop(iter);

        // Remove the documents ids from the faceted documents ids.
        let faceted_fields = self.index.faceted_fields_ids(self.wtxn)?;
        for (field_id, facet_type) in faceted_fields {
            let mut docids = self.index.faceted_documents_ids(self.wtxn, field_id)?;
            docids.difference_with(&self.documents_ids);
            self.index.put_faceted_documents_ids(self.wtxn, field_id, &docids)?;

            // We delete the entries that are part of the documents ids.
            let iter = field_id_docid_facet_values.prefix_iter_mut(self.wtxn, &[field_id])?;
            match facet_type {
                FacetType::String => {
                    let mut iter = iter.remap_key_type::<FieldDocIdFacetStringCodec>();
                    while let Some(result) = iter.next() {
                        let ((_fid, docid, _value), ()) = result?;
                        if self.documents_ids.contains(docid) {
                            iter.del_current()?;
                        }
                    }
                },
                FacetType::Number => {
                    let mut iter = iter.remap_key_type::<FieldDocIdFacetF64Codec>();
                    while let Some(result) = iter.next() {
                        let ((_fid, docid, _value), ()) = result?;
                        if self.documents_ids.contains(docid) {
                            iter.del_current()?;
                        }
                    }
                },
            }
        }

        // We delete the documents ids that are under the facet field id values.
        let mut iter = facet_field_id_value_docids.iter_mut(self.wtxn)?;
        while let Some(result) = iter.next() {
            let (bytes, mut docids) = result?;
            let previous_len = docids.len();
            docids.difference_with(&self.documents_ids);
            if docids.is_empty() {
                iter.del_current()?;
            } else if docids.len() != previous_len {
                iter.put_current(bytes, &docids)?;
            }
        }

        drop(iter);

        Ok(self.documents_ids.len())
    }
}

#[cfg(test)]
mod tests {
    use heed::EnvOpenOptions;

    use crate::update::{IndexDocuments, UpdateFormat};
    use super::*;

    #[test]
    fn delete_documents_with_numbers_as_primary_key() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with an id for only one of them.
        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"[
            { "id": 0, "name": "kevin", "object": { "key1": "value1", "key2": "value2" } },
            { "id": 1, "name": "kevina", "array": ["I", "am", "fine"] },
            { "id": 2, "name": "benoit", "array_of_object": [{ "wow": "amazing" }] }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();

        // delete those documents, ids are synchronous therefore 0, 1, and 2.
        let mut builder = DeleteDocuments::new(&mut wtxn, &index, 1).unwrap();
        builder.delete_document(0);
        builder.delete_document(1);
        builder.delete_document(2);
        builder.execute().unwrap();

        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        assert!(index.fields_distribution(&rtxn).unwrap().is_empty());
    }
}

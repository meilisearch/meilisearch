use fst::IntoStreamer;
use heed::types::ByteSlice;
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::{Index, BEU32, SmallString32, ExternalDocumentsIds};
use crate::heed_codec::facet::{FieldDocIdFacetStringCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetI64Codec};
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

    pub fn execute(self) -> anyhow::Result<usize> {
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
            docid_word_positions,
            word_pair_proximity_docids,
            facet_field_id_value_docids,
            field_id_docid_facet_values,
            documents,
        } = self.index;

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
                if let Some(content) = obkv.get(id_field) {
                    let external_id: SmallString32 = serde_json::from_slice(content).unwrap();
                    external_ids.push(external_id);
                }
                iter.del_current()?;
            }
            drop(iter);

            // We iterate througt the words positions of the document id,
            // retrieve the word and delete the positions.
            let mut iter = docid_word_positions.prefix_iter_mut(self.wtxn, &(docid, ""))?;
            while let Some(result) = iter.next() {
                let ((_docid, word), _positions) = result?;
                // This boolean will indicate if we must remove this word from the words FST.
                words.push((SmallString32::from(word), false));
                iter.del_current()?;
            }
        }

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
                FacetType::Float => {
                    let mut iter = iter.remap_key_type::<FieldDocIdFacetF64Codec>();
                    while let Some(result) = iter.next() {
                        let ((_fid, docid, _value), ()) = result?;
                        if self.documents_ids.contains(docid) {
                            iter.del_current()?;
                        }
                    }
                },
                FacetType::Integer => {
                    let mut iter = iter.remap_key_type::<FieldDocIdFacetI64Codec>();
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

        Ok(self.documents_ids.len() as usize)
    }
}

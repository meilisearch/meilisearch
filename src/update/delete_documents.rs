use std::borrow::Cow;
use std::convert::TryFrom;

use fst::{IntoStreamer, Streamer};
use roaring::RoaringBitmap;

use crate::{Index, BEU32};

pub struct DeleteDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'u>,
    index: &'i Index,
    users_ids_documents_ids: fst::Map<Vec<u8>>,
    documents_ids: RoaringBitmap,
}

impl<'t, 'u, 'i> DeleteDocuments<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'u>,
        index: &'i Index,
    ) -> anyhow::Result<DeleteDocuments<'t, 'u, 'i>>
    {
        let users_ids_documents_ids = index
            .users_ids_documents_ids(wtxn)?
            .map_data(Cow::into_owned)?;

        Ok(DeleteDocuments {
            wtxn,
            index,
            users_ids_documents_ids,
            documents_ids: RoaringBitmap::new(),
        })
    }

    pub fn delete_document(&mut self, docid: u32) {
        self.documents_ids.insert(docid);
    }

    pub fn delete_documents(&mut self, docids: &RoaringBitmap) {
        self.documents_ids.union_with(docids);
    }

    pub fn delete_user_id(&mut self, user_id: &str) -> Option<u32> {
        let docid = self.users_ids_documents_ids.get(user_id).map(|id| u32::try_from(id).unwrap())?;
        self.delete_document(docid);
        Some(docid)
    }

    pub fn execute(self) -> anyhow::Result<usize> {
        // We retrieve remove the deleted documents ids and write them into the database.
        let mut documents_ids = self.index.documents_ids(self.wtxn)?;

        // We can and must stop removing documents in a database that is empty.
        if documents_ids.is_empty() {
            return Ok(0);
        }

        documents_ids.intersect_with(&self.documents_ids);
        self.index.put_documents_ids(self.wtxn, &documents_ids)?;

        // TODO we should be able to execute a ClearDocuments operation when the number of documents
        //      to delete is exactly the number of documents in the database, however it seems that
        //      clearing a database in LMDB requires a commit for it to be effective, we can't clear
        //      and assume that the database is empty in the same wtxn or something.

        let fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
        let id_field = fields_ids_map.id("id").expect(r#"the field "id" to be present"#);

        let Index {
            main: _main,
            word_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            documents,
        } = self.index;

        // Retrieve the words and the users ids contained in the documents.
        // TODO we must use a smallword instead of a string.
        let mut words = Vec::new();
        let mut users_ids = Vec::new();
        for docid in &documents_ids {
            // We create an iterator to be able to get the content and delete the document
            // content itself. It's faster to acquire a cursor to get and delete,
            // as we avoid traversing the LMDB B-Tree two times but only once.
            let key = BEU32::new(docid);
            let mut iter = documents.range_mut(self.wtxn, &(key..=key))?;
            if let Some((_key, obkv)) = iter.next().transpose()? {
                if let Some(content) = obkv.get(id_field) {
                    let user_id: String = serde_json::from_slice(content).unwrap();
                    users_ids.push(user_id);
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
                words.push((String::from(word), false));
                iter.del_current()?;
            }
        }

        // We create the FST map of the users ids that we must delete.
        users_ids.sort_unstable();
        let users_ids_to_delete = fst::Set::from_iter(users_ids)?;
        let users_ids_to_delete = fst::Map::from(users_ids_to_delete.into_fst());

        let new_users_ids_documents_ids = {
            // We acquire the current users ids documents ids map and create
            // a difference operation between the current and to-delete users ids.
            let users_ids_documents_ids = self.index.users_ids_documents_ids(self.wtxn)?;
            let difference = users_ids_documents_ids.op().add(&users_ids_to_delete).difference();

            // We stream the new users ids that does no more contains the to-delete users ids.
            let mut iter = difference.into_stream();
            let mut new_users_ids_documents_ids_builder = fst::MapBuilder::memory();
            while let Some((userid, docids)) = iter.next() {
                new_users_ids_documents_ids_builder.insert(userid, docids[0].value)?;
            }

            // We create an FST map from the above builder.
            new_users_ids_documents_ids_builder.into_map()
        };

        // We write the new users ids into the main database.
        self.index.put_users_ids_documents_ids(self.wtxn, &new_users_ids_documents_ids)?;

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
                if key == word {
                    docids.difference_with(&mut documents_ids);
                    if docids.is_empty() {
                        iter.del_current()?;
                        *must_remove = true;
                    } else {
                        iter.put_current(key, &docids)?;
                    }
                }
            }
        }

        // We construct an FST set that contains the words to delete from the words FST.
        let words_to_delete = words.iter().filter_map(|(w, d)| if *d { Some(w) } else { None });
        let words_to_delete = fst::Set::from_iter(words_to_delete)?;

        let new_words_fst = {
            // We retrieve the current words FST from the database.
            let words_fst = self.index.words_fst(self.wtxn)?;
            let difference = words_fst.op().add(&words_to_delete).difference();

            // We stream the new users ids that does no more contains the to-delete users ids.
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
        let mut iter = word_pair_proximity_docids.iter_mut(self.wtxn)?;
        while let Some(result) = iter.next() {
            let ((w1, w2, prox), mut docids) = result?;
            docids.difference_with(&documents_ids);
            if docids.is_empty() {
                iter.del_current()?;
            } else {
                iter.put_current(&(w1, w2, prox), &docids)?;
            }
        }

        Ok(documents_ids.len() as usize)
    }
}

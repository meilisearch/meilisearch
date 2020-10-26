use std::borrow::Cow;
use std::convert::TryFrom;

use fst::{IntoStreamer, Streamer};
use grenad::CompressionType;
use itertools::Itertools;
use roaring::RoaringBitmap;

use crate::{Index, BEU32};
use super::clear_documents::ClearDocuments;

pub struct UpdateBuilder {
    log_every_n: usize,
    max_nb_chunks: Option<usize>,
    max_memory: usize,
    linked_hash_map_size: usize,
    chunk_compression_type: CompressionType,
    chunk_compression_level: Option<u32>,
    chunk_fusing_shrink_size: u64,
    enable_chunk_fusing: bool,
    indexing_jobs: Option<usize>,
}

impl UpdateBuilder {
    pub fn new() -> UpdateBuilder {
        todo!()
    }

    pub fn log_every_n(&mut self, log_every_n: usize) -> &mut Self {
        self.log_every_n = log_every_n;
        self
    }

    pub fn max_nb_chunks(&mut self, max_nb_chunks: usize) -> &mut Self {
        self.max_nb_chunks = Some(max_nb_chunks);
        self
    }

    pub fn max_memory(&mut self, max_memory: usize) -> &mut Self {
        self.max_memory = max_memory;
        self
    }

    pub fn linked_hash_map_size(&mut self, linked_hash_map_size: usize) -> &mut Self {
        self.linked_hash_map_size = linked_hash_map_size;
        self
    }

    pub fn chunk_compression_type(&mut self, chunk_compression_type: CompressionType) -> &mut Self {
        self.chunk_compression_type = chunk_compression_type;
        self
    }

    pub fn chunk_compression_level(&mut self, chunk_compression_level: u32) -> &mut Self {
        self.chunk_compression_level = Some(chunk_compression_level);
        self
    }

    pub fn chunk_fusing_shrink_size(&mut self, chunk_fusing_shrink_size: u64) -> &mut Self {
        self.chunk_fusing_shrink_size = chunk_fusing_shrink_size;
        self
    }

    pub fn enable_chunk_fusing(&mut self, enable_chunk_fusing: bool) -> &mut Self {
        self.enable_chunk_fusing = enable_chunk_fusing;
        self
    }

    pub fn indexing_jobs(&mut self, indexing_jobs: usize) -> &mut Self {
        self.indexing_jobs = Some(indexing_jobs);
        self
    }

    pub fn clear_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'u>,
        index: &'i Index,
    ) -> ClearDocuments<'t, 'u, 'i>
    {
        ClearDocuments::new(wtxn, index)
    }

    pub fn delete_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'u>,
        index: &'i Index,
    ) -> anyhow::Result<DeleteDocuments<'t, 'u, 'i>>
    {
        DeleteDocuments::new(wtxn, index)
    }

    pub fn index_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'u>,
        index: &'i Index,
    ) -> IndexDocuments<'t, 'u, 'i>
    {
        IndexDocuments::new(wtxn, index)
    }
}

pub struct DeleteDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'u>,
    index: &'i Index,
    users_ids_documents_ids: fst::Map<Vec<u8>>,
    documents_ids: RoaringBitmap,
}

impl<'t, 'u, 'i> DeleteDocuments<'t, 'u, 'i> {
    fn new(wtxn: &'t mut heed::RwTxn<'u>, index: &'i Index) -> anyhow::Result<DeleteDocuments<'t, 'u, 'i>> {
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

        // We delete the documents ids that are under the pairs of words we found.
        // TODO We can maybe improve this by using the `compute_words_pair_proximities`
        //      function instead of iterating over all the possible word pairs.
        for ((w1, _), (w2, _)) in words.iter().cartesian_product(&words) {
            let start = &(w1.as_str(), w2.as_str(), 0);
            let end = &(w1.as_str(), w2.as_str(), 7);
            let mut iter = word_pair_proximity_docids.range_mut(self.wtxn, &(start..=end))?;
            while let Some(result) = iter.next() {
                let ((w1, w2, prox), mut docids) = result?;
                docids.difference_with(&documents_ids);
                if docids.is_empty() {
                    iter.del_current()?;
                } else {
                    iter.put_current(&(w1, w2, prox), &docids)?;
                }
            }
        }

        Ok(documents_ids.len() as usize)
    }
}

pub enum IndexDocumentsMethod {
    /// Replace the previous document with the new one,
    /// removing all the already known attributes.
    ReplaceDocuments,

    /// Merge the previous version of the document with the new version,
    /// replacing old attributes values with the new ones and add the new attributes.
    UpdateDocuments,
}

pub struct IndexDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'u>,
    index: &'i Index,
    update_method: IndexDocumentsMethod,
}

impl<'t, 'u, 'i> IndexDocuments<'t, 'u, 'i> {
    fn new(wtxn: &'t mut heed::RwTxn<'u>, index: &'i Index) -> IndexDocuments<'t, 'u, 'i> {
        IndexDocuments { wtxn, index, update_method: IndexDocumentsMethod::ReplaceDocuments }
    }

    pub fn index_documents_method(&mut self, method: IndexDocumentsMethod) -> &mut Self {
        self.update_method = method;
        self
    }

    pub fn execute(self) -> anyhow::Result<()> {
        todo!()
    }
}

use std::collections::hash_map::Entry;

use fxhash::FxHashMap;
use heed::types::ByteSlice;
use heed::RoTxn;

use crate::{Index, Result};

#[derive(Default)]
pub struct DatabaseCache<'transaction> {
    pub word_pair_proximity_docids: FxHashMap<(u8, String, String), Option<&'transaction [u8]>>,
    pub word_prefix_pair_proximity_docids:
        FxHashMap<(u8, String, String), Option<&'transaction [u8]>>,
    pub prefix_word_pair_proximity_docids:
        FxHashMap<(u8, String, String), Option<&'transaction [u8]>>,
    pub word_docids: FxHashMap<String, Option<&'transaction [u8]>>,
    pub exact_word_docids: FxHashMap<String, Option<&'transaction [u8]>>,
    pub word_prefix_docids: FxHashMap<String, Option<&'transaction [u8]>>,
}
impl<'transaction> DatabaseCache<'transaction> {
    pub fn get_word_docids(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        word: &str,
    ) -> Result<Option<&'transaction [u8]>> {
        let bitmap_ptr = match self.word_docids.entry(word.to_owned()) {
            Entry::Occupied(bitmap_ptr) => *bitmap_ptr.get(),
            Entry::Vacant(entry) => {
                let bitmap_ptr = index.word_docids.remap_data_type::<ByteSlice>().get(txn, word)?;
                entry.insert(bitmap_ptr);
                bitmap_ptr
            }
        };
        Ok(bitmap_ptr)
    }
    pub fn get_prefix_docids(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        prefix: &str,
    ) -> Result<Option<&'transaction [u8]>> {
        // In the future, this will be a frozen roaring bitmap
        let bitmap_ptr = match self.word_prefix_docids.entry(prefix.to_owned()) {
            Entry::Occupied(bitmap_ptr) => *bitmap_ptr.get(),
            Entry::Vacant(entry) => {
                let bitmap_ptr =
                    index.word_prefix_docids.remap_data_type::<ByteSlice>().get(txn, prefix)?;
                entry.insert(bitmap_ptr);
                bitmap_ptr
            }
        };
        Ok(bitmap_ptr)
    }

    pub fn get_word_pair_proximity_docids(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        word1: &str,
        word2: &str,
        proximity: u8,
    ) -> Result<Option<&'transaction [u8]>> {
        let key = (proximity, word1.to_owned(), word2.to_owned());
        match self.word_pair_proximity_docids.entry(key.clone()) {
            Entry::Occupied(bitmap_ptr) => Ok(*bitmap_ptr.get()),
            Entry::Vacant(entry) => {
                // We shouldn't greedily access this DB at all
                // a DB (w1, w2) -> [proximities] would be much better
                // We could even have a DB that is (w1) -> set of words such that (w1, w2) are in proximity
                // And if we worked with words encoded as integers, the set of words could be a roaring bitmap
                // Then, to find all the proximities between two list of words, we'd do:

                // inputs:
                //    - words1 (roaring bitmap)
                //    - words2 (roaring bitmap)
                // output:
                //    - [(word1, word2, [proximities])]
                // algo:
                //  let mut ouput = vec![];
                //  for word1 in words1 {
                //      let all_words_in_proximity_of_w1 = pair_words_db.get(word1);
                //      let words_in_proximity_of_w1 = all_words_in_proximity_of_w1 & words2;
                //      for word2 in words_in_proximity_of_w1 {
                //          let proximties = prox_db.get(word1, word2);
                //          output.push(word1, word2, proximities);
                //      }
                //  }
                let bitmap_ptr = index
                    .word_pair_proximity_docids
                    .remap_data_type::<ByteSlice>()
                    .get(txn, &(key.0, key.1.as_str(), key.2.as_str()))?;
                entry.insert(bitmap_ptr);
                Ok(bitmap_ptr)
            }
        }
    }

    pub fn get_word_prefix_pair_proximity_docids(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        word1: &str,
        prefix2: &str,
        proximity: u8,
    ) -> Result<Option<&'transaction [u8]>> {
        let key = (proximity, word1.to_owned(), prefix2.to_owned());
        match self.word_prefix_pair_proximity_docids.entry(key.clone()) {
            Entry::Occupied(bitmap_ptr) => Ok(*bitmap_ptr.get()),
            Entry::Vacant(entry) => {
                let bitmap_ptr = index
                    .word_prefix_pair_proximity_docids
                    .remap_data_type::<ByteSlice>()
                    .get(txn, &(key.0, key.1.as_str(), key.2.as_str()))?;
                entry.insert(bitmap_ptr);
                Ok(bitmap_ptr)
            }
        }
    }
    pub fn get_prefix_word_pair_proximity_docids(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        left_prefix: &str,
        right: &str,
        proximity: u8,
    ) -> Result<Option<&'transaction [u8]>> {
        let key = (proximity, left_prefix.to_owned(), right.to_owned());
        match self.prefix_word_pair_proximity_docids.entry(key) {
            Entry::Occupied(bitmap_ptr) => Ok(*bitmap_ptr.get()),
            Entry::Vacant(entry) => {
                let bitmap_ptr = index
                    .prefix_word_pair_proximity_docids
                    .remap_data_type::<ByteSlice>()
                    .get(txn, &(proximity, left_prefix, right))?;
                entry.insert(bitmap_ptr);
                Ok(bitmap_ptr)
            }
        }
    }
}

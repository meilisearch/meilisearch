use super::{interner::Interned, SearchContext};
use crate::Result;
use fxhash::FxHashMap;
use heed::types::ByteSlice;
use std::collections::hash_map::Entry;

#[derive(Default)]
pub struct DatabaseCache<'search> {
    // TODO: interner for all database cache keys
    pub word_pair_proximity_docids:
        FxHashMap<(u8, Interned<String>, Interned<String>), Option<&'search [u8]>>,
    pub word_prefix_pair_proximity_docids:
        FxHashMap<(u8, Interned<String>, Interned<String>), Option<&'search [u8]>>,
    pub prefix_word_pair_proximity_docids:
        FxHashMap<(u8, Interned<String>, Interned<String>), Option<&'search [u8]>>,
    pub word_docids: FxHashMap<Interned<String>, Option<&'search [u8]>>,
    pub exact_word_docids: FxHashMap<Interned<String>, Option<&'search [u8]>>,
    pub word_prefix_docids: FxHashMap<Interned<String>, Option<&'search [u8]>>,
}
impl<'search> SearchContext<'search> {
    pub fn get_word_docids(&mut self, word: Interned<String>) -> Result<Option<&'search [u8]>> {
        let bitmap_ptr = match self.db_cache.word_docids.entry(word) {
            Entry::Occupied(bitmap_ptr) => *bitmap_ptr.get(),
            Entry::Vacant(entry) => {
                let bitmap_ptr = self
                    .index
                    .word_docids
                    .remap_data_type::<ByteSlice>()
                    .get(self.txn, self.word_interner.get(word))?;
                entry.insert(bitmap_ptr);
                bitmap_ptr
            }
        };
        Ok(bitmap_ptr)
    }
    pub fn get_prefix_docids(&mut self, prefix: Interned<String>) -> Result<Option<&'search [u8]>> {
        // In the future, this will be a frozen roaring bitmap
        let bitmap_ptr = match self.db_cache.word_prefix_docids.entry(prefix) {
            Entry::Occupied(bitmap_ptr) => *bitmap_ptr.get(),
            Entry::Vacant(entry) => {
                let bitmap_ptr = self
                    .index
                    .word_prefix_docids
                    .remap_data_type::<ByteSlice>()
                    .get(self.txn, self.word_interner.get(prefix))?;
                entry.insert(bitmap_ptr);
                bitmap_ptr
            }
        };
        Ok(bitmap_ptr)
    }

    pub fn get_word_pair_proximity_docids(
        &mut self,
        word1: Interned<String>,
        word2: Interned<String>,
        proximity: u8,
    ) -> Result<Option<&'search [u8]>> {
        let key = (proximity, word1, word2);
        match self.db_cache.word_pair_proximity_docids.entry(key) {
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
                let bitmap_ptr =
                    self.index.word_pair_proximity_docids.remap_data_type::<ByteSlice>().get(
                        self.txn,
                        &(key.0, self.word_interner.get(key.1), self.word_interner.get(key.2)),
                    )?;
                entry.insert(bitmap_ptr);
                Ok(bitmap_ptr)
            }
        }
    }

    pub fn get_word_prefix_pair_proximity_docids(
        &mut self,
        word1: Interned<String>,
        prefix2: Interned<String>,
        proximity: u8,
    ) -> Result<Option<&'search [u8]>> {
        let key = (proximity, word1, prefix2);
        match self.db_cache.word_prefix_pair_proximity_docids.entry(key) {
            Entry::Occupied(bitmap_ptr) => Ok(*bitmap_ptr.get()),
            Entry::Vacant(entry) => {
                let bitmap_ptr = self
                    .index
                    .word_prefix_pair_proximity_docids
                    .remap_data_type::<ByteSlice>()
                    .get(
                        self.txn,
                        &(key.0, self.word_interner.get(key.1), self.word_interner.get(key.2)),
                    )?;
                entry.insert(bitmap_ptr);
                Ok(bitmap_ptr)
            }
        }
    }
    pub fn get_prefix_word_pair_proximity_docids(
        &mut self,
        left_prefix: Interned<String>,
        right: Interned<String>,
        proximity: u8,
    ) -> Result<Option<&'search [u8]>> {
        let key = (proximity, left_prefix, right);
        match self.db_cache.prefix_word_pair_proximity_docids.entry(key) {
            Entry::Occupied(bitmap_ptr) => Ok(*bitmap_ptr.get()),
            Entry::Vacant(entry) => {
                let bitmap_ptr = self
                    .index
                    .prefix_word_pair_proximity_docids
                    .remap_data_type::<ByteSlice>()
                    .get(
                        self.txn,
                        &(
                            proximity,
                            self.word_interner.get(left_prefix),
                            self.word_interner.get(right),
                        ),
                    )?;
                entry.insert(bitmap_ptr);
                Ok(bitmap_ptr)
            }
        }
    }
}

use std::collections::hash_map::Entry;
use std::hash::Hash;

use fxhash::FxHashMap;
use heed::types::ByteSlice;
use heed::{BytesEncode, Database, RoTxn};

use super::interner::Interned;
use crate::{Result, SearchContext};

/// A cache storing pointers to values in the LMDB databases.
///
/// Used for performance reasons only. By using this cache, we avoid performing a
/// database lookup and instead get a direct reference to the value using a fast
/// local HashMap lookup.
#[derive(Default)]
pub struct DatabaseCache<'ctx> {
    pub word_pair_proximity_docids:
        FxHashMap<(u8, Interned<String>, Interned<String>), Option<&'ctx [u8]>>,
    pub word_prefix_pair_proximity_docids:
        FxHashMap<(u8, Interned<String>, Interned<String>), Option<&'ctx [u8]>>,
    pub prefix_word_pair_proximity_docids:
        FxHashMap<(u8, Interned<String>, Interned<String>), Option<&'ctx [u8]>>,
    pub word_docids: FxHashMap<Interned<String>, Option<&'ctx [u8]>>,
    pub exact_word_docids: FxHashMap<Interned<String>, Option<&'ctx [u8]>>,
    pub word_prefix_docids: FxHashMap<Interned<String>, Option<&'ctx [u8]>>,
}
impl<'ctx> DatabaseCache<'ctx> {
    fn get_value<'v, K1, KC>(
        txn: &'ctx RoTxn,
        cache_key: K1,
        db_key: &'v KC::EItem,
        cache: &mut FxHashMap<K1, Option<&'ctx [u8]>>,
        db: Database<KC, ByteSlice>,
    ) -> Result<Option<&'ctx [u8]>>
    where
        K1: Copy + Eq + Hash,
        KC: BytesEncode<'v>,
    {
        let bitmap_ptr = match cache.entry(cache_key) {
            Entry::Occupied(bitmap_ptr) => *bitmap_ptr.get(),
            Entry::Vacant(entry) => {
                let bitmap_ptr = db.get(txn, db_key)?;
                entry.insert(bitmap_ptr);
                bitmap_ptr
            }
        };
        Ok(bitmap_ptr)
    }
}
impl<'ctx> SearchContext<'ctx> {
    /// Retrieve or insert the given value in the `word_docids` database.
    pub fn get_db_word_docids(&mut self, word: Interned<String>) -> Result<Option<&'ctx [u8]>> {
        DatabaseCache::get_value(
            self.txn,
            word,
            self.word_interner.get(word).as_str(),
            &mut self.db_cache.word_docids,
            self.index.word_docids.remap_data_type::<ByteSlice>(),
        )
    }
    /// Retrieve or insert the given value in the `word_prefix_docids` database.
    pub fn get_db_word_prefix_docids(
        &mut self,
        prefix: Interned<String>,
    ) -> Result<Option<&'ctx [u8]>> {
        DatabaseCache::get_value(
            self.txn,
            prefix,
            self.word_interner.get(prefix).as_str(),
            &mut self.db_cache.word_prefix_docids,
            self.index.word_prefix_docids.remap_data_type::<ByteSlice>(),
        )
    }

    pub fn get_db_word_pair_proximity_docids(
        &mut self,
        word1: Interned<String>,
        word2: Interned<String>,
        proximity: u8,
    ) -> Result<Option<&'ctx [u8]>> {
        DatabaseCache::get_value(
            self.txn,
            (proximity, word1, word2),
            &(
                proximity,
                self.word_interner.get(word1).as_str(),
                self.word_interner.get(word2).as_str(),
            ),
            &mut self.db_cache.word_pair_proximity_docids,
            self.index.word_pair_proximity_docids.remap_data_type::<ByteSlice>(),
        )
    }

    pub fn get_db_word_prefix_pair_proximity_docids(
        &mut self,
        word1: Interned<String>,
        prefix2: Interned<String>,
        proximity: u8,
    ) -> Result<Option<&'ctx [u8]>> {
        DatabaseCache::get_value(
            self.txn,
            (proximity, word1, prefix2),
            &(
                proximity,
                self.word_interner.get(word1).as_str(),
                self.word_interner.get(prefix2).as_str(),
            ),
            &mut self.db_cache.word_prefix_pair_proximity_docids,
            self.index.word_prefix_pair_proximity_docids.remap_data_type::<ByteSlice>(),
        )
    }
    pub fn get_db_prefix_word_pair_proximity_docids(
        &mut self,
        left_prefix: Interned<String>,
        right: Interned<String>,
        proximity: u8,
    ) -> Result<Option<&'ctx [u8]>> {
        DatabaseCache::get_value(
            self.txn,
            (proximity, left_prefix, right),
            &(
                proximity,
                self.word_interner.get(left_prefix).as_str(),
                self.word_interner.get(right).as_str(),
            ),
            &mut self.db_cache.prefix_word_pair_proximity_docids,
            self.index.prefix_word_pair_proximity_docids.remap_data_type::<ByteSlice>(),
        )
    }
}

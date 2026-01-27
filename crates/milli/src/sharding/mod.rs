#[cfg(not(feature = "enterprise"))]
pub mod community_edition;
#[cfg(feature = "enterprise")]
pub mod enterprise_edition;
use heed::types::{DecodeIgnore, Str};
use heed::{Database, RoTxn, RwTxn};
use roaring::RoaringBitmap;

use crate::{CboRoaringBitmapCodec, Index, Result};

#[derive(Debug, Clone)]
pub struct Shards(pub Vec<Shard>);

#[derive(Debug, Clone)]
pub struct Shard {
    pub is_own: bool,
    pub name: String,
}

/// View over the `shard_docids` DB of an index
pub struct DbShardDocids(Database<Str, CboRoaringBitmapCodec>);

impl DbShardDocids {
    /// Create the view from the index.
    ///
    /// A transaction will be necessary to actually access the view.
    pub fn from_index(index: &Index) -> Self {
        Self(index.shard_docids)
    }

    /// The docids for the specified shard.
    ///
    /// Returns `Ok(None)` if the specified shard doesn't exist in the index.
    pub fn docids(&self, rtxn: &RoTxn<'_>, shard: &str) -> Result<Option<RoaringBitmap>> {
        Ok(self.0.get(rtxn, shard)?)
    }

    /// Updates the docids that belong to a shard.
    ///
    /// The shard is added if it does not exist.
    pub fn put_docids(
        &self,
        wtxn: &mut RwTxn<'_>,
        shard: &str,
        docids: &RoaringBitmap,
    ) -> Result<()> {
        Ok(self.0.put(wtxn, shard, docids)?)
    }

    /// Iterate over existing shards, and calls an update function `f` with the shard name and existing docids.
    ///
    /// `f` should update the docids for that shard and return `true` if the docids were modified and need to be updated
    /// in the index, and `false` if that is unnecessary.
    pub fn update_shards<F>(&self, wtxn: &mut RwTxn<'_>, mut f: F) -> Result<()>
    where
        F: FnMut(&str, &mut RoaringBitmap) -> bool,
    {
        let mut it = self.0.iter_mut(wtxn)?;
        while let Some(res) = it.next() {
            let (shard, mut docids) = res?;
            let shard = shard.to_owned();
            if f(&shard, &mut docids) {
                // SAFETY: we are not keeping any reference to LMDB's data
                unsafe {
                    it.put_current(&shard, &docids)?;
                }
            }
        }
        Ok(())
    }

    /// Add a new shard without any docid.
    pub fn add_shard(&self, wtxn: &mut RwTxn<'_>, shard: &str) -> Result<()> {
        self.put_docids(wtxn, shard, &RoaringBitmap::new())
    }

    /// Remove a shard with all its docids.
    pub fn remove_shard(&self, wtxn: &mut RwTxn<'_>, shard: &str) -> Result<bool> {
        Ok(self.0.delete(wtxn, shard)?)
    }

    /// Remove all documents from the shards without removing the shards.
    ///
    /// Use in `ClearDocuments::execute`
    pub fn clear_documents(&self, wtxn: &mut RwTxn<'_>) -> Result<()> {
        // we want to spare us from deserializing the docids for each shard here since we just want to clear them,
        // so we need to put with a different type as the get, but `remap_type` moves the iterator,
        // so we declare two bindings and let them "play ball" by moving the iterator
        // between the two bindings.
        let mut put_it = self.0.iter_mut(wtxn)?;
        let mut get_it = put_it.remap_data_type::<DecodeIgnore>();

        while let Some(entry) = get_it.next() {
            let (k, _) = entry?;
            let k = k.to_owned();
            put_it = get_it.remap_data_type();
            // SAFETY: k has been moved to an owned value so we are not passing any reference to LMDB data
            unsafe {
                put_it.put_current(&k, &RoaringBitmap::new())?;
            }
            get_it = put_it.remap_data_type();
        }
        Ok(())
    }

    /// Remove all shards with their docids.
    ///
    /// Use when the leader is removed.
    pub fn remove_all_shards(&self, wtxn: &mut RwTxn<'_>) -> Result<()> {
        Ok(self.0.clear(wtxn)?)
    }
}

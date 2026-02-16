#[cfg(not(feature = "enterprise"))]
pub mod community_edition;
#[cfg(feature = "enterprise")]
pub mod enterprise_edition;
use std::collections::BTreeSet;

use heed::types::{Bytes, DecodeIgnore, Str};
use heed::{Database, RoTxn, RwTxn};
use roaring::RoaringBitmap;

use crate::{CboRoaringBitmapCodec, Index, Result};

#[derive(Debug, Clone)]
pub struct Shards(Vec<Shard>);

#[derive(Debug, Clone)]
pub struct Shard {
    pub is_own: bool,
    pub name: String,
}

impl Shards {
    /// The shards as a slice of shards sorted alphabetically
    pub fn as_sorted_slice(&self) -> &[Shard] {
        &self.0
    }
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

    /// Intersection of the specified roaring with the docids for the specified shard.
    ///
    /// Returns `Ok(None)` if the specified shard doesn't exist in the index.
    pub fn docids_intersection(
        &self,
        rtxn: &RoTxn<'_>,
        shard: &str,
        universe: Option<&RoaringBitmap>,
    ) -> Result<Option<RoaringBitmap>> {
        Ok(if let Some(universe) = universe {
            let db = self.0.remap_data_type::<Bytes>();
            let Some(docids) = db.get(rtxn, shard)? else { return Ok(None) };
            Some(CboRoaringBitmapCodec::intersection_with_serialized(docids, universe)?)
        } else {
            self.0.get(rtxn, shard)?
        })
    }

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

    /// Add all shards that newly belong to the list of shards, and remove all shards that no longer belong to the list of shards.
    ///
    /// - If the shard list is unchanged, returns `None`
    /// - If the shard list is modified, returns `Some(orphans)`, with `orphans` the docids that no longer belong to any shard and will
    ///   need to be redistributed.
    pub fn rebalance_shards<'network>(
        &self,
        index: &Index,
        wtxn: &mut RwTxn<'_>,
        network_shards: &'network Shards,
    ) -> Result<ShardBalancingOutcome<'network>> {
        // we list the documents that were without shard before the rebalancing.
        // this list should normally be empty, as instances without shards should be empty when added to sharding.
        // this lets us correct and signal any error.
        let mut unsharded = index.documents_ids(wtxn)?;

        let db_keys: Result<Vec<_>> = self
            .0
            .remap_data_type::<DecodeIgnore>()
            .iter(wtxn)?
            .map(|res| {
                let (k, _) = res?;
                Ok(k.to_owned())
            })
            .collect();

        // documents that lose their current shard because it is being removed.
        // they will need to be resharded among all other shards.
        let mut desharded = RoaringBitmap::new();
        // newly added shards
        let mut new_shards = BTreeSet::new();
        let mut existing_shards = BTreeSet::new();

        let db_keys = db_keys?;
        for eob in
            itertools::merge_join_by(db_keys, network_shards.as_sorted_slice(), |left, right| {
                left.cmp(&right.name)
            })
        {
            match eob {
                itertools::EitherOrBoth::Both(left, _) => {
                    // unchanged shard, nothing to do
                    let docids = self.0.get(wtxn, &left)?.unwrap_or_default();
                    unsharded -= &docids;
                    existing_shards.insert(left);
                }
                itertools::EitherOrBoth::Left(db) => {
                    let docids = self.0.get(wtxn, &db)?.unwrap_or_default();
                    unsharded -= &docids;
                    desharded |= &docids;
                    self.remove_shard(wtxn, &db)?;
                }
                itertools::EitherOrBoth::Right(network) => {
                    self.add_shard(wtxn, &network.name)?;
                    new_shards.insert(network.name.as_str());
                }
            }
        }

        // check if we have unsharded documents
        let unsharded_len = unsharded.len();
        if unsharded_len != 0 {
            tracing::warn!("Resharding {unsharded_len} documents that are unexpectedly unsharded");
        }

        unsharded |= desharded;

        Ok(ShardBalancingOutcome { unsharded, new_shards, existing_shards })
    }
}

pub struct ShardBalancingOutcome<'network> {
    pub unsharded: RoaringBitmap,
    pub new_shards: BTreeSet<&'network str>,
    pub existing_shards: BTreeSet<String>,
}

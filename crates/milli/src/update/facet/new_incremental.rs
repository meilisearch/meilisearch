use std::ops::Bound;

use heed::types::{Bytes, DecodeIgnore};
use heed::{BytesDecode as _, Database, RwTxn};
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValue, FacetGroupValueCodec,
};
use crate::heed_codec::BytesRefCodec;
use crate::search::facet::get_highest_level;
use crate::{FieldId, Index, Result};

pub struct FacetsUpdateIncremental {
    inner: FacetsUpdateIncrementalInner,
    delta_data: Vec<FacetFieldIdChange>,
}

struct FacetsUpdateIncrementalInner {
    db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    field_id: FieldId,
    group_size: u8,
    min_level_size: u8,
    max_group_size: u8,
}

impl FacetsUpdateIncremental {
    pub fn new(
        index: &Index,
        facet_type: FacetType,
        field_id: FieldId,
        delta_data: Vec<FacetFieldIdChange>,
        group_size: u8,
        min_level_size: u8,
        max_group_size: u8,
    ) -> Self {
        FacetsUpdateIncremental {
            inner: FacetsUpdateIncrementalInner {
                db: match facet_type {
                    FacetType::String => index
                        .facet_id_string_docids
                        .remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>(),
                    FacetType::Number => index
                        .facet_id_f64_docids
                        .remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>(),
                },
                field_id,
                group_size,
                min_level_size,
                max_group_size,
            },

            delta_data,
        }
    }

    #[tracing::instrument(level = "trace", skip_all, target = "indexing::facets::incremental")]
    pub fn execute(mut self, wtxn: &mut RwTxn) -> Result<()> {
        if self.delta_data.is_empty() {
            return Ok(());
        }
        self.delta_data.sort_unstable_by(
            |FacetFieldIdChange { facet_value: left, .. },
             FacetFieldIdChange { facet_value: right, .. }| left.cmp(right),
        );

        self.inner.find_touched_parents(
            wtxn,
            0,
            self.delta_data
                .into_iter()
                // reverse lexicographic order
                .rev()
                .map(|change| change.facet_value.into()),
        )?;

        self.inner.add_or_delete_level(wtxn)
    }
}

impl FacetsUpdateIncrementalInner {
    /// WARNING: `touched_children` must be sorted in **reverse** lexicographic order.
    fn find_touched_parents(
        &self,
        wtxn: &mut RwTxn,
        child_level: u8,
        mut touched_children: impl Iterator<Item = Vec<u8>>,
    ) -> Result<()> {
        let mut touched_parents = vec![];
        let Some(parent_level) = child_level.checked_add(1) else { return Ok(()) };
        let parent_level_left_bound: FacetGroupKey<&[u8]> =
            FacetGroupKey { field_id: self.field_id, level: parent_level, left_bound: &[] };

        let mut last_parent: Option<Vec<u8>> = None;

        for child in &mut touched_children {
            if let Some(last_parent) = &last_parent {
                if child.as_slice() >= last_parent.as_slice() {
                    self.compute_parent_group(wtxn, child_level, child)?;
                    continue;
                }
            }

            // need to find a new parent

            let parent_key_prefix = FacetGroupKey {
                field_id: self.field_id,
                level: parent_level,
                left_bound: child.as_slice(),
            };

            let parent = self
                .db
                .remap_data_type::<DecodeIgnore>()
                .rev_range(
                    wtxn,
                    &(
                        Bound::Excluded(&parent_level_left_bound),
                        Bound::Included(&parent_key_prefix),
                    ),
                )?
                .next();

            match parent {
                Some(Ok((parent_key, _parent_value))) => {
                    // found parent, cache it for next keys
                    last_parent = Some(parent_key.left_bound.to_owned());

                    // add to modified list for parent level
                    touched_parents.push(parent_key.left_bound.to_owned());
                    self.compute_parent_group(wtxn, child_level, child)?;
                }
                Some(Err(err)) => return Err(err.into()),
                None => {
                    self.compute_parent_group(wtxn, child_level, child)?;
                    break;
                }
            }
        }
        // do we have children without parents?
        if let Some(child) = touched_children.next() {
            // no parent for that key
            let mut it = self
                .db
                .remap_data_type::<DecodeIgnore>()
                .prefix_iter_mut(wtxn, &parent_level_left_bound)?;
            match it.next() {
                // 1. left of the current left bound, or
                Some(Ok((first_key, _first_value))) => 'change_left_bound: {
                    // make sure we don't spill on the neighboring fid (level also included defensively)
                    if first_key.field_id != self.field_id || first_key.level != parent_level {
                        break 'change_left_bound;
                    }
                    // remove old left bound
                    unsafe { it.del_current()? };
                    // pop all elements and order to visit the new left bound
                    if let Some(first) = touched_children.last() {
                        touched_parents.push(first);
                    } else {
                        touched_parents.push(child);
                    }
                }
                Some(Err(err)) => return Err(err.into()),
                // 2. max level reached, exit
                None => {}
            }
        }
        self.find_touched_parents(
            wtxn,
            parent_level,
            touched_parents
                // no need to `rev` here because the parents were already visited in reverse order
                .into_iter(),
        )
    }

    fn compute_parent_group(
        &self,
        wtxn: &mut RwTxn<'_>,
        parent_level: u8,
        parent_left_bound: Vec<u8>,
    ) -> Result<()> {
        let mut range_left_bound = parent_left_bound;
        if parent_level == 0 {
            return Ok(());
        }
        let child_level = parent_level - 1;

        let parent_key = FacetGroupKey {
            field_id: self.field_id,
            level: parent_level,
            left_bound: range_left_bound.as_slice(),
        };
        let child_right_bound = self
            .db
            .remap_data_type::<DecodeIgnore>()
            .get_greater_than(wtxn, &parent_key)?
            .and_then(
                |(
                    FacetGroupKey {
                        level: right_level,
                        field_id: right_fid,
                        left_bound: right_bound,
                    },
                    _,
                )| {
                    if parent_level != right_level || self.field_id != right_fid {
                        // there was a greater key, but with a greater level or fid, so not a sibling to the parent: ignore
                        return None;
                    }
                    Some(right_bound.to_owned())
                },
            );
        let child_right_bound = match &child_right_bound {
            Some(right_bound) => Bound::Excluded(FacetGroupKey {
                left_bound: right_bound.as_slice(),
                field_id: self.field_id,
                level: child_level,
            }),
            None => Bound::Unbounded,
        };

        let child_left_key = FacetGroupKey {
            field_id: self.field_id,
            level: child_level,
            left_bound: range_left_bound.as_slice(),
        };
        let mut child_left_bound = Bound::Included(child_left_key);

        loop {
            let mut child_it = self.db.range(wtxn, &(child_left_bound, child_right_bound))?;
            let res: Result<_> = child_it
                .by_ref()
                .take(self.max_group_size as usize)
                // stop if we go to the next level or field id
                .take_while(|res| match res {
                    Ok((child_key, _)) => {
                        child_key.field_id == self.field_id && child_key.level == child_level
                    }
                    Err(_) => true,
                })
                .try_fold(
                    (None, FacetGroupValue { size: 0, bitmap: Default::default() }),
                    |(bounds, mut group_value), child_res| {
                        let (child_key, child_value) = child_res?;
                        let bounds = match bounds {
                            Some((left_bound, _)) => Some((left_bound, child_key.left_bound)),
                            None => Some((child_key.left_bound, child_key.left_bound)),
                        };
                        // max_group_size <= u8::MAX
                        group_value.size += 1;
                        group_value.bitmap |= &child_value.bitmap;
                        Ok((bounds, group_value))
                    },
                );

            let (bounds, group_value) = res?;

            let Some((group_left_bound, right_bound)) = bounds else {
                let update_key = FacetGroupKey {
                    field_id: self.field_id,
                    level: parent_level,
                    left_bound: range_left_bound.as_slice(),
                };
                drop(child_it);
                self.db.delete(wtxn, &update_key)?;
                break;
            };

            let current_left_bound = group_left_bound.to_owned();
            range_left_bound.clear();
            range_left_bound.extend_from_slice(right_bound);
            let child_left_key = FacetGroupKey {
                field_id: self.field_id,
                level: child_level,
                left_bound: range_left_bound.as_slice(),
            };
            child_left_bound = Bound::Excluded(child_left_key);
            drop(child_it);
            let update_key = FacetGroupKey {
                field_id: self.field_id,
                level: parent_level,
                left_bound: current_left_bound.as_slice(),
            };
            if group_value.bitmap.is_empty() {
                self.db.delete(wtxn, &update_key)?;
            } else {
                self.db.put(wtxn, &update_key, &group_value)?;
            }
        }

        Ok(())
    }

    /// Check whether the highest level has exceeded `min_level_size` * `self.group_size`.
    /// If it has, we must build an addition level above it.
    /// Then check whether the highest level is under `min_level_size`.
    /// If it has, we must remove the complete level.
    pub(crate) fn add_or_delete_level(&self, txn: &mut RwTxn<'_>) -> Result<()> {
        let highest_level = get_highest_level(txn, self.db, self.field_id)?;
        let mut highest_level_prefix = vec![];
        highest_level_prefix.extend_from_slice(&self.field_id.to_be_bytes());
        highest_level_prefix.push(highest_level);

        let size_highest_level =
            self.db.remap_types::<Bytes, Bytes>().prefix_iter(txn, &highest_level_prefix)?.count();

        if size_highest_level >= self.group_size as usize * self.min_level_size as usize {
            self.add_level(txn, highest_level, &highest_level_prefix, size_highest_level)
        } else if size_highest_level < self.min_level_size as usize && highest_level != 0 {
            self.delete_level(txn, &highest_level_prefix)
        } else {
            Ok(())
        }
    }

    /// Delete a level.
    fn delete_level(&self, txn: &mut RwTxn<'_>, highest_level_prefix: &[u8]) -> Result<()> {
        let mut to_delete = vec![];
        let mut iter =
            self.db.remap_types::<Bytes, Bytes>().prefix_iter(txn, highest_level_prefix)?;
        for el in iter.by_ref() {
            let (k, _) = el?;
            to_delete.push(
                FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(k)
                    .map_err(heed::Error::Encoding)?
                    .into_owned(),
            );
        }
        drop(iter);
        for k in to_delete {
            self.db.delete(txn, &k.as_ref())?;
        }
        Ok(())
    }

    /// Build an additional level for the field id.
    fn add_level(
        &self,
        txn: &mut RwTxn<'_>,
        highest_level: u8,
        highest_level_prefix: &[u8],
        size_highest_level: usize,
    ) -> Result<()> {
        let mut groups_iter = self
            .db
            .remap_types::<Bytes, FacetGroupValueCodec>()
            .prefix_iter(txn, highest_level_prefix)?;

        let nbr_new_groups = size_highest_level / self.group_size as usize;
        let nbr_leftover_elements = size_highest_level % self.group_size as usize;

        let mut to_add = vec![];
        for _ in 0..nbr_new_groups {
            let mut first_key = None;
            let mut values = RoaringBitmap::new();
            for _ in 0..self.group_size {
                let (key_bytes, value_i) = groups_iter.next().unwrap()?;
                let key_i = FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(key_bytes)
                    .map_err(heed::Error::Encoding)?;

                if first_key.is_none() {
                    first_key = Some(key_i);
                }
                values |= value_i.bitmap;
            }
            let key = FacetGroupKey {
                field_id: self.field_id,
                level: highest_level + 1,
                left_bound: first_key.unwrap().left_bound,
            };
            let value = FacetGroupValue { size: self.group_size, bitmap: values };
            to_add.push((key.into_owned(), value));
        }
        // now we add the rest of the level, in case its size is > group_size * min_level_size
        // this can indeed happen if the min_level_size parameter changes between two calls to `insert`
        if nbr_leftover_elements > 0 {
            let mut first_key = None;
            let mut values = RoaringBitmap::new();
            for _ in 0..nbr_leftover_elements {
                let (key_bytes, value_i) = groups_iter.next().unwrap()?;
                let key_i = FacetGroupKeyCodec::<BytesRefCodec>::bytes_decode(key_bytes)
                    .map_err(heed::Error::Encoding)?;

                if first_key.is_none() {
                    first_key = Some(key_i);
                }
                values |= value_i.bitmap;
            }
            let key = FacetGroupKey {
                field_id: self.field_id,
                level: highest_level + 1,
                left_bound: first_key.unwrap().left_bound,
            };
            // Note: nbr_leftover_elements can be casted to a u8 since it is bounded by `max_group_size`
            // when it is created above.
            let value = FacetGroupValue { size: nbr_leftover_elements as u8, bitmap: values };
            to_add.push((key.into_owned(), value));
        }

        drop(groups_iter);
        for (key, value) in to_add {
            self.db.put(txn, &key.as_ref(), &value)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct FacetFieldIdChange {
    pub facet_value: Box<[u8]>,
    pub operation: FacetFieldIdOperation,
}

#[derive(Debug, Clone, Copy)]
pub enum FacetFieldIdOperation {
    /// The docids have been modified for an existing facet value
    ///
    /// The modification must be propagated to upper levels, without changing the structure of the tree
    InPlace,
    /// A new value has been inserted
    ///
    /// The modification must be propagated to upper levels, splitting nodes and adding new levels as necessary.
    Insert,
    /// An existing value has been deleted
    ///
    /// The modification must be propagated to upper levels, merging nodes and removing levels as necessary.
    Remove,
}

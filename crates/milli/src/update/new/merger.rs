use std::cell::RefCell;

use hashbrown::HashMap;
use heed::types::Bytes;
use heed::{Database, RoTxn};
use memmap2::Mmap;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use roaring::RoaringBitmap;

use super::channel::*;
use super::extract::{
    merge_caches_sorted, transpose_and_freeze_caches, BalancedCaches, DelAddRoaringBitmap,
    FacetKind, GeoExtractorData,
};
use crate::update::facet::new_incremental::FacetFieldIdChange;
use crate::{CboRoaringBitmapCodec, FieldId, GeoPoint, Index, InternalError, Result};

#[tracing::instrument(level = "trace", skip_all, target = "indexing::merge")]
pub fn merge_and_send_rtree<'extractor, MSP>(
    datastore: impl IntoIterator<Item = RefCell<GeoExtractorData<'extractor>>>,
    rtxn: &RoTxn,
    index: &Index,
    geo_sender: GeoSender<'_, '_>,
    must_stop_processing: &MSP,
) -> Result<()>
where
    MSP: Fn() -> bool + Sync,
{
    let mut rtree = index.geo_rtree(rtxn)?.unwrap_or_default();
    let mut faceted = index.geo_faceted_documents_ids(rtxn)?;

    for data in datastore {
        if must_stop_processing() {
            return Err(InternalError::AbortedIndexation.into());
        }

        let mut frozen = data.into_inner().freeze()?;
        for result in frozen.iter_and_clear_removed()? {
            let extracted_geo_point = result?;
            let removed = rtree.remove(&GeoPoint::from(extracted_geo_point));
            debug_assert!(removed.is_some());
            let removed = faceted.remove(extracted_geo_point.docid);
            debug_assert!(removed);
        }

        for result in frozen.iter_and_clear_inserted()? {
            let extracted_geo_point = result?;
            rtree.insert(GeoPoint::from(extracted_geo_point));
            let inserted = faceted.insert(extracted_geo_point.docid);
            debug_assert!(inserted);
        }
    }

    let mut file = tempfile::tempfile()?;
    bincode::serialize_into(&mut file, &rtree).map_err(InternalError::BincodeError)?;
    file.sync_all()?;

    let rtree_mmap = unsafe { Mmap::map(&file)? };
    geo_sender.set_rtree(rtree_mmap).unwrap();
    geo_sender.set_geo_faceted(&faceted)?;

    Ok(())
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::merge")]
pub fn merge_and_send_docids<'extractor, MSP, D>(
    mut caches: Vec<BalancedCaches<'extractor>>,
    database: Database<Bytes, Bytes>,
    index: &Index,
    docids_sender: WordDocidsSender<D>,
    must_stop_processing: &MSP,
) -> Result<()>
where
    MSP: Fn() -> bool + Sync,
    D: DatabaseType + Sync,
{
    transpose_and_freeze_caches(&mut caches)?.into_par_iter().try_for_each(|frozen| {
        let rtxn = index.read_txn()?;
        if must_stop_processing() {
            return Err(InternalError::AbortedIndexation.into());
        }
        merge_caches_sorted(frozen, |key, DelAddRoaringBitmap { del, add }| {
            let current = database.get(&rtxn, key)?;
            match merge_cbo_bitmaps(current, del, add)? {
                Operation::Write(bitmap) => {
                    docids_sender.write(key, &bitmap)?;
                    Ok(())
                }
                Operation::Delete => {
                    docids_sender.delete(key)?;
                    Ok(())
                }
                Operation::Ignore => Ok(()),
            }
        })
    })
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::merge")]
pub fn merge_and_send_facet_docids<'extractor>(
    mut caches: Vec<BalancedCaches<'extractor>>,
    database: FacetDatabases,
    index: &Index,
    rtxn: &RoTxn,
    docids_sender: FacetDocidsSender,
) -> Result<FacetFieldIdsDelta> {
    let max_string_count = (index.facet_id_string_docids.len(rtxn)? / 500) as usize;
    let max_number_count = (index.facet_id_f64_docids.len(rtxn)? / 500) as usize;
    let max_string_count = max_string_count.clamp(1000, 100_000);
    let max_number_count = max_number_count.clamp(1000, 100_000);
    transpose_and_freeze_caches(&mut caches)?
        .into_par_iter()
        .map(|frozen| {
            let mut facet_field_ids_delta =
                FacetFieldIdsDelta::new(max_string_count, max_number_count);
            let rtxn = index.read_txn()?;
            merge_caches_sorted(frozen, |key, DelAddRoaringBitmap { del, add }| {
                let current = database.get_cbo_roaring_bytes_value(&rtxn, key)?;
                match merge_cbo_bitmaps(current, del, add)? {
                    Operation::Write(bitmap) => {
                        facet_field_ids_delta.register_from_key(key);
                        docids_sender.write(key, &bitmap)?;
                        Ok(())
                    }
                    Operation::Delete => {
                        facet_field_ids_delta.register_from_key(key);
                        docids_sender.delete(key)?;
                        Ok(())
                    }
                    Operation::Ignore => Ok(()),
                }
            })?;

            Ok(facet_field_ids_delta)
        })
        .reduce(
            || Ok(FacetFieldIdsDelta::new(max_string_count, max_number_count)),
            |lhs, rhs| Ok(lhs?.merge(rhs?)),
        )
}

pub struct FacetDatabases<'a> {
    index: &'a Index,
}

impl<'a> FacetDatabases<'a> {
    pub fn new(index: &'a Index) -> Self {
        Self { index }
    }

    fn get_cbo_roaring_bytes_value<'t>(
        &self,
        rtxn: &'t RoTxn<'_>,
        key: &[u8],
    ) -> heed::Result<Option<&'t [u8]>> {
        let (facet_kind, key) = FacetKind::extract_from_key(key);

        let value =
            super::channel::Database::from(facet_kind).database(self.index).get(rtxn, key)?;
        match facet_kind {
            // skip level group size
            FacetKind::String | FacetKind::Number => Ok(value.map(|v| &v[1..])),
            _ => Ok(value),
        }
    }
}

#[derive(Debug)]
pub enum FacetFieldIdDelta {
    Bulk,
    Incremental(Vec<FacetFieldIdChange>),
}

impl FacetFieldIdDelta {
    fn push(&mut self, facet_value: &[u8], max_count: usize) {
        *self = match std::mem::replace(self, FacetFieldIdDelta::Bulk) {
            FacetFieldIdDelta::Bulk => FacetFieldIdDelta::Bulk,
            FacetFieldIdDelta::Incremental(mut v) => {
                if v.len() >= max_count {
                    FacetFieldIdDelta::Bulk
                } else {
                    v.push(FacetFieldIdChange { facet_value: facet_value.into() });
                    FacetFieldIdDelta::Incremental(v)
                }
            }
        }
    }

    fn merge(&mut self, rhs: Option<Self>, max_count: usize) {
        let Some(rhs) = rhs else {
            return;
        };
        *self = match (std::mem::replace(self, FacetFieldIdDelta::Bulk), rhs) {
            (FacetFieldIdDelta::Bulk, _) | (_, FacetFieldIdDelta::Bulk) => FacetFieldIdDelta::Bulk,
            (
                FacetFieldIdDelta::Incremental(mut left),
                FacetFieldIdDelta::Incremental(mut right),
            ) => {
                if left.len() + right.len() >= max_count {
                    FacetFieldIdDelta::Bulk
                } else {
                    left.append(&mut right);
                    FacetFieldIdDelta::Incremental(left)
                }
            }
        };
    }
}

#[derive(Debug)]
pub struct FacetFieldIdsDelta {
    /// The field ids that have been modified
    modified_facet_string_ids: HashMap<FieldId, FacetFieldIdDelta, rustc_hash::FxBuildHasher>,
    modified_facet_number_ids: HashMap<FieldId, FacetFieldIdDelta, rustc_hash::FxBuildHasher>,
    max_string_count: usize,
    max_number_count: usize,
}

impl FacetFieldIdsDelta {
    pub fn new(max_string_count: usize, max_number_count: usize) -> Self {
        Self {
            max_string_count,
            max_number_count,
            modified_facet_string_ids: Default::default(),
            modified_facet_number_ids: Default::default(),
        }
    }

    fn register_facet_string_id(&mut self, field_id: FieldId, facet_value: &[u8]) {
        self.modified_facet_string_ids
            .entry(field_id)
            .or_insert(FacetFieldIdDelta::Incremental(Default::default()))
            .push(facet_value, self.max_string_count);
    }

    fn register_facet_number_id(&mut self, field_id: FieldId, facet_value: &[u8]) {
        self.modified_facet_number_ids
            .entry(field_id)
            .or_insert(FacetFieldIdDelta::Incremental(Default::default()))
            .push(facet_value, self.max_number_count);
    }

    fn register_from_key(&mut self, key: &[u8]) {
        let (facet_kind, field_id, facet_value) = self.extract_key_data(key);
        match (facet_kind, facet_value) {
            (FacetKind::Number, Some(facet_value)) => {
                self.register_facet_number_id(field_id, facet_value)
            }
            (FacetKind::String, Some(facet_value)) => {
                self.register_facet_string_id(field_id, facet_value)
            }
            _ => (),
        }
    }

    fn extract_key_data<'key>(&self, key: &'key [u8]) -> (FacetKind, FieldId, Option<&'key [u8]>) {
        let facet_kind = FacetKind::from(key[0]);
        let field_id = FieldId::from_be_bytes([key[1], key[2]]);
        let facet_value = if key.len() >= 4 {
            // level is also stored in the key at [3] (always 0)
            Some(&key[4..])
        } else {
            None
        };

        (facet_kind, field_id, facet_value)
    }

    pub fn consume_facet_string_delta(
        &mut self,
    ) -> impl Iterator<Item = (FieldId, FacetFieldIdDelta)> + '_ {
        self.modified_facet_string_ids.drain()
    }

    pub fn consume_facet_number_delta(
        &mut self,
    ) -> impl Iterator<Item = (FieldId, FacetFieldIdDelta)> + '_ {
        self.modified_facet_number_ids.drain()
    }

    pub fn merge(mut self, rhs: Self) -> Self {
        // rhs.max_xx_count is assumed to be equal to self.max_xx_count, and so gets unused
        let Self { modified_facet_number_ids, modified_facet_string_ids, .. } = rhs;
        modified_facet_number_ids.into_iter().for_each(|(fid, mut delta)| {
            let old_delta = self.modified_facet_number_ids.remove(&fid);
            delta.merge(old_delta, self.max_number_count);
            self.modified_facet_number_ids.insert(fid, delta);
        });
        modified_facet_string_ids.into_iter().for_each(|(fid, mut delta)| {
            let old_delta = self.modified_facet_string_ids.remove(&fid);
            delta.merge(old_delta, self.max_string_count);
            self.modified_facet_string_ids.insert(fid, delta);
        });
        self
    }
}

enum Operation {
    Write(RoaringBitmap),
    Delete,
    Ignore,
}

/// A function that merges the DelAdd CboRoaringBitmaps with the current bitmap.
fn merge_cbo_bitmaps(
    current: Option<&[u8]>,
    del: Option<RoaringBitmap>,
    add: Option<RoaringBitmap>,
) -> Result<Operation> {
    let current = current.map(CboRoaringBitmapCodec::deserialize_from).transpose()?;
    match (current, del, add) {
        (None, None, None) => Ok(Operation::Ignore), // but it's strange
        (None, None, Some(add)) => Ok(Operation::Write(add)),
        (None, Some(_del), None) => Ok(Operation::Ignore), // but it's strange
        (None, Some(_del), Some(add)) => Ok(Operation::Write(add)),
        (Some(_current), None, None) => Ok(Operation::Ignore), // but it's strange
        (Some(current), None, Some(add)) => Ok(Operation::Write(current | add)),
        (Some(current), Some(del), add) => {
            debug_assert!(
                del.is_subset(&current),
                "del is not a subset of current, which must be impossible."
            );
            let output = match add {
                Some(add) => (&current - (&del - &add)) | (add - del),
                None => &current - del,
            };
            if output.is_empty() {
                Ok(Operation::Delete)
            } else if current == output {
                Ok(Operation::Ignore)
            } else {
                Ok(Operation::Write(output))
            }
        }
    }
}

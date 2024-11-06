use std::io::{self};

use bincode::ErrorKind;
use hashbrown::HashSet;
use heed::types::Bytes;
use heed::{Database, RoTxn};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use roaring::RoaringBitmap;

use super::channel::*;
use super::extract::{
    merge_caches, transpose_and_freeze_caches, BalancedCaches, DelAddRoaringBitmap, FacetKind,
};
use super::DocumentChange;
use crate::{CboRoaringBitmapCodec, Error, FieldId, GeoPoint, GlobalFieldsIdsMap, Index, Result};

pub struct GeoExtractor {
    rtree: Option<rstar::RTree<GeoPoint>>,
}

impl GeoExtractor {
    pub fn new(rtxn: &RoTxn, index: &Index) -> Result<Option<Self>> {
        let is_sortable = index.sortable_fields(rtxn)?.contains("_geo");
        let is_filterable = index.filterable_fields(rtxn)?.contains("_geo");
        if is_sortable || is_filterable {
            Ok(Some(GeoExtractor { rtree: index.geo_rtree(rtxn)? }))
        } else {
            Ok(None)
        }
    }

    pub fn manage_change(
        &mut self,
        fidmap: &mut GlobalFieldsIdsMap,
        change: &DocumentChange,
    ) -> Result<()> {
        match change {
            DocumentChange::Deletion(_) => todo!(),
            DocumentChange::Update(_) => todo!(),
            DocumentChange::Insertion(_) => todo!(),
        }
    }

    pub fn serialize_rtree<W: io::Write>(self, writer: &mut W) -> Result<bool> {
        match self.rtree {
            Some(rtree) => {
                // TODO What should I do?
                bincode::serialize_into(writer, &rtree).map(|_| true).map_err(|e| match *e {
                    ErrorKind::Io(e) => Error::IoError(e),
                    ErrorKind::InvalidUtf8Encoding(_) => todo!(),
                    ErrorKind::InvalidBoolEncoding(_) => todo!(),
                    ErrorKind::InvalidCharEncoding => todo!(),
                    ErrorKind::InvalidTagEncoding(_) => todo!(),
                    ErrorKind::DeserializeAnyNotSupported => todo!(),
                    ErrorKind::SizeLimit => todo!(),
                    ErrorKind::SequenceMustHaveLength => todo!(),
                    ErrorKind::Custom(_) => todo!(),
                })
            }
            None => Ok(false),
        }
    }
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::merge")]
pub fn merge_and_send_docids<'extractor>(
    mut caches: Vec<BalancedCaches<'extractor>>,
    database: Database<Bytes, Bytes>,
    index: &Index,
    docids_sender: impl DocidsSender + Sync,
) -> Result<()> {
    transpose_and_freeze_caches(&mut caches)?.into_par_iter().try_for_each(|frozen| {
        let rtxn = index.read_txn()?;
        let mut buffer = Vec::new();
        merge_caches(frozen, |key, DelAddRoaringBitmap { del, add }| {
            let current = database.get(&rtxn, key)?;
            match merge_cbo_bitmaps(current, del, add)? {
                Operation::Write(bitmap) => {
                    let value = cbo_bitmap_serialize_into_vec(&bitmap, &mut buffer);
                    docids_sender.write(key, value).unwrap();
                    Ok(())
                }
                Operation::Delete => {
                    docids_sender.delete(key).unwrap();
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
    docids_sender: impl DocidsSender + Sync,
) -> Result<FacetFieldIdsDelta> {
    transpose_and_freeze_caches(&mut caches)?
        .into_par_iter()
        .map(|frozen| {
            let mut facet_field_ids_delta = FacetFieldIdsDelta::default();
            let rtxn = index.read_txn()?;
            let mut buffer = Vec::new();
            merge_caches(frozen, |key, DelAddRoaringBitmap { del, add }| {
                let current = database.get_cbo_roaring_bytes_value(&rtxn, key)?;
                match merge_cbo_bitmaps(current, del, add)? {
                    Operation::Write(bitmap) => {
                        facet_field_ids_delta.register_from_key(key);
                        let value = cbo_bitmap_serialize_into_vec(&bitmap, &mut buffer);
                        docids_sender.write(key, value).unwrap();
                        Ok(())
                    }
                    Operation::Delete => {
                        facet_field_ids_delta.register_from_key(key);
                        docids_sender.delete(key).unwrap();
                        Ok(())
                    }
                    Operation::Ignore => Ok(()),
                }
            })?;

            Ok(facet_field_ids_delta)
        })
        .reduce(|| Ok(FacetFieldIdsDelta::default()), |lhs, rhs| Ok(lhs?.merge(rhs?)))
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

#[derive(Debug, Default)]
pub struct FacetFieldIdsDelta {
    /// The field ids that have been modified
    modified_facet_string_ids: HashSet<FieldId>,
    modified_facet_number_ids: HashSet<FieldId>,
}

impl FacetFieldIdsDelta {
    fn register_facet_string_id(&mut self, field_id: FieldId) {
        self.modified_facet_string_ids.insert(field_id);
    }

    fn register_facet_number_id(&mut self, field_id: FieldId) {
        self.modified_facet_number_ids.insert(field_id);
    }

    fn register_from_key(&mut self, key: &[u8]) {
        let (facet_kind, field_id) = self.extract_key_data(key);
        match facet_kind {
            FacetKind::Number => self.register_facet_number_id(field_id),
            FacetKind::String => self.register_facet_string_id(field_id),
            _ => (),
        }
    }

    fn extract_key_data(&self, key: &[u8]) -> (FacetKind, FieldId) {
        let facet_kind = FacetKind::from(key[0]);
        let field_id = FieldId::from_be_bytes([key[1], key[2]]);
        (facet_kind, field_id)
    }

    pub fn modified_facet_string_ids(&self) -> Option<Vec<FieldId>> {
        if self.modified_facet_string_ids.is_empty() {
            None
        } else {
            Some(self.modified_facet_string_ids.iter().copied().collect())
        }
    }

    pub fn modified_facet_number_ids(&self) -> Option<Vec<FieldId>> {
        if self.modified_facet_number_ids.is_empty() {
            None
        } else {
            Some(self.modified_facet_number_ids.iter().copied().collect())
        }
    }

    pub fn merge(mut self, rhs: Self) -> Self {
        let Self { modified_facet_number_ids, modified_facet_string_ids } = rhs;
        modified_facet_number_ids.into_iter().for_each(|fid| {
            self.modified_facet_number_ids.insert(fid);
        });
        modified_facet_string_ids.into_iter().for_each(|fid| {
            self.modified_facet_string_ids.insert(fid);
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
            let output = match add {
                Some(add) => (&current - del) | add,
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

/// TODO Return the slice directly from the serialize_into method
fn cbo_bitmap_serialize_into_vec<'b>(bitmap: &RoaringBitmap, buffer: &'b mut Vec<u8>) -> &'b [u8] {
    buffer.clear();
    CboRoaringBitmapCodec::serialize_into(bitmap, buffer);
    buffer.as_slice()
}

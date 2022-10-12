use super::{FACET_GROUP_SIZE, FACET_MAX_GROUP_SIZE, FACET_MIN_LEVEL_SIZE};
use crate::{
    facet::FacetType,
    heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec},
    heed_codec::ByteSliceRefCodec,
    update::{FacetsUpdateBulk, FacetsUpdateIncrementalInner},
    FieldId, Index, Result,
};
use heed::RwTxn;
use roaring::RoaringBitmap;
use std::collections::{HashMap, HashSet};

pub struct FacetsDelete<'i, 'b> {
    index: &'i Index,
    database: heed::Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
    facet_type: FacetType,
    affected_facet_values: HashMap<FieldId, HashSet<Vec<u8>>>,
    docids_to_delete: &'b RoaringBitmap,
    group_size: u8,
    max_group_size: u8,
    min_level_size: u8,
}
impl<'i, 'b> FacetsDelete<'i, 'b> {
    pub fn new(
        index: &'i Index,
        facet_type: FacetType,
        affected_facet_values: HashMap<FieldId, HashSet<Vec<u8>>>,
        docids_to_delete: &'b RoaringBitmap,
    ) -> Self {
        let database = match facet_type {
            FacetType::String => index
                .facet_id_string_docids
                .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
            FacetType::Number => {
                index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>()
            }
        };
        Self {
            index,
            database,
            facet_type,
            affected_facet_values,
            docids_to_delete,
            group_size: FACET_GROUP_SIZE,
            max_group_size: FACET_MAX_GROUP_SIZE,
            min_level_size: FACET_MIN_LEVEL_SIZE,
        }
    }

    pub fn execute(self, wtxn: &mut RwTxn) -> Result<()> {
        for (field_id, affected_facet_values) in self.affected_facet_values {
            if affected_facet_values.len() >= (self.database.len(wtxn)? / 50) {
                // Bulk delete
                let mut modified = false;

                for facet_value in affected_facet_values {
                    let key =
                        FacetGroupKey { field_id, level: 0, left_bound: facet_value.as_slice() };
                    let mut old = self.database.get(wtxn, &key)?.unwrap();
                    let previous_len = old.bitmap.len();
                    old.bitmap -= self.docids_to_delete;
                    if old.bitmap.is_empty() {
                        modified = true;
                        self.database.delete(wtxn, &key)?;
                    } else if old.bitmap.len() != previous_len {
                        modified = true;
                        self.database.put(wtxn, &key, &old)?;
                    }
                }
                if modified {
                    let builder = FacetsUpdateBulk::new_not_updating_level_0(
                        self.index,
                        vec![field_id],
                        self.facet_type,
                    );
                    builder.execute(wtxn)?;
                }
            } else {
                // Incremental
                let inc = FacetsUpdateIncrementalInner {
                    db: self.database,
                    group_size: self.group_size,
                    min_level_size: self.min_level_size,
                    max_group_size: self.max_group_size,
                };
                for facet_value in affected_facet_values {
                    inc.delete(wtxn, field_id, facet_value.as_slice(), &self.docids_to_delete)?;
                }
            }
        }
        Ok(())
    }
}

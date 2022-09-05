use self::incremental::FacetsUpdateIncremental;
use super::FacetsUpdateBulk;
use crate::facet::FacetType;
use crate::heed_codec::facet::{ByteSliceRef, FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::{Index, Result};
use std::fs::File;

pub mod bulk;
pub mod incremental;

pub struct FacetsUpdate<'i> {
    index: &'i Index,
    database: heed::Database<FacetGroupKeyCodec<ByteSliceRef>, FacetGroupValueCodec>,
    facet_type: FacetType,
    new_data: grenad::Reader<File>,
    // Options:
    // there's no way to change these for now
    level_group_size: u8,
    max_level_group_size: u8,
    min_level_size: u8,
}
impl<'i> FacetsUpdate<'i> {
    pub fn new(index: &'i Index, facet_type: FacetType, new_data: grenad::Reader<File>) -> Self {
        let database = match facet_type {
            FacetType::String => {
                index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRef>>()
            }
            FacetType::Number => {
                index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRef>>()
            }
        };
        Self {
            index,
            database,
            level_group_size: 4,
            max_level_group_size: 8,
            min_level_size: 5,
            facet_type,
            new_data,
        }
    }

    pub fn execute(self, wtxn: &mut heed::RwTxn) -> Result<()> {
        if self.new_data.is_empty() {
            return Ok(());
        }
        // here, come up with a better condition!
        // ideally we'd choose which method to use for each field id individually
        // but I dont' think it's worth the effort yet
        // As a first requirement, we ask that the length of the new data is less
        // than a 1/50th of the length of the database in order to use the incremental
        // method.
        if self.new_data.len() >= (self.database.len(wtxn)? as u64 / 50) {
            let bulk_update = FacetsUpdateBulk::new(self.index, self.facet_type, self.new_data)
                .level_group_size(self.level_group_size)
                .min_level_size(self.min_level_size);
            bulk_update.execute(wtxn)?;
        } else {
            let incremental_update =
                FacetsUpdateIncremental::new(self.index, self.facet_type, self.new_data)
                    .group_size(self.level_group_size)
                    .max_group_size(self.max_level_group_size)
                    .min_level_size(self.min_level_size);
            incremental_update.execute(wtxn)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // here I want to create a benchmark
    // to find out at which point it is faster to do it incrementally

    #[test]
    fn update() {}
}

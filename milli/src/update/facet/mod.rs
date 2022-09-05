use super::{FacetsUpdateBulk, FacetsUpdateIncremental};
use crate::{
    facet::FacetType,
    heed_codec::facet::{ByteSliceRef, FacetGroupKeyCodec, FacetGroupValueCodec},
    CboRoaringBitmapCodec, FieldId, Index, Result,
};
use heed::BytesDecode;
use roaring::RoaringBitmap;
use std::{collections::HashMap, fs::File};

pub mod bulk;
pub mod incremental;

pub struct FacetsUpdate<'i> {
    index: &'i Index,
    database: heed::Database<FacetGroupKeyCodec<ByteSliceRef>, FacetGroupValueCodec>,
    level_group_size: u8,
    max_level_group_size: u8,
    min_level_size: u8,
    facet_type: FacetType,
    new_data: grenad::Reader<File>,
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
        // here, come up with a better condition!
        if self.database.is_empty(wtxn)? {
            let bulk_update = FacetsUpdateBulk::new(self.index, self.facet_type, self.new_data)
                .level_group_size(self.level_group_size)
                .min_level_size(self.min_level_size);
            bulk_update.execute(wtxn)?;
        } else {
            let indexer = FacetsUpdateIncremental::new(self.database)
                .max_group_size(self.max_level_group_size)
                .min_level_size(self.min_level_size);

            let mut new_faceted_docids = HashMap::<FieldId, RoaringBitmap>::default();

            let mut cursor = self.new_data.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                let key = FacetGroupKeyCodec::<ByteSliceRef>::bytes_decode(key)
                    .ok_or(heed::Error::Encoding)?;
                let docids =
                    CboRoaringBitmapCodec::bytes_decode(value).ok_or(heed::Error::Encoding)?;
                indexer.insert(wtxn, key.field_id, key.left_bound, &docids)?;
                *new_faceted_docids.entry(key.field_id).or_default() |= docids;
            }

            for (field_id, new_docids) in new_faceted_docids {
                let mut docids =
                    self.index.faceted_documents_ids(wtxn, field_id, self.facet_type)?;
                docids |= new_docids;
                self.index.put_faceted_documents_ids(wtxn, field_id, self.facet_type, &docids)?;
            }
        }
        Ok(())
    }
}

use std::{collections::HashMap, fs::File};

use grenad::CompressionType;
use heed::BytesDecode;
use roaring::RoaringBitmap;

use crate::{
    facet::FacetType,
    heed_codec::facet::new::{FacetGroupValueCodec, FacetKeyCodec, MyByteSlice},
    CboRoaringBitmapCodec, FieldId, Index, Result,
};

use super::{FacetsUpdateBulk, FacetsUpdateIncremental};

pub mod bulk;
pub mod incremental;

pub struct FacetsUpdate<'i> {
    index: &'i Index,
    database: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
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
                index.facet_id_string_docids.remap_key_type::<FacetKeyCodec<MyByteSlice>>()
            }
            FacetType::Number => {
                index.facet_id_f64_docids.remap_key_type::<FacetKeyCodec<MyByteSlice>>()
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

    // /// The number of elements from the level below that are represented by a single element in the level above
    // ///
    // /// This setting is always greater than or equal to 2.
    // pub fn level_group_size(&mut self, value: u8) -> &mut Self {
    //     self.level_group_size = std::cmp::max(value, 2);
    //     self
    // }

    // /// The minimum number of elements that a level is allowed to have.
    // pub fn min_level_size(&mut self, value: u8) -> &mut Self {
    //     self.min_level_size = std::cmp::max(value, 1);
    //     self
    // }

    pub fn execute(self, wtxn: &mut heed::RwTxn) -> Result<()> {
        if self.database.is_empty(wtxn)? {
            let bulk_update = FacetsUpdateBulk::new(self.index, self.facet_type, self.new_data);
            bulk_update.execute(wtxn)?;
        } else {
            let indexer = FacetsUpdateIncremental::new(self.database);

            let mut new_faceted_docids = HashMap::<FieldId, RoaringBitmap>::default();

            let mut cursor = self.new_data.into_cursor()?;
            while let Some((key, value)) = cursor.move_on_next()? {
                let key =
                    FacetKeyCodec::<MyByteSlice>::bytes_decode(key).ok_or(heed::Error::Encoding)?;
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

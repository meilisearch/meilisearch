use std::borrow::Cow;
use std::io;

use grenad2::MergeFunction;
use roaring::RoaringBitmap;

use crate::update::del_add::DelAdd;
use crate::update::new::indexer::{KvReaderDelAdd, KvWriterDelAdd};

/// Do a union of CboRoaringBitmaps on both sides of a DelAdd obkv
/// separately and outputs a new DelAdd with both unions.
pub struct DelAddRoaringBitmapMerger;

impl MergeFunction for DelAddRoaringBitmapMerger {
    type Error = io::Error;

    fn merge<'a>(
        &self,
        _key: &[u8],
        values: &[Cow<'a, [u8]>],
    ) -> std::result::Result<Cow<'a, [u8]>, Self::Error> {
        if values.len() == 1 {
            Ok(values[0].clone())
        } else {
            // Retrieve the bitmaps from both sides
            let mut del_bitmaps_bytes = Vec::new();
            let mut add_bitmaps_bytes = Vec::new();
            for value in values {
                let obkv: &KvReaderDelAdd = value.as_ref().into();
                if let Some(bitmap_bytes) = obkv.get(DelAdd::Deletion) {
                    del_bitmaps_bytes.push(bitmap_bytes);
                }
                if let Some(bitmap_bytes) = obkv.get(DelAdd::Addition) {
                    add_bitmaps_bytes.push(bitmap_bytes);
                }
            }

            let mut output_deladd_obkv = KvWriterDelAdd::memory();

            // Deletion
            let mut buffer = Vec::new();
            let mut merged = RoaringBitmap::new();
            for bytes in del_bitmaps_bytes {
                merged |= RoaringBitmap::deserialize_unchecked_from(bytes)?;
            }
            merged.serialize_into(&mut buffer)?;
            output_deladd_obkv.insert(DelAdd::Deletion, &buffer)?;

            // Addition
            buffer.clear();
            merged.clear();
            for bytes in add_bitmaps_bytes {
                merged |= RoaringBitmap::deserialize_unchecked_from(bytes)?;
            }
            merged.serialize_into(&mut buffer)?;
            output_deladd_obkv.insert(DelAdd::Addition, &buffer)?;

            output_deladd_obkv.into_inner().map(Cow::from).map_err(Into::into)
        }
    }
}

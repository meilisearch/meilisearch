use std::borrow::Cow;
use std::convert::TryInto;
use std::mem::size_of;

use heed::{BoxedError, BytesDecode};
use roaring::RoaringBitmap;

use crate::heed_codec::BytesDecodeOwned;

pub struct BoRoaringBitmapCodec;

impl BoRoaringBitmapCodec {
    pub fn serialize_into(bitmap: &RoaringBitmap, out: &mut Vec<u8>) {
        out.reserve(bitmap.len() as usize * size_of::<u32>());
        bitmap.iter().map(u32::to_ne_bytes).for_each(|bytes| out.extend_from_slice(&bytes));
    }
}

impl BytesDecode<'_> for BoRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        let mut bitmap = RoaringBitmap::new();

        for chunk in bytes.chunks(size_of::<u32>()) {
            let bytes = chunk.try_into()?;
            bitmap.push(u32::from_ne_bytes(bytes));
        }

        Ok(bitmap)
    }
}

impl BytesDecodeOwned for BoRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::bytes_decode(bytes)
    }
}

impl heed::BytesEncode<'_> for BoRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        let mut out = Vec::new();
        BoRoaringBitmapCodec::serialize_into(item, &mut out);
        Ok(Cow::Owned(out))
    }
}

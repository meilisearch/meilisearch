use std::mem;

use heed::BytesDecode;

use crate::heed_codec::BytesDecodeOwned;

pub struct BoRoaringBitmapLenCodec;

impl BytesDecode<'_> for BoRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        Some((bytes.len() / mem::size_of::<u32>()) as u64)
    }
}

impl BytesDecodeOwned for BoRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode_owned(bytes: &[u8]) -> Option<Self::DItem> {
        Self::bytes_decode(bytes)
    }
}

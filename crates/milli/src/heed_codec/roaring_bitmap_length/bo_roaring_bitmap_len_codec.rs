use std::mem;

use heed::{BoxedError, BytesDecode};

use crate::heed_codec::BytesDecodeOwned;

pub struct BoRoaringBitmapLenCodec;

impl BytesDecode<'_> for BoRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Ok((bytes.len() / mem::size_of::<u32>()) as u64)
    }
}

impl BytesDecodeOwned for BoRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::bytes_decode(bytes)
    }
}

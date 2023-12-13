use std::mem;

use heed::{BoxedError, BytesDecode};

use super::{BoRoaringBitmapLenCodec, RoaringBitmapLenCodec};
use crate::heed_codec::roaring_bitmap::cbo_roaring_bitmap_codec::THRESHOLD;
use crate::heed_codec::BytesDecodeOwned;

pub struct CboRoaringBitmapLenCodec;

impl BytesDecode<'_> for CboRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        if bytes.len() <= THRESHOLD * mem::size_of::<u32>() {
            // If there is threshold or less than threshold integers that can fit into this array
            // of bytes it means that we used the ByteOrder codec serializer.
            BoRoaringBitmapLenCodec::bytes_decode(bytes)
        } else {
            // Otherwise, it means we used the classic RoaringBitmapCodec and
            // that the header takes threshold integers.
            RoaringBitmapLenCodec::bytes_decode(bytes)
        }
    }
}

impl BytesDecodeOwned for CboRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::bytes_decode(bytes)
    }
}

use std::mem;

use super::{BoRoaringBitmapLenCodec, RoaringBitmapLenCodec};
use crate::heed_codec::roaring_bitmap::cbo_roaring_bitmap_codec::THRESHOLD;

pub struct CboRoaringBitmapLenCodec;

impl heed::BytesDecode<'_> for CboRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
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

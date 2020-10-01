use std::borrow::Cow;
use std::mem::size_of;
use roaring::RoaringBitmap;
use super::{BoRoaringBitmapCodec, RoaringBitmapCodec};

/// A conditionnal codec that either use the RoaringBitmap
/// or a lighter ByteOrder en/decoding method.
pub struct CboRoaringBitmapCodec;

impl heed::BytesDecode<'_> for CboRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        if bytes.len() <= 4 * size_of::<u32>() {
            // If there is 4 or less than 4 integers that can fit into this array
            // of bytes it means that we used the ByteOrder codec serializer.
            BoRoaringBitmapCodec::bytes_decode(bytes)
        } else {
            // Otherwise, it means we used the classic RoaringBitmapCodec and
            // that the header takes 4 integers.
            RoaringBitmapCodec::bytes_decode(bytes)
        }
    }
}

impl heed::BytesEncode<'_> for CboRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        if item.len() <= 4 {
            // If the number of items (u32s) to encode is less than or equal to 4
            // it means that it would weigh the same or less than the RoaringBitmap
            // header, so we directly encode them using ByteOrder instead.
            BoRoaringBitmapCodec::bytes_encode(item)
        } else {
            // Otherwise, we use the classic RoaringBitmapCodec that writes a header.
            RoaringBitmapCodec::bytes_encode(item)
        }
    }
}

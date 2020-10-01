use std::borrow::Cow;
use std::io;
use std::mem::size_of;

use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use roaring::RoaringBitmap;

/// A conditionnal codec that either use the RoaringBitmap
/// or a lighter ByteOrder en/decoding method.
pub struct CboRoaringBitmapCodec;

impl CboRoaringBitmapCodec {
    pub fn serialized_size(roaring: &RoaringBitmap) -> usize {
        if roaring.len() <= 4 {
            roaring.len() as usize * size_of::<u32>()
        } else {
            roaring.serialized_size()
        }
    }

    pub fn serialize_into(roaring: &RoaringBitmap, vec: &mut Vec<u8>) -> io::Result<()> {
        if roaring.len() <= 4 {
            // If the number of items (u32s) to encode is less than or equal to 4
            // it means that it would weigh the same or less than the RoaringBitmap
            // header, so we directly encode them using ByteOrder instead.
            for integer in roaring {
                vec.write_u32::<NativeEndian>(integer)?;
            }
            Ok(())
        } else {
            // Otherwise, we use the classic RoaringBitmapCodec that writes a header.
            roaring.serialize_into(vec)
        }
    }

    pub fn deserialize_from(mut bytes: &[u8]) -> io::Result<RoaringBitmap> {
        if bytes.len() <= 4 * size_of::<u32>() {
            // If there is 4 or less than 4 integers that can fit into this array
            // of bytes it means that we used the ByteOrder codec serializer.
            let mut bitmap = RoaringBitmap::new();
            while let Ok(integer) = bytes.read_u32::<NativeEndian>() {
                bitmap.insert(integer);
            }
            Ok(bitmap)
        } else {
            // Otherwise, it means we used the classic RoaringBitmapCodec and
            // that the header takes 4 integers.
            RoaringBitmap::deserialize_from(bytes)
        }
    }
}

impl heed::BytesDecode<'_> for CboRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        Self::deserialize_from(bytes).ok()
    }
}

impl heed::BytesEncode<'_> for CboRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let mut vec = Vec::with_capacity(Self::serialized_size(item));
        Self::serialize_into(item, &mut vec).ok()?;
        Some(Cow::Owned(vec))
    }
}

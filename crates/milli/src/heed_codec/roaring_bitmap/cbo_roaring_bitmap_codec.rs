use std::borrow::Cow;
use std::io::{self, Cursor};
use std::mem::size_of;

use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use heed::BoxedError;
use roaring::RoaringBitmap;

use crate::heed_codec::BytesDecodeOwned;

/// This is the limit where using a byteorder became less size efficient
/// than using a direct roaring encoding, it is also the point where we are able
/// to determine the encoding used only by using the array of bytes length.
pub const THRESHOLD: usize = 7;

/// A conditionnal codec that either use the RoaringBitmap
/// or a lighter ByteOrder en/decoding method.
pub struct CboRoaringBitmapCodec;

impl CboRoaringBitmapCodec {
    /// If the number of items (u32s) to encode is less than or equal to the threshold
    /// it means that it would weigh the same or less than the RoaringBitmap
    /// header, so we directly encode them using ByteOrder instead.
    pub fn bitmap_serialize_as_raw_u32s(roaring: &RoaringBitmap) -> bool {
        roaring.len() <= THRESHOLD as u64
    }

    pub fn bytes_deserialize_as_raw_u32s(bytes: &[u8]) -> bool {
        bytes.len() <= THRESHOLD * size_of::<u32>()
    }

    pub fn serialized_size(roaring: &RoaringBitmap) -> usize {
        if Self::bitmap_serialize_as_raw_u32s(roaring) {
            roaring.len() as usize * size_of::<u32>()
        } else {
            roaring.serialized_size()
        }
    }

    pub fn serialize_into_vec(roaring: &RoaringBitmap, vec: &mut Vec<u8>) {
        Self::serialize_into_writer(roaring, vec).unwrap()
    }

    pub fn serialize_into_writer<W: io::Write>(
        roaring: &RoaringBitmap,
        mut writer: W,
    ) -> io::Result<()> {
        if Self::bitmap_serialize_as_raw_u32s(roaring) {
            for integer in roaring {
                writer.write_u32::<NativeEndian>(integer)?;
            }
        } else {
            // Otherwise, we use the classic RoaringBitmapCodec that writes a header.
            roaring.serialize_into(writer)?;
        }

        Ok(())
    }

    pub fn deserialize_from(mut bytes: &[u8]) -> io::Result<RoaringBitmap> {
        if Self::bytes_deserialize_as_raw_u32s(bytes) {
            // If there is threshold or less than threshold integers that can fit into this array
            // of bytes it means that we used the ByteOrder codec serializer.
            let mut bitmap = RoaringBitmap::new();
            while let Ok(integer) = bytes.read_u32::<NativeEndian>() {
                bitmap.insert(integer);
            }
            Ok(bitmap)
        } else {
            // Otherwise, it means we used the classic RoaringBitmapCodec and
            // that the header takes threshold integers.
            RoaringBitmap::deserialize_unchecked_from(bytes)
        }
    }

    pub fn intersection_with_serialized(
        mut bytes: &[u8],
        other: &RoaringBitmap,
    ) -> io::Result<RoaringBitmap> {
        // See above `deserialize_from` method for implementation details.
        if Self::bytes_deserialize_as_raw_u32s(bytes) {
            let mut bitmap = RoaringBitmap::new();
            while let Ok(integer) = bytes.read_u32::<NativeEndian>() {
                if other.contains(integer) {
                    bitmap.insert(integer);
                }
            }
            Ok(bitmap)
        } else {
            other.intersection_with_serialized_unchecked(Cursor::new(bytes))
        }
    }
}

impl heed::BytesDecode<'_> for CboRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::deserialize_from(bytes).map_err(Into::into)
    }
}

impl BytesDecodeOwned for CboRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::deserialize_from(bytes).map_err(Into::into)
    }
}

impl heed::BytesEncode<'_> for CboRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        let mut vec = Vec::with_capacity(Self::serialized_size(item));
        Self::serialize_into_vec(item, &mut vec);
        Ok(Cow::Owned(vec))
    }
}

use std::borrow::Cow;
use std::io;
use std::mem::size_of;

use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use roaring::RoaringBitmap;

/// This is the limit where using a byteorder became less size efficient
/// than using a direct roaring encoding, it is also the point where we are able
/// to determine the encoding used only by using the array of bytes length.
pub const THRESHOLD: usize = 7;

/// A conditionnal codec that either use the RoaringBitmap
/// or a lighter ByteOrder en/decoding method.
pub struct CboRoaringBitmapCodec;

impl CboRoaringBitmapCodec {
    pub fn serialized_size(roaring: &RoaringBitmap) -> usize {
        if roaring.len() <= THRESHOLD as u64 {
            roaring.len() as usize * size_of::<u32>()
        } else {
            roaring.serialized_size()
        }
    }

    pub fn serialize_into(roaring: &RoaringBitmap, vec: &mut Vec<u8>) {
        if roaring.len() <= THRESHOLD as u64 {
            // If the number of items (u32s) to encode is less than or equal to the threshold
            // it means that it would weigh the same or less than the RoaringBitmap
            // header, so we directly encode them using ByteOrder instead.
            for integer in roaring {
                vec.write_u32::<NativeEndian>(integer).unwrap();
            }
        } else {
            // Otherwise, we use the classic RoaringBitmapCodec that writes a header.
            roaring.serialize_into(vec).unwrap();
        }
    }

    pub fn deserialize_from(mut bytes: &[u8]) -> io::Result<RoaringBitmap> {
        if bytes.len() <= THRESHOLD * size_of::<u32>() {
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
        Self::serialize_into(item, &mut vec);
        Some(Cow::Owned(vec))
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use heed::{BytesDecode, BytesEncode};

    use super::*;

    #[test]
    fn verify_encoding_decoding() {
        let input = RoaringBitmap::from_iter(0..THRESHOLD as u32);
        let bytes = CboRoaringBitmapCodec::bytes_encode(&input).unwrap();
        let output = CboRoaringBitmapCodec::bytes_decode(&bytes).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn verify_threshold() {
        let input = RoaringBitmap::from_iter(0..THRESHOLD as u32);

        // use roaring bitmap
        let mut bytes = Vec::new();
        input.serialize_into(&mut bytes).unwrap();
        let roaring_size = bytes.len();

        // use byteorder directly
        let mut bytes = Vec::new();
        for integer in input {
            bytes.write_u32::<NativeEndian>(integer).unwrap();
        }
        let bo_size = bytes.len();

        assert!(roaring_size > bo_size);
    }
}

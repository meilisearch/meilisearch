use std::borrow::Cow;
use std::io::{self, Cursor};
use std::mem::size_of;

use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use heed::BoxedError;
use roaring::RoaringBitmap;

use crate::heed_codec::BytesDecodeOwned;
use crate::update::del_add::{DelAdd, KvReaderDelAdd};

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

    pub fn serialize_into_vec(roaring: &RoaringBitmap, vec: &mut Vec<u8>) {
        Self::serialize_into_writer(roaring, vec).unwrap()
    }

    pub fn serialize_into_writer<W: io::Write>(
        roaring: &RoaringBitmap,
        mut writer: W,
    ) -> io::Result<()> {
        if roaring.len() <= THRESHOLD as u64 {
            // If the number of items (u32s) to encode is less than or equal to the threshold
            // it means that it would weigh the same or less than the RoaringBitmap
            // header, so we directly encode them using ByteOrder instead.
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
            RoaringBitmap::deserialize_unchecked_from(bytes)
        }
    }

    pub fn intersection_with_serialized(
        mut bytes: &[u8],
        other: &RoaringBitmap,
    ) -> io::Result<RoaringBitmap> {
        // See above `deserialize_from` method for implementation details.
        if bytes.len() <= THRESHOLD * size_of::<u32>() {
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

    /// Merge serialized CboRoaringBitmaps in a buffer.
    ///
    /// if the merged values length is under the threshold, values are directly
    /// serialized in the buffer else a RoaringBitmap is created from the
    /// values and is serialized in the buffer.
    pub fn merge_into<I, A>(slices: I, buffer: &mut Vec<u8>) -> io::Result<()>
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>,
    {
        let mut roaring = RoaringBitmap::new();
        let mut vec = Vec::new();

        for bytes in slices {
            if bytes.as_ref().len() <= THRESHOLD * size_of::<u32>() {
                let mut reader = bytes.as_ref();
                while let Ok(integer) = reader.read_u32::<NativeEndian>() {
                    vec.push(integer);
                }
            } else {
                roaring |= RoaringBitmap::deserialize_unchecked_from(bytes.as_ref())?;
            }
        }

        if roaring.is_empty() {
            vec.sort_unstable();
            vec.dedup();

            if vec.len() <= THRESHOLD {
                for integer in vec {
                    buffer.extend_from_slice(&integer.to_ne_bytes());
                }
            } else {
                // We can unwrap safely because the vector is sorted upper.
                let roaring = RoaringBitmap::from_sorted_iter(vec).unwrap();
                roaring.serialize_into(buffer)?;
            }
        } else {
            roaring.extend(vec);
            roaring.serialize_into(buffer)?;
        }

        Ok(())
    }

    /// Merges a DelAdd delta into a CboRoaringBitmap.
    pub fn merge_deladd_into<'a>(
        deladd: &KvReaderDelAdd,
        previous: &[u8],
        buffer: &'a mut Vec<u8>,
    ) -> io::Result<Option<&'a [u8]>> {
        // Deserialize the bitmap that is already there
        let mut previous = Self::deserialize_from(previous)?;

        // Remove integers we no more want in the previous bitmap
        if let Some(value) = deladd.get(DelAdd::Deletion) {
            previous -= Self::deserialize_from(value)?;
        }

        // Insert the new integers we want in the previous bitmap
        if let Some(value) = deladd.get(DelAdd::Addition) {
            previous |= Self::deserialize_from(value)?;
        }

        if previous.is_empty() {
            return Ok(None);
        }

        Self::serialize_into_vec(&previous, buffer);
        Ok(Some(&buffer[..]))
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

    #[test]
    fn merge_cbo_roaring_bitmaps() {
        let mut buffer = Vec::new();

        let small_data = [
            RoaringBitmap::from_sorted_iter(1..4).unwrap(),
            RoaringBitmap::from_sorted_iter(2..5).unwrap(),
            RoaringBitmap::from_sorted_iter(4..6).unwrap(),
            RoaringBitmap::from_sorted_iter(1..3).unwrap(),
        ];

        let small_data: Vec<_> =
            small_data.iter().map(|b| CboRoaringBitmapCodec::bytes_encode(b).unwrap()).collect();
        CboRoaringBitmapCodec::merge_into(small_data.as_slice(), &mut buffer).unwrap();
        let bitmap = CboRoaringBitmapCodec::deserialize_from(&buffer).unwrap();
        let expected = RoaringBitmap::from_sorted_iter(1..6).unwrap();
        assert_eq!(bitmap, expected);

        let medium_data = [
            RoaringBitmap::from_sorted_iter(1..4).unwrap(),
            RoaringBitmap::from_sorted_iter(2..5).unwrap(),
            RoaringBitmap::from_sorted_iter(4..8).unwrap(),
            RoaringBitmap::from_sorted_iter(0..3).unwrap(),
            RoaringBitmap::from_sorted_iter(7..23).unwrap(),
        ];

        let medium_data: Vec<_> =
            medium_data.iter().map(|b| CboRoaringBitmapCodec::bytes_encode(b).unwrap()).collect();
        buffer.clear();
        CboRoaringBitmapCodec::merge_into(medium_data.as_slice(), &mut buffer).unwrap();

        let bitmap = CboRoaringBitmapCodec::deserialize_from(&buffer).unwrap();
        let expected = RoaringBitmap::from_sorted_iter(0..23).unwrap();
        assert_eq!(bitmap, expected);
    }
}

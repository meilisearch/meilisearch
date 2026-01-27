use std::borrow::Cow;
use std::io::{self, Cursor, ErrorKind};
use std::sync::OnceLock;

use byteorder::{NativeEndian, ReadBytesExt as _};
use heed::BoxedError;
use roaring::RoaringBitmap;

use super::cbo_roaring_bitmap_codec::CboRoaringBitmapCodec;
use super::de_roaring_bitmap_codec::DeRoaringBitmapCodec;
use crate::heed_codec::roaring_bitmap::take_all_blocks;
use crate::heed_codec::BytesDecodeOwned;
use crate::update::del_add::{DelAdd, KvReaderDelAdd};

/// Defines the status of the delta encoding on whether we have enabled it or not.
pub static DELTA_ENCODING_STATUS: DeltaEncodingStatusLock = DeltaEncodingStatusLock::new();

pub struct DeCboRoaringBitmapCodec;

impl DeCboRoaringBitmapCodec {
    pub fn serialized_size_with_tmp_buffer(
        bitmap: &RoaringBitmap,
        tmp_buffer: &mut Vec<u32>,
    ) -> usize {
        // We are stuck with this format because the CboRoaringBitmapCodec decides to write
        // raw and unencoded u32s, without a header when there is at most THRESHOLD elements.
        if CboRoaringBitmapCodec::bitmap_serialize_as_raw_u32s(bitmap)
            || DELTA_ENCODING_STATUS.is_disabled()
        {
            CboRoaringBitmapCodec::serialized_size(bitmap)
        } else {
            DeRoaringBitmapCodec::serialized_size_with_tmp_buffer(bitmap, tmp_buffer)
        }
    }

    /// Writes the delta-encoded compressed version of
    /// the given roaring bitmap into the provided writer.
    pub fn serialize_into<W: io::Write>(bitmap: &RoaringBitmap, writer: &mut W) -> io::Result<()> {
        let mut tmp_buffer = Vec::new();
        Self::serialize_into_with_tmp_buffer(bitmap, writer, &mut tmp_buffer)
    }

    /// Same as [Self::serialize_into] but accepts a buffer to avoid allocating one.
    ///
    /// Note that we always serialize the bitmap with the delta-encoded compressed version.
    pub fn serialize_into_with_tmp_buffer<W: io::Write>(
        bitmap: &RoaringBitmap,
        writer: &mut W,
        tmp_buffer: &mut Vec<u32>,
    ) -> io::Result<()> {
        // We are stuck with this format because the CboRoaringBitmapCodec decides to write
        // raw and unencoded u32s, without a header when there is at most THRESHOLD elements.
        if CboRoaringBitmapCodec::bitmap_serialize_as_raw_u32s(bitmap)
            || DELTA_ENCODING_STATUS.is_disabled()
        {
            CboRoaringBitmapCodec::serialize_into_writer(bitmap, writer)
        } else {
            DeRoaringBitmapCodec::serialize_into_with_tmp_buffer(bitmap, writer, tmp_buffer)
        }
    }

    /// Returns the delta-decoded roaring bitmap from the compressed bytes.
    pub fn deserialize_from(compressed: &[u8]) -> io::Result<RoaringBitmap> {
        let mut tmp_buffer = Vec::new();
        Self::deserialize_from_with_tmp_buffer(compressed, &mut tmp_buffer)
    }

    /// Same as [Self::deserialize_from] but accepts a buffer to avoid allocating one.
    ///
    /// It tries to decode the input by using the delta-decoded version and
    /// if it fails, falls back to the CboRoaringBitmap version.
    pub fn deserialize_from_with_tmp_buffer(
        input: &[u8],
        tmp_buffer: &mut Vec<u32>,
    ) -> io::Result<RoaringBitmap> {
        // The input is too short to be a valid delta-decoded bitmap.
        // We fall back to the CboRoaringBitmap version with raw u32s.
        if CboRoaringBitmapCodec::bytes_deserialize_as_raw_u32s(input) {
            return CboRoaringBitmapCodec::deserialize_from(input);
        }

        match DeRoaringBitmapCodec::deserialize_from_with_tmp_buffer(
            input,
            take_all_blocks,
            tmp_buffer,
        ) {
            Ok(bitmap) => Ok(bitmap),
            // If the error kind is Other it means that the delta-decoder found
            // an invalid magic header. We fall back to the CboRoaringBitmap version.
            Err(e) if e.kind() == ErrorKind::Other => {
                CboRoaringBitmapCodec::deserialize_from(input)
            }
            Err(e) => Err(e),
        }
    }

    /// Merge serialized DeCboRoaringBitmaps in a buffer.
    ///
    /// If the merged values length is under the threshold, values are directly
    /// serialized in the buffer else a delta-encoded list of integers is created
    /// from the values and is serialized in the buffer.
    pub fn merge_into<I, A>(slices: I, buffer: &mut Vec<u8>) -> io::Result<()>
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>,
    {
        let mut roaring = RoaringBitmap::new();
        let mut vec = Vec::new();
        let mut tmp_buffer = Vec::new();

        for bytes in slices {
            if CboRoaringBitmapCodec::bytes_deserialize_as_raw_u32s(bytes.as_ref()) {
                let mut reader = bytes.as_ref();
                while let Ok(integer) = reader.read_u32::<NativeEndian>() {
                    vec.push(integer);
                }
            } else {
                roaring |= DeCboRoaringBitmapCodec::deserialize_from_with_tmp_buffer(
                    bytes.as_ref(),
                    &mut tmp_buffer,
                )?;
            }
        }

        roaring.extend(vec);

        DeCboRoaringBitmapCodec::serialize_into_with_tmp_buffer(&roaring, buffer, &mut tmp_buffer)?;

        Ok(())
    }

    /// Do an intersection directly with a serialized delta-encoded bitmap.
    ///
    /// When doing the intersection we only need to deserialize the necessary
    /// bitmap containers and avoid a lot of unnecessary allocations. We do
    /// that by skipping entire delta-encoded blocks when possible to avoid
    /// storing them in the bitmap we use for the final intersection.
    pub fn intersection_with_serialized(
        bytes: &[u8],
        other: &RoaringBitmap,
    ) -> io::Result<RoaringBitmap> {
        if CboRoaringBitmapCodec::bytes_deserialize_as_raw_u32s(bytes) {
            return CboRoaringBitmapCodec::intersection_with_serialized(bytes, other);
        }

        // TODO move this tmp buffer outside
        let mut tmp_buffer = Vec::new();
        let filter_block = |first, last| other.range_cardinality(first..=last) == 0;

        match DeRoaringBitmapCodec::deserialize_from_with_tmp_buffer(
            bytes,
            filter_block,
            &mut tmp_buffer,
        ) {
            Ok(bitmap) => Ok(bitmap & other),
            // If the error kind is Other it means that the delta-decoder found
            // an invalid magic header. We fall back to the CboRoaringBitmap version.
            Err(e) if e.kind() == ErrorKind::Other => {
                other.intersection_with_serialized_unchecked(Cursor::new(bytes))
            }
            Err(e) => Err(e),
        }
    }

    pub fn merge_deladd_into<'a>(
        deladd: &KvReaderDelAdd,
        previous: &[u8],
        buffer: &'a mut Vec<u8>,
        tmp_buffer: &mut Vec<u32>,
    ) -> io::Result<Option<&'a [u8]>> {
        // Deserialize the bitmap that is already there
        let mut previous = Self::deserialize_from_with_tmp_buffer(previous, tmp_buffer)?;

        // Remove integers we no more want in the previous bitmap
        if let Some(value) = deladd.get(DelAdd::Deletion) {
            previous -= Self::deserialize_from_with_tmp_buffer(value, tmp_buffer)?;
        }

        // Insert the new integers we want in the previous bitmap
        if let Some(value) = deladd.get(DelAdd::Addition) {
            previous |= Self::deserialize_from_with_tmp_buffer(value, tmp_buffer)?;
        }

        if previous.is_empty() {
            return Ok(None);
        }

        Self::serialize_into_with_tmp_buffer(&previous, buffer, tmp_buffer)?;

        Ok(Some(&buffer[..]))
    }
}

impl heed::BytesDecode<'_> for DeCboRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::deserialize_from(bytes).map_err(Into::into)
    }
}

impl BytesDecodeOwned for DeCboRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::deserialize_from(bytes).map_err(Into::into)
    }
}

impl heed::BytesEncode<'_> for DeCboRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        let mut tmp_buffer = Vec::new();
        let capacity = Self::serialized_size_with_tmp_buffer(item, &mut tmp_buffer);
        let mut output = Vec::with_capacity(capacity);
        Self::serialize_into_with_tmp_buffer(item, &mut output, &mut tmp_buffer)?;
        Ok(Cow::Owned(output))
    }
}

/// Manages the global status of the delta encoding.
///
/// Whether we must use delta encoding or not when encoding roaring bitmaps.
#[derive(Default)]
pub struct DeltaEncodingStatusLock(OnceLock<DeltaEncodingStatus>);

impl DeltaEncodingStatusLock {
    pub const fn new() -> Self {
        Self(OnceLock::new())
    }
}

#[derive(Default)]
enum DeltaEncodingStatus {
    Enabled,
    #[default]
    Disabled,
}

impl DeltaEncodingStatusLock {
    pub fn set_to_enabled(&self) -> Result<(), ()> {
        self.0.set(DeltaEncodingStatus::Enabled).map_err(drop)
    }

    pub fn set_to_disabled(&self) -> Result<(), ()> {
        self.0.set(DeltaEncodingStatus::Disabled).map_err(drop)
    }

    pub fn is_enabled(&self) -> bool {
        matches!(self.0.get(), Some(DeltaEncodingStatus::Enabled))
    }

    pub fn is_disabled(&self) -> bool {
        !self.is_enabled()
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use byteorder::WriteBytesExt as _;
    use heed::{BytesDecode, BytesEncode};
    use quickcheck::quickcheck;
    use roaring::RoaringBitmap;

    use super::super::super::roaring_bitmap_length::DeCboRoaringBitmapLenCodec;
    use super::super::THRESHOLD;
    use super::*;

    #[test]
    fn verify_encoding_decoding() {
        let input = RoaringBitmap::from_iter(0..THRESHOLD as u32);
        let bytes = DeCboRoaringBitmapCodec::bytes_encode(&input).unwrap();
        let output = DeCboRoaringBitmapCodec::bytes_decode(&bytes).unwrap();
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
    fn merge_de_cbo_roaring_bitmaps() {
        let mut buffer = Vec::new();

        let small_data = [
            RoaringBitmap::from_sorted_iter(1..4).unwrap(),
            RoaringBitmap::from_sorted_iter(2..5).unwrap(),
            RoaringBitmap::from_sorted_iter(4..6).unwrap(),
            RoaringBitmap::from_sorted_iter(1..3).unwrap(),
        ];

        let small_data: Vec<_> =
            small_data.iter().map(|b| DeCboRoaringBitmapCodec::bytes_encode(b).unwrap()).collect();
        DeCboRoaringBitmapCodec::merge_into(small_data.as_slice(), &mut buffer).unwrap();
        let bitmap = DeCboRoaringBitmapCodec::deserialize_from(&buffer).unwrap();
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
            medium_data.iter().map(|b| DeCboRoaringBitmapCodec::bytes_encode(b).unwrap()).collect();
        buffer.clear();
        DeCboRoaringBitmapCodec::merge_into(medium_data.as_slice(), &mut buffer).unwrap();

        let bitmap = DeCboRoaringBitmapCodec::deserialize_from(&buffer).unwrap();
        let expected = RoaringBitmap::from_sorted_iter(0..23).unwrap();
        assert_eq!(bitmap, expected);
    }

    quickcheck! {
        fn qc_random(xs: Vec<u32>) -> bool {
            let bitmap = RoaringBitmap::from_iter(xs);
            let mut compressed = Vec::new();
            let mut tmp_buffer = Vec::new();
            DeCboRoaringBitmapCodec::serialize_into_with_tmp_buffer(&bitmap, &mut compressed, &mut tmp_buffer).unwrap();
            let length = DeCboRoaringBitmapLenCodec::bytes_decode(&compressed[..]).unwrap();
            let decompressed = DeCboRoaringBitmapCodec::deserialize_from_with_tmp_buffer(&compressed[..], &mut tmp_buffer).unwrap();
            length == bitmap.len() && decompressed == bitmap
        }
    }

    quickcheck! {
        fn qc_random_check_serialized_size(xs: Vec<u32>) -> bool {
            let bitmap = RoaringBitmap::from_iter(xs);
            let mut compressed = Vec::new();
            let mut tmp_buffer = Vec::new();
            DeCboRoaringBitmapCodec::serialize_into_with_tmp_buffer(&bitmap, &mut compressed, &mut tmp_buffer).unwrap();
            let length = DeCboRoaringBitmapLenCodec::bytes_decode(&compressed).unwrap();
            let expected_len = DeCboRoaringBitmapCodec::serialized_size_with_tmp_buffer(&bitmap, &mut tmp_buffer);
            length == bitmap.len() && compressed.len() == expected_len
        }
    }

    quickcheck! {
        fn qc_random_intersection_with_serialized(lhs: Vec<u32>, rhs: Vec<u32>) -> bool {
            let mut compressed = Vec::new();
            let mut tmp_buffer = Vec::new();

            let lhs = RoaringBitmap::from_iter(lhs);
            let rhs = RoaringBitmap::from_iter(rhs);
            DeCboRoaringBitmapCodec::serialize_into_with_tmp_buffer(&lhs, &mut compressed, &mut tmp_buffer).unwrap();

            let intersection = DeCboRoaringBitmapCodec::intersection_with_serialized(&compressed, &rhs).unwrap();
            let expected_intersection = lhs & rhs;

            intersection == expected_intersection
        }
    }
}

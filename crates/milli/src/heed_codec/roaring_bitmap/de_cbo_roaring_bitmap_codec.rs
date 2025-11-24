use std::borrow::Cow;
use std::io::{self, ErrorKind};

use heed::BoxedError;
use roaring::RoaringBitmap;

use super::cbo_roaring_bitmap_codec::CboRoaringBitmapCodec;
use super::de_roaring_bitmap_codec::DeRoaringBitmapCodec;
use crate::heed_codec::BytesDecodeOwned;

pub struct DeCboRoaringBitmapCodec;

impl DeCboRoaringBitmapCodec {
    pub fn serialized_size_with_tmp_buffer(
        bitmap: &RoaringBitmap,
        tmp_buffer: &mut Vec<u32>,
    ) -> usize {
        DeRoaringBitmapCodec::serialized_size_with_tmp_buffer(bitmap, tmp_buffer)
    }

    /// Writes the delta-encoded compressed version of
    /// the given roaring bitmap into the provided writer.
    pub fn serialize_into<W: io::Write>(bitmap: &RoaringBitmap, writer: W) -> io::Result<()> {
        let mut tmp_buffer = Vec::new();
        Self::serialize_into_with_tmp_buffer(bitmap, writer, &mut tmp_buffer)
    }

    /// Same as [Self::serialize_into] but accepts a buffer to avoid allocating one.
    ///
    /// Note that we always serialize the bitmap with the delta-encoded compressed version.
    pub fn serialize_into_with_tmp_buffer<W: io::Write>(
        bitmap: &RoaringBitmap,
        writer: W,
        tmp_buffer: &mut Vec<u32>,
    ) -> io::Result<()> {
        // We are stuck with this format because the CboRoaringBitmapCodec decides to write
        // raw and unencoded u32s, without a header when there is at most THRESHOLD elements.
        if CboRoaringBitmapCodec::bitmap_serialize_as_raw_u32s(bitmap) {
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

        match DeRoaringBitmapCodec::deserialize_from_with_tmp_buffer(input, tmp_buffer) {
            Ok(bitmap) => Ok(bitmap),
            // If the error kind is Other it means that the delta-decoder found
            // an invalid magic header. We fall back to the CboRoaringBitmap version.
            Err(e) if e.kind() == ErrorKind::Other => {
                CboRoaringBitmapCodec::deserialize_from(input)
            }
            Err(e) => Err(e),
        }
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
        // let mut vec = Vec::with_capacity(Self::serialized_size(item));
        // Self::serialize_into_vec(item, &mut vec);
        // Ok(Cow::Owned(vec))
        todo!()
    }
}

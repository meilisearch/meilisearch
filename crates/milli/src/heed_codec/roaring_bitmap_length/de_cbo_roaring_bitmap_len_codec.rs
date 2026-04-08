use std::io::ErrorKind;

use heed::{BoxedError, BytesDecode};

use super::BoRoaringBitmapLenCodec;
use crate::heed_codec::roaring_bitmap::cbo_roaring_bitmap_codec::CboRoaringBitmapCodec;
use crate::heed_codec::roaring_bitmap::de_roaring_bitmap_codec::DeRoaringBitmapCodec;
use crate::heed_codec::roaring_bitmap_length::CboRoaringBitmapLenCodec;
use crate::heed_codec::BytesDecodeOwned;

pub struct DeCboRoaringBitmapLenCodec;

impl BytesDecode<'_> for DeCboRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        if CboRoaringBitmapCodec::bytes_deserialize_as_raw_u32s(bytes) {
            // If there is threshold or less than threshold integers that can fit
            // into this array of bytes it means that we used the ByteOrder codec
            // serializer.
            BoRoaringBitmapLenCodec::bytes_decode(bytes)
        } else {
            match DeRoaringBitmapCodec::deserialize_length_from(bytes) {
                Ok(bitmap) => Ok(bitmap),
                // If the error kind is Other it means that the delta-decoder found
                // an invalid magic header. We fall back to the CboRoaringBitmap version.
                Err(e) if e.kind() == ErrorKind::Other => {
                    CboRoaringBitmapLenCodec::bytes_decode(bytes)
                }
                Err(e) => Err(e.into()),
            }
        }
    }
}

impl BytesDecodeOwned for DeCboRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::bytes_decode(bytes)
    }
}

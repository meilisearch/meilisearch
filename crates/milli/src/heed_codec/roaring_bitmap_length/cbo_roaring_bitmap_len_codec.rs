use heed::{BoxedError, BytesDecode};

use super::RoaringBitmapLenCodec;
use crate::heed_codec::BytesDecodeOwned;

pub struct CboRoaringBitmapLenCodec;

impl BytesDecode<'_> for CboRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        // Otherwise, it means we used the classic RoaringBitmapCodec and
        // that the header takes threshold integers.
        RoaringBitmapLenCodec::bytes_decode(bytes)
    }
}

impl BytesDecodeOwned for CboRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::bytes_decode(bytes)
    }
}

use std::borrow::Cow;

use heed::{BytesDecode, BytesEncode};

/// A codec for values of type `&[u8]`. Unlike `ByteSlice`, its `EItem` and `DItem` associated
/// types are equivalent (= `&'a [u8]`) and these values can reside within another structure.
pub struct ByteSliceRefCodec;

impl<'a> BytesEncode<'a> for ByteSliceRefCodec {
    type EItem = &'a [u8];

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        Some(Cow::Borrowed(item))
    }
}

impl<'a> BytesDecode<'a> for ByteSliceRefCodec {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        Some(bytes)
    }
}

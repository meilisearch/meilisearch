use std::borrow::Cow;

use heed::{BoxedError, BytesDecode, BytesEncode};

/// A codec for values of type `&str`. Unlike `Str`, its `EItem` and `DItem` associated
/// types are equivalent (= `&'a str`) and these values can reside within another structure.
pub struct StrRefCodec;
impl<'a> BytesEncode<'a> for StrRefCodec {
    type EItem = &'a str;

    fn bytes_encode(item: &'a &'a str) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(item.as_bytes()))
    }
}
impl<'a> BytesDecode<'a> for StrRefCodec {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        std::str::from_utf8(bytes).map_err(Into::into)
    }
}

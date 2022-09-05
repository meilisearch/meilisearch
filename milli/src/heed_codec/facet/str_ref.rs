use std::borrow::Cow;

use heed::{BytesDecode, BytesEncode};

pub struct StrRefCodec;
impl<'a> BytesEncode<'a> for StrRefCodec {
    type EItem = &'a str;

    fn bytes_encode(item: &'a &'a str) -> Option<Cow<'a, [u8]>> {
        Some(Cow::Borrowed(item.as_bytes()))
    }
}
impl<'a> BytesDecode<'a> for StrRefCodec {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let s = std::str::from_utf8(bytes).unwrap();
        Some(s)
    }
}

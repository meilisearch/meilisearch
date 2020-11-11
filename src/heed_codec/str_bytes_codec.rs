use std::borrow::Cow;
use std::str;

pub struct StrBytesCodec;

impl<'a> heed::BytesDecode<'a> for StrBytesCodec {
    type DItem = (&'a str, &'a [u8]);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let s1_end = bytes.iter().position(|b| *b == 0)?;
        let (s1_bytes, s2_bytes) = bytes.split_at(s1_end);
        let s1 = str::from_utf8(s1_bytes).ok()?;
        let s2 = &s2_bytes[1..];
        Some((s1, s2))
    }
}

impl<'a> heed::BytesEncode<'a> for StrBytesCodec {
    type EItem = (&'a str, &'a [u8]);

    fn bytes_encode((s1, s2): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(s1.len() + s2.len() + 1);
        bytes.extend_from_slice(s1.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(s2);
        Some(Cow::Owned(bytes))
    }
}

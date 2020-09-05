use std::borrow::Cow;
use std::convert::TryInto;
use std::str;

pub struct StrBEU32Codec;

impl<'a> heed::BytesDecode<'a> for StrBEU32Codec {
    type DItem = (&'a str, u32);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (str_bytes, n_bytes) = bytes.split_at(bytes.len() - 4);
        let s = str::from_utf8(str_bytes).ok()?;
        let n = n_bytes.try_into().map(u32::from_be_bytes).ok()?;
        Some((s, n))
    }
}

impl<'a> heed::BytesEncode<'a> for StrBEU32Codec {
    type EItem = (&'a str, u32);

    fn bytes_encode((s, n): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(s.len() + 4);
        bytes.extend_from_slice(s.as_bytes());
        bytes.extend_from_slice(&n.to_be_bytes());
        Some(Cow::Owned(bytes))
    }
}

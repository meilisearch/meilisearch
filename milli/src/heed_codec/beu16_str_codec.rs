use std::borrow::Cow;
use std::convert::TryInto;
use std::str;

pub struct BEU16StrCodec;

impl<'a> heed::BytesDecode<'a> for BEU16StrCodec {
    type DItem = (u16, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (n_bytes, str_bytes) = bytes.split_at(2);
        let n = n_bytes.try_into().map(u16::from_be_bytes).ok()?;
        let s = str::from_utf8(str_bytes).ok()?;
        Some((n, s))
    }
}

impl<'a> heed::BytesEncode<'a> for BEU16StrCodec {
    type EItem = (u16, &'a str);

    fn bytes_encode((n, s): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(s.len() + 2);
        bytes.extend_from_slice(&n.to_be_bytes());
        bytes.extend_from_slice(s.as_bytes());
        Some(Cow::Owned(bytes))
    }
}

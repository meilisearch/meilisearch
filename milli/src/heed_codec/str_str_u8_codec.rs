use std::borrow::Cow;
use std::str;

use heed::BoxedError;

pub struct U8StrStrCodec;

impl<'a> heed::BytesDecode<'a> for U8StrStrCodec {
    type DItem = (u8, &'a str, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        let (n, bytes) = bytes.split_first().ok_or("not enough bytes").map_err(BoxedError::from)?;
        let s1_end = bytes
            .iter()
            .position(|b| *b == 0)
            .ok_or("cannot find nul byte")
            .map_err(BoxedError::from)?;
        let (s1_bytes, rest) = bytes.split_at(s1_end);
        let s2_bytes = &rest[1..];
        let s1 = str::from_utf8(s1_bytes)?;
        let s2 = str::from_utf8(s2_bytes)?;
        Ok((*n, s1, s2))
    }
}

impl<'a> heed::BytesEncode<'a> for U8StrStrCodec {
    type EItem = (u8, &'a str, &'a str);

    fn bytes_encode((n, s1, s2): &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        let mut bytes = Vec::with_capacity(s1.len() + s2.len() + 1);
        bytes.push(*n);
        bytes.extend_from_slice(s1.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(s2.as_bytes());
        Ok(Cow::Owned(bytes))
    }
}
pub struct UncheckedU8StrStrCodec;

impl<'a> heed::BytesDecode<'a> for UncheckedU8StrStrCodec {
    type DItem = (u8, &'a [u8], &'a [u8]);

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        let (n, bytes) = bytes.split_first().ok_or("not enough bytes").map_err(BoxedError::from)?;
        let s1_end = bytes
            .iter()
            .position(|b| *b == 0)
            .ok_or("cannot find nul byte")
            .map_err(BoxedError::from)?;
        let (s1_bytes, rest) = bytes.split_at(s1_end);
        let s2_bytes = &rest[1..];
        Ok((*n, s1_bytes, s2_bytes))
    }
}

impl<'a> heed::BytesEncode<'a> for UncheckedU8StrStrCodec {
    type EItem = (u8, &'a [u8], &'a [u8]);

    fn bytes_encode((n, s1, s2): &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        let mut bytes = Vec::with_capacity(s1.len() + s2.len() + 1);
        bytes.push(*n);
        bytes.extend_from_slice(s1);
        bytes.push(0);
        bytes.extend_from_slice(s2);
        Ok(Cow::Owned(bytes))
    }
}

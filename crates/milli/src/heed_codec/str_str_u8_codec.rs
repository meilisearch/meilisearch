use std::borrow::Cow;
use std::ffi::CStr;
use std::str;

use heed::BoxedError;

use super::SliceTooShortError;

pub struct U8StrStrCodec;

impl<'a> heed::BytesDecode<'a> for U8StrStrCodec {
    type DItem = (u8, &'a str, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        let (n, bytes) = bytes.split_first().ok_or(SliceTooShortError)?;
        let cstr = CStr::from_bytes_until_nul(bytes)?;
        let s1 = cstr.to_str()?;
        // skip '\0' byte between the two strings.
        let s2 = str::from_utf8(&bytes[s1.len() + 1..])?;
        Ok((*n, s1, s2))
    }
}

impl<'a> heed::BytesEncode<'a> for U8StrStrCodec {
    type EItem = (u8, &'a str, &'a str);

    fn bytes_encode((n, s1, s2): &Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
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
        let (n, bytes) = bytes.split_first().ok_or(SliceTooShortError)?;
        let cstr = CStr::from_bytes_until_nul(bytes)?;
        let s1_bytes = cstr.to_bytes();
        // skip '\0' byte between the two strings.
        let s2_bytes = &bytes[s1_bytes.len() + 1..];
        Ok((*n, s1_bytes, s2_bytes))
    }
}

impl<'a> heed::BytesEncode<'a> for UncheckedU8StrStrCodec {
    type EItem = (u8, &'a [u8], &'a [u8]);

    fn bytes_encode((n, s1, s2): &Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let mut bytes = Vec::with_capacity(s1.len() + s2.len() + 1);
        bytes.push(*n);
        bytes.extend_from_slice(s1);
        bytes.push(0);
        bytes.extend_from_slice(s2);
        Ok(Cow::Owned(bytes))
    }
}

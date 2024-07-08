use std::borrow::Cow;
use std::convert::TryInto;
use std::str;

use heed::BoxedError;

pub struct BEU32StrCodec;

impl<'a> heed::BytesDecode<'a> for BEU32StrCodec {
    type DItem = (u32, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        let (n_bytes, str_bytes) = bytes.split_at(4);
        let n = n_bytes.try_into().map(u32::from_be_bytes)?;
        let s = str::from_utf8(str_bytes)?;
        Ok((n, s))
    }
}

impl<'a> heed::BytesEncode<'a> for BEU32StrCodec {
    type EItem = (u32, &'a str);

    fn bytes_encode((n, s): &Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let mut bytes = Vec::with_capacity(s.len() + 4);
        bytes.extend_from_slice(&n.to_be_bytes());
        bytes.extend_from_slice(s.as_bytes());
        Ok(Cow::Owned(bytes))
    }
}

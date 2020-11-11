use std::borrow::Cow;
use std::str;

use crate::heed_codec::StrBytesCodec;

pub struct FacetValueStringCodec;

impl<'a> heed::BytesDecode<'a> for FacetValueStringCodec {
    type DItem = (&'a str, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (name, bytes) = StrBytesCodec::bytes_decode(bytes)?;
        let value = str::from_utf8(bytes).ok()?;
        Some((name, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FacetValueStringCodec {
    type EItem = (&'a str, &'a str);

    fn bytes_encode((name, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let tuple = (*name, value.as_bytes());
        StrBytesCodec::bytes_encode(&tuple).map(Cow::into_owned).map(Cow::Owned)
    }
}

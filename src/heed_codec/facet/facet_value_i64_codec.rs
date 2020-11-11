use std::borrow::Cow;
use std::convert::TryInto;
use std::str;

use crate::heed_codec::StrBytesCodec;
use crate::facet::value_encoding::{i64_from_bytes, i64_into_bytes};

pub struct FacetValueI64Codec;

impl<'a> heed::BytesDecode<'a> for FacetValueI64Codec {
    type DItem = (&'a str, i64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (name, bytes) = StrBytesCodec::bytes_decode(bytes)?;
        let value = bytes.try_into().map(i64_from_bytes).ok()?;
        Some((name, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FacetValueI64Codec {
    type EItem = (&'a str, i64);

    fn bytes_encode((name, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let value = i64_into_bytes(*value);
        let tuple = (*name, &value[..]);
        StrBytesCodec::bytes_encode(&tuple).map(Cow::into_owned).map(Cow::Owned)
    }
}

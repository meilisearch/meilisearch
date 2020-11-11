use std::borrow::Cow;
use std::convert::TryInto;
use std::str;

use crate::heed_codec::StrBytesCodec;
use crate::facet::value_encoding::{u64_from_bytes, u64_into_bytes};

pub struct FacetValueU64Codec;

impl<'a> heed::BytesDecode<'a> for FacetValueU64Codec {
    type DItem = (&'a str, u64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (name, bytes) = StrBytesCodec::bytes_decode(bytes)?;
        let value = bytes.try_into().map(u64_from_bytes).ok()?;
        Some((name, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FacetValueU64Codec {
    type EItem = (&'a str, u64);

    fn bytes_encode((name, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let value = u64_into_bytes(*value);
        let tuple = (*name, &value[..]);
        StrBytesCodec::bytes_encode(&tuple).map(Cow::into_owned).map(Cow::Owned)
    }
}

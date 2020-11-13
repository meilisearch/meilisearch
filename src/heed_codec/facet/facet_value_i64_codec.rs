use std::borrow::Cow;
use std::convert::TryInto;

use crate::facet::value_encoding::{i64_from_bytes, i64_into_bytes};

pub struct FacetValueI64Codec;

impl<'a> heed::BytesDecode<'a> for FacetValueI64Codec {
    type DItem = (u8, i64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, buffer) = bytes.split_first()?;
        let value = buffer.try_into().map(i64_from_bytes).ok()?;
        Some((*field_id, value))
    }
}

impl heed::BytesEncode<'_> for FacetValueI64Codec {
    type EItem = (u8, i64);

    fn bytes_encode((field_id, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let value = i64_into_bytes(*value);
        let mut bytes = Vec::with_capacity(value.len() + 1);
        bytes.push(*field_id);
        bytes.extend_from_slice(&value[..]);
        Some(Cow::Owned(bytes))
    }
}

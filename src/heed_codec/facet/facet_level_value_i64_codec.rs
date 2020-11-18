use std::borrow::Cow;
use std::convert::TryInto;

use crate::facet::value_encoding::{i64_from_bytes, i64_into_bytes};

pub struct FacetLevelValueI64Codec;

impl<'a> heed::BytesDecode<'a> for FacetLevelValueI64Codec {
    type DItem = (u8, u8, i64, i64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, bytes) = bytes.split_first()?;
        let (level, bytes) = bytes.split_first()?;

        let left = bytes[..8].try_into().map(i64_from_bytes).ok()?;
        let right = if *level != 0 {
            bytes[8..].try_into().map(i64_from_bytes).ok()?
        } else {
            left
        };

        Some((*field_id, *level, left, right))
    }
}

impl heed::BytesEncode<'_> for FacetLevelValueI64Codec {
    type EItem = (u8, u8, i64, i64);

    fn bytes_encode((field_id, level, left, right): &Self::EItem) -> Option<Cow<[u8]>> {
        let left = i64_into_bytes(*left);
        let right = i64_into_bytes(*right);

        let mut bytes = Vec::with_capacity(2 + left.len() + right.len());
        bytes.push(*field_id);
        bytes.push(*level);
        bytes.extend_from_slice(&left[..]);
        if *level != 0 {
            bytes.extend_from_slice(&right[..]);
        }

        Some(Cow::Owned(bytes))
    }
}

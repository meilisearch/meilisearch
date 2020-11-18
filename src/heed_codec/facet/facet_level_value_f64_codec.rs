use std::borrow::Cow;
use std::convert::TryInto;

use crate::facet::value_encoding::f64_into_bytes;

// TODO do not de/serialize right bound when level = 0
pub struct FacetLevelValueF64Codec;

impl<'a> heed::BytesDecode<'a> for FacetLevelValueF64Codec {
    type DItem = (u8, u8, f64, f64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, bytes) = bytes.split_first()?;
        let (level, bytes) = bytes.split_first()?;

        let left = bytes[16..24].try_into().ok().map(f64::from_be_bytes)?;
        let right = bytes[24..].try_into().ok().map(f64::from_be_bytes)?;

        Some((*field_id, *level, left, right))
    }
}

impl heed::BytesEncode<'_> for FacetLevelValueF64Codec {
    type EItem = (u8, u8, f64, f64);

    fn bytes_encode((field_id, level, left, right): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut buffer = [0u8; 32];

        // Write the globally ordered floats.
        let bytes = f64_into_bytes(*left)?;
        buffer[..8].copy_from_slice(&bytes[..]);

        let bytes = f64_into_bytes(*right)?;
        buffer[8..16].copy_from_slice(&bytes[..]);

        // Then the f64 values just to be able to read them back.
        let bytes = left.to_be_bytes();
        buffer[16..24].copy_from_slice(&bytes[..]);

        let bytes = right.to_be_bytes();
        buffer[24..].copy_from_slice(&bytes[..]);

        let mut bytes = Vec::with_capacity(buffer.len() + 2);
        bytes.push(*field_id);
        bytes.push(*level);
        bytes.extend_from_slice(&buffer[..]);
        Some(Cow::Owned(bytes))
    }
}

#[cfg(test)]
mod tests {
    use heed::{BytesEncode, BytesDecode};
    use super::*;

    #[test]
    fn globally_ordered_f64() {
        let bytes = FacetLevelValueF64Codec::bytes_encode(&(3, 0, -32.0, 32.0)).unwrap();
        let (name, level, left, right) = FacetLevelValueF64Codec::bytes_decode(&bytes).unwrap();
        assert_eq!((name, level, left, right), (3, 0, -32.0, 32.0));
    }
}

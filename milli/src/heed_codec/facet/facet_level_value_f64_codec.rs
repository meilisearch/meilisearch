use std::borrow::Cow;
use std::convert::TryInto;

use crate::facet::value_encoding::f64_into_bytes;
use crate::{try_split_array_at, FieldId};

// TODO do not de/serialize right bound when level = 0
pub struct FacetLevelValueF64Codec;

impl<'a> heed::BytesDecode<'a> for FacetLevelValueF64Codec {
    type DItem = (FieldId, u8, f64, f64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id_bytes, bytes) = try_split_array_at(bytes)?;
        let field_id = u16::from_be_bytes(field_id_bytes);
        let (level, bytes) = bytes.split_first()?;

        let (left, right) = if *level != 0 {
            let left = bytes[16..24].try_into().ok().map(f64::from_be_bytes)?;
            let right = bytes[24..].try_into().ok().map(f64::from_be_bytes)?;
            (left, right)
        } else {
            let left = bytes[8..].try_into().ok().map(f64::from_be_bytes)?;
            (left, left)
        };

        Some((field_id, *level, left, right))
    }
}

impl heed::BytesEncode<'_> for FacetLevelValueF64Codec {
    type EItem = (FieldId, u8, f64, f64);

    fn bytes_encode((field_id, level, left, right): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut buffer = [0u8; 32];

        let len = if *level != 0 {
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

            32 // length
        } else {
            // Write the globally ordered floats.
            let bytes = f64_into_bytes(*left)?;
            buffer[..8].copy_from_slice(&bytes[..]);

            // Then the f64 values just to be able to read them back.
            let bytes = left.to_be_bytes();
            buffer[8..16].copy_from_slice(&bytes[..]);

            16 // length
        };

        let mut bytes = Vec::with_capacity(len + 3);
        bytes.extend_from_slice(&field_id.to_be_bytes());
        bytes.push(*level);
        bytes.extend_from_slice(&buffer[..len]);
        Some(Cow::Owned(bytes))
    }
}

#[cfg(test)]
mod tests {
    use heed::{BytesDecode, BytesEncode};

    use super::*;

    #[test]
    fn globally_ordered_f64() {
        let bytes = FacetLevelValueF64Codec::bytes_encode(&(3, 0, 32.0, 0.0)).unwrap();
        let (name, level, left, right) = FacetLevelValueF64Codec::bytes_decode(&bytes).unwrap();
        assert_eq!((name, level, left, right), (3, 0, 32.0, 32.0));

        let bytes = FacetLevelValueF64Codec::bytes_encode(&(3, 1, -32.0, 32.0)).unwrap();
        let (name, level, left, right) = FacetLevelValueF64Codec::bytes_decode(&bytes).unwrap();
        assert_eq!((name, level, left, right), (3, 1, -32.0, 32.0));
    }
}

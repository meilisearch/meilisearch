use std::borrow::Cow;
use std::convert::TryInto;

use crate::facet::value_encoding::f64_into_bytes;

pub struct FacetValueF64Codec;

impl<'a> heed::BytesDecode<'a> for FacetValueF64Codec {
    type DItem = (u8, f64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, buffer) = bytes.split_first()?;
        let value = buffer[8..].try_into().ok().map(f64::from_be_bytes)?;
        Some((*field_id, value))
    }
}

impl heed::BytesEncode<'_> for FacetValueF64Codec {
    type EItem = (u8, f64);

    fn bytes_encode((field_id, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut buffer = [0u8; 16];

        // Write the globally ordered float.
        let bytes = f64_into_bytes(*value)?;
        buffer[..8].copy_from_slice(&bytes[..]);

        // Then the f64 value just to be able to read it back.
        let bytes = value.to_be_bytes();
        buffer[8..].copy_from_slice(&bytes[..]);

        let mut bytes = Vec::with_capacity(buffer.len() + 1);
        bytes.push(*field_id);
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
        let bytes = FacetValueF64Codec::bytes_encode(&(3, -32.0)).unwrap();
        let (name, value) = FacetValueF64Codec::bytes_decode(&bytes).unwrap();
        assert_eq!((name, value), (3, -32.0));
    }
}

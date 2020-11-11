use std::borrow::Cow;
use std::convert::TryInto;
use std::str;

use crate::heed_codec::StrBytesCodec;
use crate::facet::value_encoding::f64_into_bytes;

pub struct FacetValueF64Codec;

impl<'a> heed::BytesDecode<'a> for FacetValueF64Codec {
    type DItem = (&'a str, f64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (name, buffer) = StrBytesCodec::bytes_decode(bytes)?;
        let value = buffer[8..].try_into().ok().map(f64::from_be_bytes)?;
        Some((name, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FacetValueF64Codec {
    type EItem = (&'a str, f64);

    fn bytes_encode((name, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut buffer = [0u8; 16];

        // Write the globally ordered float.
        let bytes = f64_into_bytes(*value)?;
        buffer[..8].copy_from_slice(&bytes[..]);

        // Then the f64 value just to be able to read it back.
        let bytes = value.to_be_bytes();
        buffer[8..].copy_from_slice(&bytes[..]);

        let tuple = (*name, &buffer[..]);
        StrBytesCodec::bytes_encode(&tuple).map(Cow::into_owned).map(Cow::Owned)
    }
}

#[cfg(test)]
mod tests {
    use heed::{BytesEncode, BytesDecode};
    use super::*;

    #[test]
    fn globally_ordered_f64() {
        let bytes = FacetValueF64Codec::bytes_encode(&("hello", -32.0)).unwrap();
        let (name, value) = FacetValueF64Codec::bytes_decode(&bytes).unwrap();
        assert_eq!((name, value), ("hello", -32.0));
    }
}

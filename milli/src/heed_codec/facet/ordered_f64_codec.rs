use std::borrow::Cow;
use std::convert::TryInto;

use heed::{BoxedError, BytesDecode};

use crate::facet::value_encoding::f64_into_bytes;

pub struct OrderedF64Codec;

impl<'a> BytesDecode<'a> for OrderedF64Codec {
    type DItem = f64;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        if bytes.len() < 16 {
            panic!() // TODO don't panic
        }
        let f = bytes[8..].try_into().ok().map(f64::from_be_bytes).unwrap();
        Ok(f)
    }
}

impl heed::BytesEncode<'_> for OrderedF64Codec {
    type EItem = f64;

    fn bytes_encode(f: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        let mut buffer = [0u8; 16];

        // write the globally ordered float
        let bytes = f64_into_bytes(*f).unwrap();
        buffer[..8].copy_from_slice(&bytes[..]);
        // Then the f64 value just to be able to read it back
        let bytes = f.to_be_bytes();
        buffer[8..16].copy_from_slice(&bytes[..]);

        Ok(Cow::Owned(buffer.to_vec()))
    }
}

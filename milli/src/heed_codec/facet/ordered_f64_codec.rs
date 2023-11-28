use std::borrow::Cow;
use std::convert::TryInto;

use heed::{BoxedError, BytesDecode};
use thiserror::Error;

use crate::facet::value_encoding::f64_into_bytes;
use crate::heed_codec::SliceTooShortError;

pub struct OrderedF64Codec;

impl<'a> BytesDecode<'a> for OrderedF64Codec {
    type DItem = f64;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        if bytes.len() < 16 {
            Err(SliceTooShortError.into())
        } else {
            bytes[8..].try_into().map(f64::from_be_bytes).map_err(Into::into)
        }
    }
}

impl heed::BytesEncode<'_> for OrderedF64Codec {
    type EItem = f64;

    fn bytes_encode(f: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        let mut buffer = [0u8; 16];

        // write the globally ordered float
        let bytes = f64_into_bytes(*f).ok_or(InvalidGloballyOrderedFloatError { float: *f })?;
        buffer[..8].copy_from_slice(&bytes[..]);
        // Then the f64 value just to be able to read it back
        let bytes = f.to_be_bytes();
        buffer[8..16].copy_from_slice(&bytes[..]);

        Ok(Cow::Owned(buffer.to_vec()))
    }
}

#[derive(Error, Debug)]
#[error("the float {float} cannot be converted to a globally ordered representation")]
pub struct InvalidGloballyOrderedFloatError {
    float: f64,
}

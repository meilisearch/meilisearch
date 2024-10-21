use std::borrow::Cow;

use fst::Set;
use heed::{BoxedError, BytesDecode, BytesEncode};

/// A codec for values of type `Set<&[u8]>`.
pub struct FstSetCodec;

impl<'a> BytesEncode<'a> for FstSetCodec {
    type EItem = Set<Vec<u8>>;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(item.as_fst().as_bytes()))
    }
}

impl<'a> BytesDecode<'a> for FstSetCodec {
    type DItem = Set<&'a [u8]>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Set::new(bytes).map_err(Into::into)
    }
}

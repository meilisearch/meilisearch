use std::borrow::Cow;

use meilisearch_types::heed::{BoxedError, BytesDecode, BytesEncode};
use uuid::Uuid;

/// A heed codec for value of struct Uuid.
pub struct UuidCodec;

impl<'a> BytesDecode<'a> for UuidCodec {
    type DItem = Uuid;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        bytes.try_into().map(Uuid::from_bytes).map_err(Into::into)
    }
}

impl BytesEncode<'_> for UuidCodec {
    type EItem = Uuid;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        Ok(Cow::Borrowed(item.as_bytes()))
    }
}

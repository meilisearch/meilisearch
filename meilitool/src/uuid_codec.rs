use std::borrow::Cow;
use std::convert::TryInto;

use meilisearch_types::heed::{BytesDecode, BytesEncode};
use uuid::Uuid;

/// A heed codec for value of struct Uuid.
pub struct UuidCodec;

impl<'a> BytesDecode<'a> for UuidCodec {
    type DItem = Uuid;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        bytes.try_into().ok().map(Uuid::from_bytes)
    }
}

impl BytesEncode<'_> for UuidCodec {
    type EItem = Uuid;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(item.as_bytes()))
    }
}

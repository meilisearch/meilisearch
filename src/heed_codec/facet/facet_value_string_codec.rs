use std::borrow::Cow;
use std::str;

use crate::FieldId;

pub struct FacetValueStringCodec;

impl<'a> heed::BytesDecode<'a> for FacetValueStringCodec {
    type DItem = (FieldId, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, bytes) = bytes.split_first()?;
        let value = str::from_utf8(bytes).ok()?;
        Some((*field_id, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FacetValueStringCodec {
    type EItem = (FieldId, &'a str);

    fn bytes_encode((field_id, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(value.len() + 1);
        bytes.push(*field_id);
        bytes.extend_from_slice(value.as_bytes());
        Some(Cow::Owned(bytes))
    }
}

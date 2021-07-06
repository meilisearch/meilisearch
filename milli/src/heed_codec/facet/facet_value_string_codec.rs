use std::borrow::Cow;
use std::str;

use crate::{try_split_array_at, FieldId};

pub struct FacetValueStringCodec;

impl FacetValueStringCodec {
    pub fn serialize_into(field_id: FieldId, value: &str, out: &mut Vec<u8>) {
        out.reserve(value.len() + 2);
        out.extend_from_slice(&field_id.to_be_bytes());
        out.extend_from_slice(value.as_bytes());
    }
}

impl<'a> heed::BytesDecode<'a> for FacetValueStringCodec {
    type DItem = (FieldId, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id_bytes, bytes) = try_split_array_at(bytes)?;
        let field_id = u16::from_be_bytes(field_id_bytes);
        let value = str::from_utf8(bytes).ok()?;
        Some((field_id, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FacetValueStringCodec {
    type EItem = (FieldId, &'a str);

    fn bytes_encode((field_id, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::new();
        FacetValueStringCodec::serialize_into(*field_id, value, &mut bytes);
        Some(Cow::Owned(bytes))
    }
}

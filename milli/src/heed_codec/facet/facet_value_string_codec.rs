use std::borrow::Cow;
use std::str;

use crate::FieldId;

pub struct FacetValueStringCodec;

impl FacetValueStringCodec {
    pub fn serialize_into(field_id: FieldId, value: &str, out: &mut Vec<u8>) {
        out.reserve(value.len() + 1);
        out.push(field_id);
        out.extend_from_slice(value.as_bytes());
    }
}

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
        let mut bytes = Vec::new();
        FacetValueStringCodec::serialize_into(*field_id, value, &mut bytes);
        Some(Cow::Owned(bytes))
    }
}

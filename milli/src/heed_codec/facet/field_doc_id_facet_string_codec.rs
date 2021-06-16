use std::borrow::Cow;
use std::convert::TryInto;
use std::str;

use crate::{DocumentId, FieldId};

pub struct FieldDocIdFacetStringCodec;

impl FieldDocIdFacetStringCodec {
    pub fn serialize_into(
        field_id: FieldId,
        document_id: DocumentId,
        value: &str,
        out: &mut Vec<u8>,
    ) {
        out.reserve(1 + 4 + value.len());
        out.push(field_id);
        out.extend_from_slice(&document_id.to_be_bytes());
        out.extend_from_slice(value.as_bytes());
    }
}

impl<'a> heed::BytesDecode<'a> for FieldDocIdFacetStringCodec {
    type DItem = (FieldId, DocumentId, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, bytes) = bytes.split_first()?;
        let (document_id_bytes, bytes) = bytes.split_at(4);
        let document_id = document_id_bytes.try_into().map(u32::from_be_bytes).ok()?;
        let value = str::from_utf8(bytes).ok()?;
        Some((*field_id, document_id, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FieldDocIdFacetStringCodec {
    type EItem = (FieldId, DocumentId, &'a str);

    fn bytes_encode((field_id, document_id, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::new();
        FieldDocIdFacetStringCodec::serialize_into(*field_id, *document_id, value, &mut bytes);
        Some(Cow::Owned(bytes))
    }
}

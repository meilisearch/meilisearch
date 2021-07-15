use std::borrow::Cow;
use std::str;

use crate::{try_split_array_at, DocumentId, FieldId};

pub struct FieldDocIdFacetStringCodec;

impl FieldDocIdFacetStringCodec {
    pub fn serialize_into(
        field_id: FieldId,
        document_id: DocumentId,
        normalized_value: &str,
        out: &mut Vec<u8>,
    ) {
        out.reserve(2 + 4 + normalized_value.len());
        out.extend_from_slice(&field_id.to_be_bytes());
        out.extend_from_slice(&document_id.to_be_bytes());
        out.extend_from_slice(normalized_value.as_bytes());
    }
}

impl<'a> heed::BytesDecode<'a> for FieldDocIdFacetStringCodec {
    type DItem = (FieldId, DocumentId, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id_bytes, bytes) = try_split_array_at(bytes)?;
        let field_id = u16::from_be_bytes(field_id_bytes);

        let (document_id_bytes, bytes) = try_split_array_at(bytes)?;
        let document_id = u32::from_be_bytes(document_id_bytes);

        let normalized_value = str::from_utf8(bytes).ok()?;
        Some((field_id, document_id, normalized_value))
    }
}

impl<'a> heed::BytesEncode<'a> for FieldDocIdFacetStringCodec {
    type EItem = (FieldId, DocumentId, &'a str);

    fn bytes_encode((field_id, document_id, normalized_value): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::new();
        FieldDocIdFacetStringCodec::serialize_into(
            *field_id,
            *document_id,
            normalized_value,
            &mut bytes,
        );
        Some(Cow::Owned(bytes))
    }
}

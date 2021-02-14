use std::borrow::Cow;
use std::convert::TryInto;
use std::str;

use crate::{FieldId, DocumentId};

pub struct FieldDocIdFacetStringCodec;

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
        let mut bytes = Vec::with_capacity(1 + 4 + value.len());
        bytes.push(*field_id);
        bytes.extend_from_slice(&document_id.to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
        Some(Cow::Owned(bytes))
    }
}

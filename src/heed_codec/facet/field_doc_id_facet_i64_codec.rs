use std::borrow::Cow;
use std::convert::TryInto;

use crate::facet::value_encoding::{i64_into_bytes, i64_from_bytes};
use crate::{FieldId, DocumentId};

pub struct FieldDocIdFacetI64Codec;

impl<'a> heed::BytesDecode<'a> for FieldDocIdFacetI64Codec {
    type DItem = (FieldId, DocumentId, i64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, bytes) = bytes.split_first()?;

        let (document_id_bytes, bytes) = bytes.split_at(4);
        let document_id = document_id_bytes.try_into().map(u32::from_be_bytes).ok()?;

        let value = bytes[..8].try_into().map(i64_from_bytes).ok()?;

        Some((*field_id, document_id, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FieldDocIdFacetI64Codec {
    type EItem = (FieldId, DocumentId, i64);

    fn bytes_encode((field_id, document_id, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(1 + 4 + 8);
        bytes.push(*field_id);
        bytes.extend_from_slice(&document_id.to_be_bytes());
        bytes.extend_from_slice(&i64_into_bytes(*value));
        Some(Cow::Owned(bytes))
    }
}

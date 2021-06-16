use std::borrow::Cow;
use std::convert::TryInto;

use crate::facet::value_encoding::f64_into_bytes;
use crate::{DocumentId, FieldId};

pub struct FieldDocIdFacetF64Codec;

impl<'a> heed::BytesDecode<'a> for FieldDocIdFacetF64Codec {
    type DItem = (FieldId, DocumentId, f64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, bytes) = bytes.split_first()?;

        let (document_id_bytes, bytes) = bytes.split_at(4);
        let document_id = document_id_bytes.try_into().map(u32::from_be_bytes).ok()?;

        let value = bytes[8..16].try_into().map(f64::from_be_bytes).ok()?;

        Some((*field_id, document_id, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FieldDocIdFacetF64Codec {
    type EItem = (FieldId, DocumentId, f64);

    fn bytes_encode((field_id, document_id, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(1 + 4 + 8 + 8);
        bytes.push(*field_id);
        bytes.extend_from_slice(&document_id.to_be_bytes());
        let value_bytes = f64_into_bytes(*value)?;
        bytes.extend_from_slice(&value_bytes);
        bytes.extend_from_slice(&value.to_be_bytes());
        Some(Cow::Owned(bytes))
    }
}

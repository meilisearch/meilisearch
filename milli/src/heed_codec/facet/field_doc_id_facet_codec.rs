use std::borrow::Cow;
use std::marker::PhantomData;

use heed::{BytesDecode, BytesEncode};

use crate::{try_split_array_at, DocumentId, FieldId};

pub struct FieldDocIdFacetCodec<C>(PhantomData<C>);

impl<'a, C> BytesDecode<'a> for FieldDocIdFacetCodec<C>
where
    C: BytesDecode<'a>,
{
    type DItem = (FieldId, DocumentId, C::DItem);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id_bytes, bytes) = try_split_array_at(bytes)?;
        let field_id = u16::from_be_bytes(field_id_bytes);

        let (document_id_bytes, bytes) = try_split_array_at(bytes)?;
        let document_id = u32::from_be_bytes(document_id_bytes);

        let value = C::bytes_decode(bytes)?;

        Some((field_id, document_id, value))
    }
}

impl<'a, C> BytesEncode<'a> for FieldDocIdFacetCodec<C>
where
    C: BytesEncode<'a>,
{
    type EItem = (FieldId, DocumentId, C::EItem);

    fn bytes_encode((field_id, document_id, value): &'a Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(&field_id.to_be_bytes()); // 2 bytes
        bytes.extend_from_slice(&document_id.to_be_bytes()); // 4 bytes
        let value_bytes = C::bytes_encode(value)?;
        // variable length, if f64 -> 16 bytes, if string -> large, potentially
        bytes.extend_from_slice(&value_bytes);
        Some(Cow::Owned(bytes))
    }
}

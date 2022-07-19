mod facet_level_value_f64_codec;
mod facet_level_value_u32_codec;
mod facet_string_level_zero_codec;
mod facet_string_level_zero_value_codec;
mod facet_string_zero_bounds_value_codec;
mod field_doc_id_facet_f64_codec;
mod field_doc_id_facet_string_codec;

pub use self::facet_level_value_f64_codec::FacetLevelValueF64Codec;
pub use self::facet_level_value_u32_codec::FacetLevelValueU32Codec;
pub use self::facet_string_level_zero_codec::FacetStringLevelZeroCodec;
pub use self::facet_string_level_zero_value_codec::{
    decode_prefix_string, encode_prefix_string, FacetStringLevelZeroValueCodec,
};
pub use self::facet_string_zero_bounds_value_codec::FacetStringZeroBoundsValueCodec;
pub use self::field_doc_id_facet_f64_codec::FieldDocIdFacetF64Codec;
pub use self::field_doc_id_facet_string_codec::FieldDocIdFacetStringCodec;

/// Tries to split a slice in half at the given middle point,
/// `None` if the slice is too short.
pub fn try_split_at(slice: &[u8], mid: usize) -> Option<(&[u8], &[u8])> {
    if slice.len() >= mid {
        Some(slice.split_at(mid))
    } else {
        None
    }
}

use crate::{try_split_array_at, DocumentId, FieldId};
use std::borrow::Cow;
use std::convert::TryInto;

pub struct FieldIdCodec;

impl<'a> heed::BytesDecode<'a> for FieldIdCodec {
    type DItem = FieldId;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id_bytes, _) = try_split_array_at(bytes)?;
        let field_id = u16::from_be_bytes(field_id_bytes);
        Some(field_id)
    }
}

impl<'a> heed::BytesEncode<'a> for FieldIdCodec {
    type EItem = FieldId;

    fn bytes_encode(field_id: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Owned(field_id.to_be_bytes().to_vec()))
    }
}

pub struct FieldIdDocIdCodec;

impl<'a> heed::BytesDecode<'a> for FieldIdDocIdCodec {
    type DItem = (FieldId, DocumentId);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id_bytes, bytes) = try_split_array_at(bytes)?;
        let field_id = u16::from_be_bytes(field_id_bytes);

        let document_id_bytes = bytes[..4].try_into().ok()?;
        let document_id = u32::from_be_bytes(document_id_bytes);

        Some((field_id, document_id))
    }
}

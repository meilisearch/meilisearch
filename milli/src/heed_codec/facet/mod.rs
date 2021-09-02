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

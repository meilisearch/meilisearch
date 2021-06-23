use std::borrow::Cow;
use std::str;

use crate::FieldId;

/// A codec that stores the field id, level 0, and facet string.
///
/// It can only be used to encode the facet string of the level 0,
/// as it hardcodes the level.
///
/// We encode the level 0 to not break the lexicographical ordering of the LMDB keys,
/// and make sure that the levels are not mixed-up. The level 0 is special, the key
/// are strings, other levels represent groups and keys are simply two integers.
pub struct FacetStringLevelZeroCodec;

impl FacetStringLevelZeroCodec {
    pub fn serialize_into(field_id: FieldId, value: &str, out: &mut Vec<u8>) {
        out.reserve(value.len() + 2);
        out.push(field_id);
        out.push(0); // the level zero (for LMDB ordering only)
        out.extend_from_slice(value.as_bytes());
    }
}

impl<'a> heed::BytesDecode<'a> for FacetStringLevelZeroCodec {
    type DItem = (FieldId, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, bytes) = bytes.split_first()?;
        let (level, bytes) = bytes.split_first()?;

        if *level != 0 {
            return None;
        }

        let value = str::from_utf8(bytes).ok()?;
        Some((*field_id, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FacetStringLevelZeroCodec {
    type EItem = (FieldId, &'a str);

    fn bytes_encode((field_id, value): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::new();
        FacetStringLevelZeroCodec::serialize_into(*field_id, value, &mut bytes);
        Some(Cow::Owned(bytes))
    }
}

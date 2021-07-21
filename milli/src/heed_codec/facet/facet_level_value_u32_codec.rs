use std::borrow::Cow;
use std::convert::TryInto;
use std::num::NonZeroU8;

use crate::{try_split_array_at, FieldId};

/// A codec that stores the field id, level 1 and higher and the groups ids.
///
/// It can only be used to encode the facet string of the level 1 or higher.
pub struct FacetLevelValueU32Codec;

impl<'a> heed::BytesDecode<'a> for FacetLevelValueU32Codec {
    type DItem = (FieldId, NonZeroU8, u32, u32);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id_bytes, bytes) = try_split_array_at(bytes)?;
        let field_id = u16::from_be_bytes(field_id_bytes);
        let (level, bytes) = bytes.split_first()?;
        let level = NonZeroU8::new(*level)?;
        let left = bytes[8..12].try_into().ok().map(u32::from_be_bytes)?;
        let right = bytes[12..].try_into().ok().map(u32::from_be_bytes)?;
        Some((field_id, level, left, right))
    }
}

impl heed::BytesEncode<'_> for FacetLevelValueU32Codec {
    type EItem = (FieldId, NonZeroU8, u32, u32);

    fn bytes_encode((field_id, level, left, right): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut buffer = [0u8; 16];

        // Write the big-endian integers.
        let bytes = left.to_be_bytes();
        buffer[..4].copy_from_slice(&bytes[..]);

        let bytes = right.to_be_bytes();
        buffer[4..8].copy_from_slice(&bytes[..]);

        // Then the u32 values just to be able to read them back.
        let bytes = left.to_be_bytes();
        buffer[8..12].copy_from_slice(&bytes[..]);

        let bytes = right.to_be_bytes();
        buffer[12..].copy_from_slice(&bytes[..]);

        let mut bytes = Vec::with_capacity(buffer.len() + 2 + 1);
        bytes.extend_from_slice(&field_id.to_be_bytes());
        bytes.push(level.get());
        bytes.extend_from_slice(&buffer);

        Some(Cow::Owned(bytes))
    }
}

use std::borrow::Cow;

use crate::{try_split_array_at, FieldId};

pub struct FieldIdWordCountCodec;

impl<'a> heed::BytesDecode<'a> for FieldIdWordCountCodec {
    type DItem = (FieldId, u8);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id_bytes, bytes) = try_split_array_at(bytes)?;
        let field_id = u16::from_be_bytes(field_id_bytes);
        let ([word_count], _nothing) = try_split_array_at(bytes)?;
        Some((field_id, word_count))
    }
}

impl<'a> heed::BytesEncode<'a> for FieldIdWordCountCodec {
    type EItem = (FieldId, u8);

    fn bytes_encode((field_id, word_count): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(2 + 1);
        bytes.extend_from_slice(&field_id.to_be_bytes());
        bytes.push(*word_count);
        Some(Cow::Owned(bytes))
    }
}

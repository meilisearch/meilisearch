use std::borrow::Cow;

use heed::BoxedError;

use super::SliceTooShortError;
use crate::{try_split_array_at, FieldId};

pub struct FieldIdWordCountCodec;

impl<'a> heed::BytesDecode<'a> for FieldIdWordCountCodec {
    type DItem = (FieldId, u8);

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        let (field_id_bytes, bytes) = try_split_array_at(bytes).ok_or(SliceTooShortError)?;
        let field_id = u16::from_be_bytes(field_id_bytes);
        let ([word_count], _nothing) = try_split_array_at(bytes).ok_or(SliceTooShortError)?;
        Ok((field_id, word_count))
    }
}

impl<'a> heed::BytesEncode<'a> for FieldIdWordCountCodec {
    type EItem = (FieldId, u8);

    fn bytes_encode((field_id, word_count): &Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let mut bytes = Vec::with_capacity(2 + 1);
        bytes.extend_from_slice(&field_id.to_be_bytes());
        bytes.push(*word_count);
        Ok(Cow::Owned(bytes))
    }
}

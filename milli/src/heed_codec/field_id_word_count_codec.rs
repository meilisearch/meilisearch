use std::{borrow::Cow, convert::TryInto};

use crate::FieldId;

pub struct FieldIdWordCountCodec;

impl<'a> heed::BytesDecode<'a> for FieldIdWordCountCodec {
    type DItem = (FieldId, u8);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let [field_id, word_count]: [u8; 2] = bytes.try_into().ok()?;
        Some((field_id, word_count))
    }
}

impl<'a> heed::BytesEncode<'a> for FieldIdWordCountCodec {
    type EItem = (FieldId, u8);

    fn bytes_encode((field_id, word_count): &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Owned(vec![*field_id, *word_count]))
    }
}

use crate::{FieldId, BEU16};
use heed::zerocopy::AsBytes;
use std::{borrow::Cow, convert::TryInto};

pub struct FieldIdCodec;

impl<'a> heed::BytesDecode<'a> for FieldIdCodec {
    type DItem = FieldId;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let bytes: [u8; 2] = bytes[..2].try_into().ok()?;
        let field_id = BEU16::from(bytes).get();
        Some(field_id)
    }
}

impl<'a> heed::BytesEncode<'a> for FieldIdCodec {
    type EItem = FieldId;

    fn bytes_encode(field_id: &Self::EItem) -> Option<Cow<[u8]>> {
        let field_id = BEU16::new(*field_id);
        let bytes = field_id.as_bytes();
        Some(Cow::Owned(bytes.to_vec()))
    }
}

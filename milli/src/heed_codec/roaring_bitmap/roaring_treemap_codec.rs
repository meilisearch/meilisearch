use std::borrow::Cow;

use roaring::RoaringTreemap;

use crate::heed_codec::BytesDecodeOwned;

pub struct RoaringTreemapCodec;

impl heed::BytesDecode<'_> for RoaringTreemapCodec {
    type DItem = RoaringTreemap;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        RoaringTreemap::deserialize_unchecked_from(bytes).ok()
    }
}

impl BytesDecodeOwned for RoaringTreemapCodec {
    type DItem = RoaringTreemap;

    fn bytes_decode_owned(bytes: &[u8]) -> Option<Self::DItem> {
        RoaringTreemap::deserialize_from(bytes).ok()
    }
}

impl heed::BytesEncode<'_> for RoaringTreemapCodec {
    type EItem = RoaringTreemap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(item.serialized_size());
        item.serialize_into(&mut bytes).ok()?;
        Some(Cow::Owned(bytes))
    }
}

use std::borrow::Cow;

use roaring::RoaringBitmap;

pub struct RoaringBitmapCodec;

impl heed::BytesDecode<'_> for RoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        RoaringBitmap::deserialize_unchecked_from(bytes).ok()
    }
}

impl heed::BytesEncode<'_> for RoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(item.serialized_size());
        item.serialize_into(&mut bytes).ok()?;
        Some(Cow::Owned(bytes))
    }
}

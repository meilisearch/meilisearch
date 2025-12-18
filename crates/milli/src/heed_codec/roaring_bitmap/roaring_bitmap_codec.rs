use std::borrow::Cow;

use heed::BoxedError;
use roaring::RoaringBitmap;

use crate::heed_codec::BytesDecodeOwned;

pub struct RoaringBitmapCodec;

impl heed::BytesDecode<'_> for RoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        RoaringBitmap::deserialize_unchecked_from(bytes).map_err(Into::into)
    }
}

impl BytesDecodeOwned for RoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        RoaringBitmap::deserialize_from(bytes).map_err(Into::into)
    }
}

impl heed::BytesEncode<'_> for RoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        let mut bytes = Vec::with_capacity(item.serialized_size());
        item.serialize_into(&mut bytes)?;
        Ok(Cow::Owned(bytes))
    }
}

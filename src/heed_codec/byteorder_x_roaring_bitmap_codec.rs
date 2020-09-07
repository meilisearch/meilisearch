use std::borrow::Cow;
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use roaring::RoaringBitmap;

pub struct ByteorderXRoaringBitmapCodec;

impl heed::BytesDecode<'_> for ByteorderXRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(mut bytes: &[u8]) -> Option<Self::DItem> {
        let mut bitmap = RoaringBitmap::new();
        while let Ok(integer) = bytes.read_u32::<NativeEndian>() {
            bitmap.insert(integer);
        }
        Some(bitmap)
    }
}

impl heed::BytesEncode<'_> for ByteorderXRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(item.len() as usize * 4);
        for integer in item.iter() {
            bytes.write_u32::<NativeEndian>(integer).ok()?;
        }
        Some(Cow::Owned(bytes))
    }
}

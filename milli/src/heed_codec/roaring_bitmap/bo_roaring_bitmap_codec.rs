use std::borrow::Cow;
use std::mem::size_of;

use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use roaring::RoaringBitmap;

pub struct BoRoaringBitmapCodec;

impl heed::BytesDecode<'_> for BoRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        let mut bitmap = RoaringBitmap::new();
        let num_u32 = bytes.len() / size_of::<u32>();
        for i in 0..num_u32 {
            let start = i * size_of::<u32>();
            let end = (i + 1) * size_of::<u32>();
            let mut bytes = bytes.get(start..end)?;
            let integer = bytes.read_u32::<NativeEndian>().ok()?;
            bitmap.insert(integer);
        }
        Some(bitmap)
    }
}

impl heed::BytesEncode<'_> for BoRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(item.len() as usize * 4);
        for integer in item.iter() {
            bytes.write_u32::<NativeEndian>(integer).ok()?;
        }
        Some(Cow::Owned(bytes))
    }
}

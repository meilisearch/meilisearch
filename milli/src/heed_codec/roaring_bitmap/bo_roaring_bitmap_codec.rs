use std::borrow::Cow;
use std::convert::TryInto;
use std::mem::size_of;

use roaring::RoaringBitmap;

pub struct BoRoaringBitmapCodec;

impl heed::BytesDecode<'_> for BoRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        let mut bitmap = RoaringBitmap::new();

        for chunk in bytes.chunks(size_of::<u32>()) {
            let bytes = chunk.try_into().ok()?;
            bitmap.push(u32::from_ne_bytes(bytes));
        }

        Some(bitmap)
    }
}

impl heed::BytesEncode<'_> for BoRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let mut out = Vec::with_capacity(item.len() as usize * size_of::<u32>());

        item.iter()
            .map(|i| i.to_ne_bytes())
            .for_each(|bytes| out.extend_from_slice(&bytes));

        Some(Cow::Owned(out))
    }
}

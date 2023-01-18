use std::borrow::Cow;
use std::convert::TryInto;
use std::mem::size_of;

use roaring::RoaringBitmap;

pub struct BoRoaringBitmapCodec;

impl BoRoaringBitmapCodec {
    pub fn serialize_into(bitmap: &RoaringBitmap, out: &mut Vec<u8>) {
        out.reserve(bitmap.len() as usize * size_of::<u32>());
        bitmap.iter().map(u32::to_ne_bytes).for_each(|bytes| out.extend_from_slice(&bytes));
    }
}

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
        let mut out = Vec::new();
        BoRoaringBitmapCodec::serialize_into(item, &mut out);
        Some(Cow::Owned(out))
    }
}

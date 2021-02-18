use std::mem;

pub struct BoRoaringBitmapLenCodec;

impl heed::BytesDecode<'_> for BoRoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        Some((bytes.len() / mem::size_of::<u32>()) as u64)
    }
}

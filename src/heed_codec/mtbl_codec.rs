use std::borrow::Cow;
use oxidized_mtbl::Reader;

pub struct MtblCodec;

impl<'a> heed::BytesDecode<'a> for MtblCodec {
    type DItem = Reader<&'a [u8]>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        Reader::new(bytes).ok()
    }
}

impl heed::BytesEncode<'_> for MtblCodec {
    type EItem = [u8];

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(item))
    }
}

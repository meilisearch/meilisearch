use std::borrow::Cow;

use obkv::{KvReaderU16, KvWriterU16};

pub struct ObkvCodec;

impl<'a> heed::BytesDecode<'a> for ObkvCodec {
    type DItem = KvReaderU16<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        Some(KvReaderU16::new(bytes))
    }
}

impl heed::BytesEncode<'_> for ObkvCodec {
    type EItem = KvWriterU16<Vec<u8>>;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        item.clone().into_inner().map(Cow::Owned).ok()
    }
}

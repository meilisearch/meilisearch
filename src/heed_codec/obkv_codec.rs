use std::borrow::Cow;
use obkv::{KvReader, KvWriter};

pub struct ObkvCodec;

impl<'a> heed::BytesDecode<'a> for ObkvCodec {
    type DItem = KvReader<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        Some(KvReader::new(bytes))
    }
}

impl heed::BytesEncode<'_> for ObkvCodec {
    type EItem = KvWriter<Vec<u8>>;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        item.clone().into_inner().map(Cow::Owned).ok()
    }
}

use std::borrow::Cow;

use heed::BoxedError;
use obkv::{KvReaderU16, KvWriterU16};

pub struct ObkvCodec;

impl<'a> heed::BytesDecode<'a> for ObkvCodec {
    type DItem = &'a KvReaderU16;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(KvReaderU16::from_slice(bytes))
    }
}

impl heed::BytesEncode<'_> for ObkvCodec {
    type EItem = KvWriterU16<Vec<u8>>;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        item.clone().into_inner().map(Cow::Owned).map_err(Into::into)
    }
}

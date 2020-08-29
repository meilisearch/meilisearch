use std::borrow::Cow;
use std::marker::PhantomData;
use oxidized_mtbl::Reader;

pub struct MtblCodec<A>(PhantomData<A>);

impl<'a> heed::BytesDecode<'a> for MtblCodec<&'a [u8]> {
    type DItem = Reader<&'a [u8]>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        Reader::new(bytes).ok()
    }
}

impl<'a, A: AsRef<[u8]> + 'a> heed::BytesEncode<'a> for MtblCodec<A> {
    type EItem = Reader<A>;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(item.as_bytes()))
    }
}

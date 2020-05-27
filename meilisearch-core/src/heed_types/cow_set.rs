use std::borrow::Cow;

use heed::{types::CowSlice, BytesEncode, BytesDecode};
use sdset::{Set, SetBuf};
use zerocopy::{AsBytes, FromBytes};

pub struct CowSet<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> BytesEncode<'a> for CowSet<T>
where
    T: AsBytes,
{
    type EItem = Set<T>;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<[u8]>> {
        CowSlice::bytes_encode(item.as_slice())
    }
}

impl<'a, T: 'a> BytesDecode<'a> for CowSet<T>
where
    T: FromBytes + Copy,
{
    type DItem = Cow<'a, Set<T>>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        match CowSlice::<T>::bytes_decode(bytes)? {
            Cow::Owned(vec) => Some(Cow::Owned(SetBuf::new_unchecked(vec))),
            Cow::Borrowed(slice) => Some(Cow::Borrowed(Set::new_unchecked(slice))),
        }
    }
}

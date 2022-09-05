use std::borrow::Cow;
use std::convert::TryFrom;
use std::marker::PhantomData;

use heed::{BytesDecode, BytesEncode};
use roaring::RoaringBitmap;

use crate::CboRoaringBitmapCodec;

pub mod ordered_f64_codec;
pub mod str_ref;
// TODO: these codecs were quickly written and not fast/resilient enough

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FacetKey<T> {
    pub field_id: u16,
    pub level: u8,
    pub left_bound: T,
}
impl<'a> FacetKey<&'a [u8]> {
    pub fn into_owned(self) -> FacetKey<Vec<u8>> {
        FacetKey {
            field_id: self.field_id,
            level: self.level,
            left_bound: self.left_bound.to_vec(),
        }
    }
}

impl<'a> FacetKey<Vec<u8>> {
    pub fn as_ref(&self) -> FacetKey<&[u8]> {
        FacetKey {
            field_id: self.field_id,
            level: self.level,
            left_bound: self.left_bound.as_slice(),
        }
    }
}

#[derive(Debug)]
pub struct FacetGroupValue {
    pub size: u8,
    pub bitmap: RoaringBitmap,
}

pub struct FacetKeyCodec<T> {
    _phantom: PhantomData<T>,
}

impl<'a, T> heed::BytesEncode<'a> for FacetKeyCodec<T>
where
    T: BytesEncode<'a>,
    T::EItem: Sized,
{
    type EItem = FacetKey<T::EItem>;

    fn bytes_encode(value: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut v = vec![];
        v.extend_from_slice(&value.field_id.to_be_bytes());
        v.extend_from_slice(&[value.level]);

        let bound = T::bytes_encode(&value.left_bound)?;
        v.extend_from_slice(&bound);

        Some(Cow::Owned(v))
    }
}
impl<'a, T> heed::BytesDecode<'a> for FacetKeyCodec<T>
where
    T: BytesDecode<'a>,
{
    type DItem = FacetKey<T::DItem>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let fid = u16::from_be_bytes(<[u8; 2]>::try_from(&bytes[0..=1]).ok()?);
        let level = bytes[2];
        let bound = T::bytes_decode(&bytes[3..])?;
        Some(FacetKey { field_id: fid, level, left_bound: bound })
    }
}

pub struct FacetGroupValueCodec;
impl<'a> heed::BytesEncode<'a> for FacetGroupValueCodec {
    type EItem = FacetGroupValue;

    fn bytes_encode(value: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut v = vec![];
        v.push(value.size);
        CboRoaringBitmapCodec::serialize_into(&value.bitmap, &mut v);
        Some(Cow::Owned(v))
    }
}
impl<'a> heed::BytesDecode<'a> for FacetGroupValueCodec {
    type DItem = FacetGroupValue;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let size = bytes[0];
        let bitmap = CboRoaringBitmapCodec::deserialize_from(&bytes[1..]).ok()?;
        Some(FacetGroupValue { size, bitmap })
    }
}

// TODO: get rid of this codec as it is named confusingly + should really be part of heed
// or even replace the current ByteSlice codec
pub struct MyByteSlice;

impl<'a> BytesEncode<'a> for MyByteSlice {
    type EItem = &'a [u8];

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        Some(Cow::Borrowed(item))
    }
}

impl<'a> BytesDecode<'a> for MyByteSlice {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        Some(bytes)
    }
}

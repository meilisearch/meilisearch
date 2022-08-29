use heed::{BytesDecode, BytesEncode};
use roaring::RoaringBitmap;
use std::{borrow::Cow, convert::TryFrom, marker::PhantomData};

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

        let bound = T::bytes_encode(&value.left_bound).unwrap();
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
        let fid = u16::from_be_bytes(<[u8; 2]>::try_from(&bytes[0..=1]).unwrap());
        let level = bytes[2];
        let bound = T::bytes_decode(&bytes[3..]).unwrap();
        Some(FacetKey { field_id: fid, level, left_bound: bound })
    }
}

pub struct FacetGroupValueCodec;
impl<'a> heed::BytesEncode<'a> for FacetGroupValueCodec {
    type EItem = FacetGroupValue;

    fn bytes_encode(value: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut v = vec![];
        v.push(value.size);
        value.bitmap.serialize_into(&mut v).unwrap();
        Some(Cow::Owned(v))
    }
}
impl<'a> heed::BytesDecode<'a> for FacetGroupValueCodec {
    type DItem = FacetGroupValue;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let size = bytes[0];
        let bitmap = RoaringBitmap::deserialize_from(&bytes[1..]).unwrap();
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

// I won't need these ones anymore
// pub struct U16Codec;
// impl<'a> BytesEncode<'a> for U16Codec {
//     type EItem = u16;

//     fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
//         Some(Cow::Owned(item.to_be_bytes().to_vec()))
//     }
// }
// impl<'a> BytesDecode<'a> for U16Codec {
//     type DItem = u16;

//     fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
//         Some(u16::from_be_bytes(bytes[0..=1].try_into().unwrap()))
//     }
// }

// pub struct StrCodec;
// impl<'a> BytesEncode<'a> for StrCodec {
//     type EItem = &'a str;

//     fn bytes_encode(item: &'a &'a str) -> Option<Cow<'a, [u8]>> {
//         Some(Cow::Borrowed(item.as_bytes()))
//     }
// }
// impl<'a> BytesDecode<'a> for StrCodec {
//     type DItem = &'a str;

//     fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
//         let s = std::str::from_utf8(bytes).unwrap();
//         Some(s)
//     }
// }

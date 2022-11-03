mod field_doc_id_facet_codec;
mod ordered_f64_codec;

use std::borrow::Cow;
use std::convert::TryFrom;
use std::marker::PhantomData;

use heed::types::{DecodeIgnore, OwnedType};
use heed::{BytesDecode, BytesEncode};
use roaring::RoaringBitmap;

pub use self::field_doc_id_facet_codec::FieldDocIdFacetCodec;
pub use self::ordered_f64_codec::OrderedF64Codec;
use super::StrRefCodec;
use crate::{CboRoaringBitmapCodec, BEU16};

pub type FieldDocIdFacetF64Codec = FieldDocIdFacetCodec<OrderedF64Codec>;
pub type FieldDocIdFacetStringCodec = FieldDocIdFacetCodec<StrRefCodec>;
pub type FieldDocIdFacetIgnoreCodec = FieldDocIdFacetCodec<DecodeIgnore>;

pub type FieldIdCodec = OwnedType<BEU16>;

/// Tries to split a slice in half at the given middle point,
/// `None` if the slice is too short.
pub fn try_split_at(slice: &[u8], mid: usize) -> Option<(&[u8], &[u8])> {
    if slice.len() >= mid {
        Some(slice.split_at(mid))
    } else {
        None
    }
}

/// The key in the [`facet_id_string_docids` and `facet_id_f64_docids`][`Index::facet_id_string_docids`]
/// databases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)] // TODO: try removing PartialOrd and Ord
pub struct FacetGroupKey<T> {
    pub field_id: u16,
    pub level: u8,
    pub left_bound: T,
}

/// The value in the [`facet_id_string_docids` and `facet_id_f64_docids`][`Index::facet_id_string_docids`]
/// databases.
#[derive(Debug)]
pub struct FacetGroupValue {
    pub size: u8,
    pub bitmap: RoaringBitmap,
}

pub struct FacetGroupKeyCodec<T> {
    _phantom: PhantomData<T>,
}

impl<'a, T> heed::BytesEncode<'a> for FacetGroupKeyCodec<T>
where
    T: BytesEncode<'a>,
    T::EItem: Sized,
{
    type EItem = FacetGroupKey<T::EItem>;

    fn bytes_encode(value: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut v = vec![];
        v.extend_from_slice(&value.field_id.to_be_bytes());
        v.extend_from_slice(&[value.level]);

        let bound = T::bytes_encode(&value.left_bound)?;
        v.extend_from_slice(&bound);

        Some(Cow::Owned(v))
    }
}
impl<'a, T> heed::BytesDecode<'a> for FacetGroupKeyCodec<T>
where
    T: BytesDecode<'a>,
{
    type DItem = FacetGroupKey<T::DItem>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let fid = u16::from_be_bytes(<[u8; 2]>::try_from(&bytes[0..=1]).ok()?);
        let level = bytes[2];
        let bound = T::bytes_decode(&bytes[3..])?;
        Some(FacetGroupKey { field_id: fid, level, left_bound: bound })
    }
}

pub struct FacetGroupValueCodec;
impl<'a> heed::BytesEncode<'a> for FacetGroupValueCodec {
    type EItem = FacetGroupValue;

    fn bytes_encode(value: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut v = vec![value.size];
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

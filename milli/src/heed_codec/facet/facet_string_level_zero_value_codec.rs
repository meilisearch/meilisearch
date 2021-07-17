use std::borrow::Cow;
use std::convert::TryInto;
use std::{marker, str};

use super::try_split_at;

/// A codec that encodes a string in front of the value.
///
/// The usecase is for the facet string levels algorithm where we must know the
/// original string of a normalized facet value, the original values are stored
/// in the value to not break the lexicographical ordering of the LMDB keys.
pub struct FacetStringLevelZeroValueCodec<C>(marker::PhantomData<C>);

impl<'a, C> heed::BytesDecode<'a> for FacetStringLevelZeroValueCodec<C>
where
    C: heed::BytesDecode<'a>,
{
    type DItem = (&'a str, C::DItem);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (string_len, bytes) = try_split_at(bytes, 2)?;
        let string_len = string_len.try_into().ok().map(u16::from_be_bytes)?;

        let (string, bytes) = try_split_at(bytes, string_len as usize)?;
        let string = str::from_utf8(string).ok()?;

        C::bytes_decode(bytes).map(|item| (string, item))
    }
}

impl<'a, C> heed::BytesEncode<'a> for FacetStringLevelZeroValueCodec<C>
where
    C: heed::BytesEncode<'a>,
{
    type EItem = (&'a str, C::EItem);

    fn bytes_encode((string, value): &'a Self::EItem) -> Option<Cow<[u8]>> {
        let string_len: u16 = string.len().try_into().ok()?;
        let value_bytes = C::bytes_encode(&value)?;

        let mut bytes = Vec::with_capacity(2 + string.len() + value_bytes.len());
        bytes.extend_from_slice(&string_len.to_be_bytes());
        bytes.extend_from_slice(string.as_bytes());
        bytes.extend_from_slice(&value_bytes[..]);

        Some(Cow::Owned(bytes))
    }
}

#[cfg(test)]
mod tests {
    use heed::types::Unit;
    use heed::{BytesDecode, BytesEncode};
    use roaring::RoaringBitmap;

    use super::*;
    use crate::CboRoaringBitmapCodec;

    #[test]
    fn deserialize_roaring_bitmaps() {
        let string = "abc";
        let docids: RoaringBitmap = (0..100).chain(3500..4398).collect();
        let key = (string, docids.clone());
        let bytes =
            FacetStringLevelZeroValueCodec::<CboRoaringBitmapCodec>::bytes_encode(&key).unwrap();
        let (out_string, out_docids) =
            FacetStringLevelZeroValueCodec::<CboRoaringBitmapCodec>::bytes_decode(&bytes).unwrap();
        assert_eq!((out_string, out_docids), (string, docids));
    }

    #[test]
    fn deserialize_unit() {
        let string = "def";
        let key = (string, ());
        let bytes = FacetStringLevelZeroValueCodec::<Unit>::bytes_encode(&key).unwrap();
        let (out_string, out_unit) =
            FacetStringLevelZeroValueCodec::<Unit>::bytes_decode(&bytes).unwrap();
        assert_eq!((out_string, out_unit), (string, ()));
    }
}

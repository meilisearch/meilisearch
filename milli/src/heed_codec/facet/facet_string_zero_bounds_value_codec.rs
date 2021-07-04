use std::borrow::Cow;
use std::convert::TryInto;
use std::{marker, str};

/// A codec that encodes two strings in front of the value.
///
/// The usecase is for the facet string levels algorithm where we must
/// know the origin of a group, the group left and right bounds are stored
/// in the value to not break the lexicographical ordering of the LMDB keys.
pub struct FacetStringZeroBoundsValueCodec<C>(marker::PhantomData<C>);

impl<'a, C> heed::BytesDecode<'a> for FacetStringZeroBoundsValueCodec<C>
where
    C: heed::BytesDecode<'a>,
{
    type DItem = (Option<(&'a str, &'a str)>, C::DItem);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (contains_bounds, bytes) = bytes.split_first()?;

        if *contains_bounds != 0 {
            let (left_len, bytes) = try_split_at(bytes, 2)?;
            let (right_len, bytes) = try_split_at(bytes, 2)?;

            let left_len = left_len.try_into().ok().map(u16::from_be_bytes)?;
            let right_len = right_len.try_into().ok().map(u16::from_be_bytes)?;

            let (left, bytes) = try_split_at(bytes, left_len as usize)?;
            let (right, bytes) = try_split_at(bytes, right_len as usize)?;

            let left = str::from_utf8(left).ok()?;
            let right = str::from_utf8(right).ok()?;

            C::bytes_decode(bytes).map(|item| (Some((left, right)), item))
        } else {
            C::bytes_decode(bytes).map(|item| (None, item))
        }
    }
}

impl<'a, C> heed::BytesEncode<'a> for FacetStringZeroBoundsValueCodec<C>
where
    C: heed::BytesEncode<'a>,
{
    type EItem = (Option<(&'a str, &'a str)>, C::EItem);

    fn bytes_encode((bounds, value): &'a Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::new();

        match bounds {
            Some((left, right)) => {
                bytes.push(u8::max_value());

                if left.is_empty() || right.is_empty() {
                    return None;
                }

                let left_len: u16 = left.len().try_into().ok()?;
                let right_len: u16 = right.len().try_into().ok()?;

                bytes.extend_from_slice(&left_len.to_be_bytes());
                bytes.extend_from_slice(&right_len.to_be_bytes());

                bytes.extend_from_slice(left.as_bytes());
                bytes.extend_from_slice(right.as_bytes());

                let value_bytes = C::bytes_encode(&value)?;
                bytes.extend_from_slice(&value_bytes[..]);

                Some(Cow::Owned(bytes))
            }
            None => {
                bytes.push(0);
                let value_bytes = C::bytes_encode(&value)?;
                bytes.extend_from_slice(&value_bytes[..]);
                Some(Cow::Owned(bytes))
            }
        }
    }
}

/// Tries to split a slice in half at the given middle point,
/// `None` if the slice is too short.
fn try_split_at(slice: &[u8], mid: usize) -> Option<(&[u8], &[u8])> {
    if slice.len() >= mid {
        Some(slice.split_at(mid))
    } else {
        None
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
        let bounds = Some(("abc", "def"));
        let docids: RoaringBitmap = (0..100).chain(3500..4398).collect();
        let key = (bounds, docids.clone());
        let bytes =
            FacetStringZeroBoundsValueCodec::<CboRoaringBitmapCodec>::bytes_encode(&key).unwrap();
        let (out_bounds, out_docids) =
            FacetStringZeroBoundsValueCodec::<CboRoaringBitmapCodec>::bytes_decode(&bytes).unwrap();
        assert_eq!((out_bounds, out_docids), (bounds, docids));
    }

    #[test]
    fn deserialize_unit() {
        let bounds = Some(("abc", "def"));
        let key = (bounds, ());
        let bytes = FacetStringZeroBoundsValueCodec::<Unit>::bytes_encode(&key).unwrap();
        let (out_bounds, out_unit) =
            FacetStringZeroBoundsValueCodec::<Unit>::bytes_decode(&bytes).unwrap();
        assert_eq!((out_bounds, out_unit), (bounds, ()));
    }
}

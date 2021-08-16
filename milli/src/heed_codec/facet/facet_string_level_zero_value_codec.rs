use std::borrow::Cow;
use std::convert::TryInto;
use std::{marker, str};

use crate::error::SerializationError;
use crate::heed_codec::RoaringBitmapCodec;
use crate::{try_split_array_at, try_split_at, Result};
pub type FacetStringLevelZeroValueCodec = StringValueCodec<RoaringBitmapCodec>;

/// A codec that encodes a string in front of a value.
///
/// The usecase is for the facet string levels algorithm where we must know the
/// original string of a normalized facet value, the original values are stored
/// in the value to not break the lexicographical ordering of the LMDB keys.
pub struct StringValueCodec<C>(marker::PhantomData<C>);

impl<'a, C> heed::BytesDecode<'a> for StringValueCodec<C>
where
    C: heed::BytesDecode<'a>,
{
    type DItem = (&'a str, C::DItem);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (string, bytes) = decode_prefix_string(bytes)?;

        C::bytes_decode(bytes).map(|item| (string, item))
    }
}

impl<'a, C> heed::BytesEncode<'a> for StringValueCodec<C>
where
    C: heed::BytesEncode<'a>,
{
    type EItem = (&'a str, C::EItem);

    fn bytes_encode((string, value): &'a Self::EItem) -> Option<Cow<[u8]>> {
        let value_bytes = C::bytes_encode(&value)?;

        let mut bytes = Vec::with_capacity(2 + string.len() + value_bytes.len());
        encode_prefix_string(string, &mut bytes).ok()?;
        bytes.extend_from_slice(&value_bytes[..]);

        Some(Cow::Owned(bytes))
    }
}

pub fn decode_prefix_string(value: &[u8]) -> Option<(&str, &[u8])> {
    let (original_length_bytes, bytes) = try_split_array_at(value)?;
    let original_length = u16::from_be_bytes(original_length_bytes) as usize;
    let (string, bytes) = try_split_at(bytes, original_length)?;
    let string = str::from_utf8(string).ok()?;

    Some((string, bytes))
}

pub fn encode_prefix_string(string: &str, buffer: &mut Vec<u8>) -> Result<()> {
    let string_len: u16 =
        string.len().try_into().map_err(|_| SerializationError::InvalidNumberSerialization)?;
    buffer.extend_from_slice(&string_len.to_be_bytes());
    buffer.extend_from_slice(string.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use heed::types::Unit;
    use heed::{BytesDecode, BytesEncode};
    use roaring::RoaringBitmap;

    use super::*;

    #[test]
    fn deserialize_roaring_bitmaps() {
        let string = "abc";
        let docids: RoaringBitmap = (0..100).chain(3500..4398).collect();
        let key = (string, docids.clone());
        let bytes = StringValueCodec::<RoaringBitmapCodec>::bytes_encode(&key).unwrap();
        let (out_string, out_docids) =
            StringValueCodec::<RoaringBitmapCodec>::bytes_decode(&bytes).unwrap();
        assert_eq!((out_string, out_docids), (string, docids));
    }

    #[test]
    fn deserialize_unit() {
        let string = "def";
        let key = (string, ());
        let bytes = StringValueCodec::<Unit>::bytes_encode(&key).unwrap();
        let (out_string, out_unit) = StringValueCodec::<Unit>::bytes_decode(&bytes).unwrap();
        assert_eq!((out_string, out_unit), (string, ()));
    }
}

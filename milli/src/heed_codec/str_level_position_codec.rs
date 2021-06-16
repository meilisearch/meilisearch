use std::borrow::Cow;
use std::convert::{TryFrom, TryInto};
use std::mem::size_of;
use std::str;

use crate::TreeLevel;

pub struct StrLevelPositionCodec;

impl<'a> heed::BytesDecode<'a> for StrLevelPositionCodec {
    type DItem = (&'a str, TreeLevel, u32, u32);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let footer_len = size_of::<u8>() + size_of::<u32>() * 2;

        if bytes.len() < footer_len {
            return None;
        }

        let (word, bytes) = bytes.split_at(bytes.len() - footer_len);
        let word = str::from_utf8(word).ok()?;

        let (level, bytes) = bytes.split_first()?;
        let left = bytes[..4].try_into().map(u32::from_be_bytes).ok()?;
        let right = bytes[4..].try_into().map(u32::from_be_bytes).ok()?;
        let level = TreeLevel::try_from(*level).ok()?;

        Some((word, level, left, right))
    }
}

impl<'a> heed::BytesEncode<'a> for StrLevelPositionCodec {
    type EItem = (&'a str, TreeLevel, u32, u32);

    fn bytes_encode((word, level, left, right): &Self::EItem) -> Option<Cow<[u8]>> {
        let left = left.to_be_bytes();
        let right = right.to_be_bytes();

        let mut bytes = Vec::with_capacity(word.len() + 1 + left.len() + right.len());
        bytes.extend_from_slice(word.as_bytes());
        bytes.push((*level).into());
        bytes.extend_from_slice(&left[..]);
        bytes.extend_from_slice(&right[..]);

        Some(Cow::Owned(bytes))
    }
}

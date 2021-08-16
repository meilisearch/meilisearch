use std::borrow::Cow;
use std::io;
use std::result::Result as StdResult;

use roaring::RoaringBitmap;

use super::read_u32_ne_bytes;
use crate::heed_codec::facet::{decode_prefix_string, encode_prefix_string};
use crate::heed_codec::CboRoaringBitmapCodec;
use crate::Result;

pub type MergeFn = for<'a> fn(&[u8], &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>>;

pub fn concat_u32s_array<'a>(_key: &[u8], values: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
    if values.len() == 1 {
        Ok(values[0].clone())
    } else {
        let capacity = values.iter().map(|v| v.len()).sum::<usize>();
        let mut output = Vec::with_capacity(capacity);
        values.iter().for_each(|integers| output.extend_from_slice(integers));
        Ok(Cow::Owned(output))
    }
}

pub fn roaring_bitmap_from_u32s_array(slice: &[u8]) -> RoaringBitmap {
    read_u32_ne_bytes(slice).collect()
}

pub fn serialize_roaring_bitmap(bitmap: &RoaringBitmap, buffer: &mut Vec<u8>) -> io::Result<()> {
    buffer.clear();
    buffer.reserve(bitmap.serialized_size());
    bitmap.serialize_into(buffer)
}

pub fn merge_roaring_bitmaps<'a>(_key: &[u8], values: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
    if values.len() == 1 {
        Ok(values[0].clone())
    } else {
        let merged = values
            .iter()
            .map(AsRef::as_ref)
            .map(RoaringBitmap::deserialize_from)
            .map(StdResult::unwrap)
            .reduce(|a, b| a | b)
            .unwrap();
        let mut buffer = Vec::new();
        serialize_roaring_bitmap(&merged, &mut buffer)?;
        Ok(Cow::Owned(buffer))
    }
}

pub fn keep_first_prefix_value_merge_roaring_bitmaps<'a>(
    _key: &[u8],
    values: &[Cow<'a, [u8]>],
) -> Result<Cow<'a, [u8]>> {
    if values.len() == 1 {
        Ok(values[0].clone())
    } else {
        let original = decode_prefix_string(&values[0]).unwrap().0;
        let merged_bitmaps = values
            .iter()
            .map(AsRef::as_ref)
            .map(decode_prefix_string)
            .map(Option::unwrap)
            .map(|(_, bitmap_bytes)| bitmap_bytes)
            .map(RoaringBitmap::deserialize_from)
            .map(StdResult::unwrap)
            .reduce(|a, b| a | b)
            .unwrap();

        let cap = std::mem::size_of::<u16>() + original.len() + merged_bitmaps.serialized_size();
        let mut buffer = Vec::with_capacity(cap);
        encode_prefix_string(original, &mut buffer)?;
        merged_bitmaps.serialize_into(&mut buffer)?;
        Ok(Cow::Owned(buffer))
    }
}

pub fn keep_first<'a>(_key: &[u8], values: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
    Ok(values[0].clone())
}

/// Only the last value associated with an id is kept.
pub fn keep_latest_obkv<'a>(_key: &[u8], obkvs: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
    Ok(obkvs.last().unwrap().clone())
}

/// Merge all the obks in the order we see them.
pub fn merge_obkvs<'a>(_key: &[u8], obkvs: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
    Ok(obkvs
        .into_iter()
        .cloned()
        .reduce(|acc, current| {
            let first = obkv::KvReader::new(&acc);
            let second = obkv::KvReader::new(&current);
            let mut buffer = Vec::new();
            merge_two_obkvs(first, second, &mut buffer);
            Cow::from(buffer)
        })
        .unwrap())
}

pub fn merge_two_obkvs(base: obkv::KvReaderU16, update: obkv::KvReaderU16, buffer: &mut Vec<u8>) {
    use itertools::merge_join_by;
    use itertools::EitherOrBoth::{Both, Left, Right};

    buffer.clear();

    let mut writer = obkv::KvWriter::new(buffer);
    for eob in merge_join_by(base.iter(), update.iter(), |(b, _), (u, _)| b.cmp(u)) {
        match eob {
            Both(_, (k, v)) | Left((k, v)) | Right((k, v)) => writer.insert(k, v).unwrap(),
        }
    }

    writer.finish().unwrap();
}

pub fn merge_cbo_roaring_bitmaps<'a>(
    _key: &[u8],
    values: &[Cow<'a, [u8]>],
) -> Result<Cow<'a, [u8]>> {
    match values.split_first().unwrap() {
        (head, []) => Ok(head.clone()),
        (head, tail) => {
            let mut head = CboRoaringBitmapCodec::deserialize_from(&head[..])?;

            for value in tail {
                head |= CboRoaringBitmapCodec::deserialize_from(&value[..])?;
            }

            let mut vec = Vec::new();
            CboRoaringBitmapCodec::serialize_into(&head, &mut vec);
            Ok(Cow::from(vec))
        }
    }
}

// /// Uses the FacetStringLevelZeroValueCodec to merge the values.
// pub fn tuple_string_cbo_roaring_bitmap_merge<'a>(
//     _key: &[u8],
//     values: &[Cow<[u8]>],
// ) -> Result<Cow<'a, [u8]>> {
//     let (head, tail) = values.split_first().unwrap();
//     let (head_string, mut head_rb) = FacetStringLevelZeroValueCodec::bytes_decode(&head[..])
//         .ok_or(SerializationError::Decoding { db_name: None })?;

//     for value in tail {
//         let (_string, rb) = FacetStringLevelZeroValueCodec::bytes_decode(&value[..])
//             .ok_or(SerializationError::Decoding { db_name: None })?;
//         head_rb |= rb;
//     }

//     FacetStringLevelZeroValueCodec::bytes_encode(&(head_string, head_rb))
//         .map(|cow| cow.into_owned())
//         .ok_or(SerializationError::Encoding { db_name: None })
//         .map_err(Into::into)
// }

// pub fn cbo_roaring_bitmap_merge<'a>(_key: &[u8], values: &[Cow<[u8]>]) -> Result<Cow<'a, [u8]>> {
//     let (head, tail) = values.split_first().unwrap();
//     let mut head = CboRoaringBitmapCodec::deserialize_from(&head[..])?;

//     for value in tail {
//         head |= CboRoaringBitmapCodec::deserialize_from(&value[..])?;
//     }

//     let mut vec = Vec::new();
//     CboRoaringBitmapCodec::serialize_into(&head, &mut vec);
//     Ok(vec)
// }

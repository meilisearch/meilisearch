use std::borrow::Cow;
use std::io;
use std::result::Result as StdResult;

use roaring::RoaringBitmap;

use super::read_u32_ne_bytes;
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
        .iter()
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
    if values.len() == 1 {
        Ok(values[0].clone())
    } else {
        let mut vec = Vec::new();
        CboRoaringBitmapCodec::merge_into(values, &mut vec)?;
        Ok(Cow::from(vec))
    }
}

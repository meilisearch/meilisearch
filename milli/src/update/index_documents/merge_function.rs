use std::borrow::Cow;

use fst::IntoStreamer;
use roaring::RoaringBitmap;

use crate::heed_codec::CboRoaringBitmapCodec;

/// Only the last value associated with an id is kept.
pub fn keep_latest_obkv(_key: &[u8], obkvs: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    Ok(obkvs.last().unwrap().clone().into_owned())
}

/// Merge all the obks in the order we see them.
pub fn merge_obkvs(_key: &[u8], obkvs: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    let mut iter = obkvs.iter();
    let first = iter.next().map(|b| b.clone().into_owned()).unwrap();
    Ok(iter.fold(first, |acc, current| {
        let first = obkv::KvReader::new(&acc);
        let second = obkv::KvReader::new(current);
        let mut buffer = Vec::new();
        merge_two_obkvs(first, second, &mut buffer);
        buffer
    }))
}

// Union of multiple FSTs
pub fn fst_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    let fsts = values.iter().map(fst::Set::new).collect::<Result<Vec<_>, _>>()?;
    let op_builder: fst::set::OpBuilder = fsts.iter().map(|fst| fst.into_stream()).collect();
    let op = op_builder.r#union();

    let mut build = fst::SetBuilder::memory();
    build.extend_stream(op.into_stream()).unwrap();
    Ok(build.into_inner().unwrap())
}

pub fn keep_first(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    Ok(values.first().unwrap().to_vec())
}

pub fn merge_two_obkvs(base: obkv::KvReader, update: obkv::KvReader, buffer: &mut Vec<u8>) {
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

pub fn roaring_bitmap_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    let (head, tail) = values.split_first().unwrap();
    let mut head = RoaringBitmap::deserialize_from(&head[..])?;

    for value in tail {
        let bitmap = RoaringBitmap::deserialize_from(&value[..])?;
        head.union_with(&bitmap);
    }

    let mut vec = Vec::with_capacity(head.serialized_size());
    head.serialize_into(&mut vec)?;
    Ok(vec)
}

pub fn cbo_roaring_bitmap_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    let (head, tail) = values.split_first().unwrap();
    let mut head = CboRoaringBitmapCodec::deserialize_from(&head[..])?;

    for value in tail {
        let bitmap = CboRoaringBitmapCodec::deserialize_from(&value[..])?;
        head.union_with(&bitmap);
    }

    let mut vec = Vec::new();
    CboRoaringBitmapCodec::serialize_into(&head, &mut vec);
    Ok(vec)
}

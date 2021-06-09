use std::borrow::Cow;

use anyhow::bail;
use bstr::ByteSlice as _;
use fst::IntoStreamer;
use roaring::RoaringBitmap;

use crate::heed_codec::CboRoaringBitmapCodec;

// Union of multiple FSTs
pub fn fst_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    let fsts = values.iter().map(fst::Set::new).collect::<Result<Vec<_>, _>>()?;
    let op_builder: fst::set::OpBuilder = fsts.iter().map(|fst| fst.into_stream()).collect();
    let op = op_builder.r#union();

    let mut build = fst::SetBuilder::memory();
    build.extend_stream(op.into_stream()).unwrap();
    Ok(build.into_inner().unwrap())
}

pub fn docid_word_positions_merge(key: &[u8], _values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    panic!("merging docid word positions is an error ({:?})", key.as_bstr())
}

pub fn keep_first(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    Ok(values.first().unwrap().to_vec())
}

pub fn documents_merge(key: &[u8], _values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    bail!("merging documents is an error ({:?})", key.as_bstr())
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

use std::borrow::Cow;

use anyhow::{bail, ensure, Context};
use bstr::ByteSlice as _;
use fst::IntoStreamer;
use roaring::RoaringBitmap;

use crate::heed_codec::CboRoaringBitmapCodec;

const WORDS_FST_KEY: &[u8] = crate::index::WORDS_FST_KEY.as_bytes();
const FIELDS_IDS_MAP_KEY: &[u8] = crate::index::FIELDS_IDS_MAP_KEY.as_bytes();
const DOCUMENTS_IDS_KEY: &[u8] = crate::index::DOCUMENTS_IDS_KEY.as_bytes();

pub fn main_merge(key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    match key {
        WORDS_FST_KEY => {
            let fsts: Vec<_> = values.iter().map(|v| fst::Set::new(v).unwrap()).collect();

            // Union of the FSTs
            let mut op = fst::set::OpBuilder::new();
            fsts.iter().for_each(|fst| op.push(fst.into_stream()));
            let op = op.r#union();

            let mut build = fst::SetBuilder::memory();
            build.extend_stream(op.into_stream()).unwrap();
            Ok(build.into_inner().unwrap())
        },
        FIELDS_IDS_MAP_KEY => {
            ensure!(values.windows(2).all(|vs| vs[0] == vs[1]), "fields ids map doesn't match");
            Ok(values[0].to_vec())
        },
        DOCUMENTS_IDS_KEY => roaring_bitmap_merge(values),
        otherwise => bail!("wut {:?}", otherwise),
    }
}

pub fn word_docids_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    roaring_bitmap_merge(values)
}

pub fn docid_word_positions_merge(key: &[u8], _values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    bail!("merging docid word positions is an error ({:?})", key.as_bstr())
}

pub fn field_id_docid_facet_values_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    let first = values.first().context("no value to merge")?;
    ensure!(values.iter().all(|v| v == first), "invalid field id docid facet value merging");
    Ok(first.to_vec())
}

pub fn words_pairs_proximities_docids_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    cbo_roaring_bitmap_merge(values)
}

pub fn word_prefix_level_positions_docids_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    cbo_roaring_bitmap_merge(values)
}

pub fn word_level_position_docids_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    cbo_roaring_bitmap_merge(values)
}

pub fn facet_field_value_docids_merge(_key: &[u8], values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    cbo_roaring_bitmap_merge(values)
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

fn roaring_bitmap_merge(values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
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

fn cbo_roaring_bitmap_merge(values: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    let (head, tail) = values.split_first().unwrap();
    let mut head = CboRoaringBitmapCodec::deserialize_from(&head[..])?;

    for value in tail {
        let bitmap = CboRoaringBitmapCodec::deserialize_from(&value[..])?;
        head.union_with(&bitmap);
    }

    let mut vec = Vec::new();
    CboRoaringBitmapCodec::serialize_into(&head, &mut vec)?;
    Ok(vec)
}

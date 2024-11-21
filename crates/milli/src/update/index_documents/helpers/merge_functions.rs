use std::borrow::Cow;
use std::collections::BTreeSet;
use std::io;
use std::result::Result as StdResult;

use either::Either;
use grenad::MergeFunction;
use roaring::RoaringBitmap;

use crate::heed_codec::CboRoaringBitmapCodec;
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::transform::Operation;
use crate::Result;

pub type EitherObkvMerge =
    Either<ObkvsKeepLastAdditionMergeDeletions, ObkvsMergeAdditionsAndDeletions>;

pub fn serialize_roaring_bitmap(bitmap: &RoaringBitmap, buffer: &mut Vec<u8>) -> io::Result<()> {
    buffer.clear();
    buffer.reserve(bitmap.serialized_size());
    bitmap.serialize_into(buffer)
}

pub struct MergeRoaringBitmaps;

impl MergeFunction for MergeRoaringBitmaps {
    type Error = crate::Error;

    fn merge<'a>(&self, _key: &[u8], values: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
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
}

pub struct KeepFirst;

impl MergeFunction for KeepFirst {
    type Error = crate::Error;

    fn merge<'a>(&self, _key: &[u8], values: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
        Ok(values[0].clone())
    }
}

/// Only the last value associated with an id is kept.
pub struct KeepLatestObkv;

impl MergeFunction for KeepLatestObkv {
    type Error = crate::Error;

    fn merge<'a>(&self, _key: &[u8], obkvs: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
        Ok(obkvs.last().unwrap().clone())
    }
}

pub fn merge_two_del_add_obkvs(
    base: &obkv::KvReaderU16,
    update: &obkv::KvReaderU16,
    merge_additions: bool,
    buffer: &mut Vec<u8>,
) {
    use itertools::merge_join_by;
    use itertools::EitherOrBoth::{Both, Left, Right};

    buffer.clear();

    let mut writer = obkv::KvWriter::new(buffer);
    let mut value_buffer = Vec::new();
    for eob in merge_join_by(base.iter(), update.iter(), |(b, _), (u, _)| b.cmp(u)) {
        match eob {
            Left((k, v)) => {
                if merge_additions {
                    writer.insert(k, v).unwrap()
                } else {
                    // If merge_additions is false, recreate an obkv keeping the deletions only.
                    value_buffer.clear();
                    let mut value_writer = KvWriterDelAdd::new(&mut value_buffer);
                    let base_reader = KvReaderDelAdd::from_slice(v);

                    if let Some(deletion) = base_reader.get(DelAdd::Deletion) {
                        value_writer.insert(DelAdd::Deletion, deletion).unwrap();
                        value_writer.finish().unwrap();
                        writer.insert(k, &value_buffer).unwrap()
                    }
                }
            }
            Right((k, v)) => writer.insert(k, v).unwrap(),
            Both((k, base), (_, update)) => {
                // merge deletions and additions.
                value_buffer.clear();
                let mut value_writer = KvWriterDelAdd::new(&mut value_buffer);
                let base_reader = KvReaderDelAdd::from_slice(base);
                let update_reader = KvReaderDelAdd::from_slice(update);

                // keep newest deletion.
                if let Some(deletion) = update_reader
                    .get(DelAdd::Deletion)
                    .or_else(|| base_reader.get(DelAdd::Deletion))
                {
                    value_writer.insert(DelAdd::Deletion, deletion).unwrap();
                }

                // keep base addition only if merge_additions is true.
                let base_addition =
                    merge_additions.then(|| base_reader.get(DelAdd::Addition)).flatten();
                // keep newest addition.
                // TODO use or_else
                if let Some(addition) = update_reader.get(DelAdd::Addition).or(base_addition) {
                    value_writer.insert(DelAdd::Addition, addition).unwrap();
                }

                value_writer.finish().unwrap();
                writer.insert(k, &value_buffer).unwrap()
            }
        }
    }

    writer.finish().unwrap();
}

/// Merge all the obkvs from the newest to the oldest.
fn inner_merge_del_add_obkvs<'a>(
    obkvs: &[Cow<'a, [u8]>],
    merge_additions: bool,
) -> Result<Cow<'a, [u8]>> {
    // pop the newest operation from the list.
    let (newest, obkvs) = obkvs.split_last().unwrap();
    // keep the operation type for the returned value.
    let newest_operation_type = newest[0];

    // treat the newest obkv as the starting point of the merge.
    let mut acc_operation_type = newest_operation_type;
    let mut acc = newest[1..].to_vec();
    let mut buffer = Vec::new();
    // reverse iter from the most recent to the oldest.
    for current in obkvs.iter().rev() {
        // if in the previous iteration there was a complete deletion,
        // stop the merge process.
        if acc_operation_type == Operation::Deletion as u8 {
            break;
        }

        let newest = obkv::KvReader::from_slice(&acc);
        let oldest = obkv::KvReader::from_slice(&current[1..]);
        merge_two_del_add_obkvs(oldest, newest, merge_additions, &mut buffer);

        // we want the result of the merge into our accumulator.
        std::mem::swap(&mut acc, &mut buffer);
        acc_operation_type = current[0];
    }

    acc.insert(0, newest_operation_type);
    Ok(Cow::from(acc))
}

/// Merge all the obkvs from the newest to the oldest.
#[derive(Copy, Clone)]
pub struct ObkvsMergeAdditionsAndDeletions;

impl MergeFunction for ObkvsMergeAdditionsAndDeletions {
    type Error = crate::Error;

    fn merge<'a>(&self, _key: &[u8], obkvs: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
        inner_merge_del_add_obkvs(obkvs, true)
    }
}

/// Merge all the obkvs deletions from the newest to the oldest and keep only the newest additions.
#[derive(Copy, Clone)]
pub struct ObkvsKeepLastAdditionMergeDeletions;

impl MergeFunction for ObkvsKeepLastAdditionMergeDeletions {
    type Error = crate::Error;

    fn merge<'a>(&self, _key: &[u8], obkvs: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
        inner_merge_del_add_obkvs(obkvs, false)
    }
}

/// Do a union of all the CboRoaringBitmaps in the values.
pub struct MergeCboRoaringBitmaps;

impl MergeFunction for MergeCboRoaringBitmaps {
    type Error = crate::Error;

    fn merge<'a>(&self, _key: &[u8], values: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
        if values.len() == 1 {
            Ok(values[0].clone())
        } else {
            let mut vec = Vec::new();
            CboRoaringBitmapCodec::merge_into(values, &mut vec)?;
            Ok(Cow::from(vec))
        }
    }
}

/// Do a union of CboRoaringBitmaps on both sides of a DelAdd obkv
/// separately and outputs a new DelAdd with both unions.
pub struct MergeDeladdCboRoaringBitmaps;

impl MergeFunction for MergeDeladdCboRoaringBitmaps {
    type Error = crate::Error;

    fn merge<'a>(&self, _key: &[u8], values: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
        if values.len() == 1 {
            Ok(values[0].clone())
        } else {
            // Retrieve the bitmaps from both sides
            let mut del_bitmaps_bytes = Vec::new();
            let mut add_bitmaps_bytes = Vec::new();
            for value in values {
                let obkv = KvReaderDelAdd::from_slice(value);
                if let Some(bitmap_bytes) = obkv.get(DelAdd::Deletion) {
                    del_bitmaps_bytes.push(bitmap_bytes);
                }
                if let Some(bitmap_bytes) = obkv.get(DelAdd::Addition) {
                    add_bitmaps_bytes.push(bitmap_bytes);
                }
            }

            let mut output_deladd_obkv = KvWriterDelAdd::memory();
            let mut buffer = Vec::new();
            CboRoaringBitmapCodec::merge_into(del_bitmaps_bytes, &mut buffer)?;
            output_deladd_obkv.insert(DelAdd::Deletion, &buffer)?;
            buffer.clear();
            CboRoaringBitmapCodec::merge_into(add_bitmaps_bytes, &mut buffer)?;
            output_deladd_obkv.insert(DelAdd::Addition, &buffer)?;
            output_deladd_obkv.into_inner().map(Cow::from).map_err(Into::into)
        }
    }
}

/// A function that merges a DelAdd of bitmao into an already existing bitmap.
///
/// The first argument is the DelAdd obkv of CboRoaringBitmaps and
/// the second one is the CboRoaringBitmap to merge into.
pub fn merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap<'a>(
    deladd_obkv: &[u8],
    previous: &[u8],
    buffer: &'a mut Vec<u8>,
) -> Result<Option<&'a [u8]>> {
    Ok(CboRoaringBitmapCodec::merge_deladd_into(
        KvReaderDelAdd::from_slice(deladd_obkv),
        previous,
        buffer,
    )?)
}

/// Do a union of BtreeSet on both sides of a DelAdd obkv
/// separately and outputs a new DelAdd with both unions.
pub struct MergeDeladdBtreesetString;

impl MergeFunction for MergeDeladdBtreesetString {
    type Error = crate::Error;

    fn merge<'a>(&self, _key: &[u8], values: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
        if values.len() == 1 {
            Ok(values[0].clone())
        } else {
            // Retrieve the bitmaps from both sides
            let mut del_set = BTreeSet::new();
            let mut add_set = BTreeSet::new();
            for value in values {
                let obkv = KvReaderDelAdd::from_slice(value);
                if let Some(bytes) = obkv.get(DelAdd::Deletion) {
                    let set = serde_json::from_slice::<BTreeSet<String>>(bytes).unwrap();
                    for value in set {
                        del_set.insert(value);
                    }
                }
                if let Some(bytes) = obkv.get(DelAdd::Addition) {
                    let set = serde_json::from_slice::<BTreeSet<String>>(bytes).unwrap();
                    for value in set {
                        add_set.insert(value);
                    }
                }
            }

            let mut output_deladd_obkv = KvWriterDelAdd::memory();
            let del = serde_json::to_vec(&del_set).unwrap();
            output_deladd_obkv.insert(DelAdd::Deletion, &del)?;
            let add = serde_json::to_vec(&add_set).unwrap();
            output_deladd_obkv.insert(DelAdd::Addition, &add)?;
            output_deladd_obkv.into_inner().map(Cow::from).map_err(Into::into)
        }
    }
}

/// Used when trying to merge readers, but you don't actually care about the values.
pub struct MergeIgnoreValues;

impl MergeFunction for MergeIgnoreValues {
    type Error = crate::Error;

    fn merge<'a>(
        &self,
        _key: &[u8],
        _values: &[Cow<'a, [u8]>],
    ) -> std::result::Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Owned(Vec::new()))
    }
}

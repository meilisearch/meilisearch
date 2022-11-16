mod clonable_mmap;
mod grenad_helpers;
mod merge_functions;

use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};

pub use clonable_mmap::{ClonableMmap, CursorClonableMmap};
use fst::{IntoStreamer, Streamer};
pub use grenad_helpers::{
    as_cloneable_grenad, create_sorter, create_writer, grenad_obkv_into_chunks,
    merge_ignore_values, sorter_into_lmdb_database, sorter_into_reader, writer_into_reader,
    GrenadParameters, MergeableReader,
};
pub use merge_functions::{
    concat_u32s_array, keep_first, keep_latest_obkv, merge_cbo_roaring_bitmaps, merge_obkvs,
    merge_roaring_bitmaps, merge_two_obkvs, roaring_bitmap_from_u32s_array,
    serialize_roaring_bitmap, MergeFn,
};

/// The maximum length a LMDB key can be.
///
/// Note that the actual allowed length is a little bit higher, but
/// we keep a margin of safety.
const MAX_LMDB_KEY_LENGTH: usize = 500;

/// The maximum length a field value can be when inserted in an LMDB key.
///
/// This number is determined by the keys of the different facet databases
/// and adding a margin of safety.
pub const MAX_FACET_VALUE_LENGTH: usize = MAX_LMDB_KEY_LENGTH - 20;

/// The maximum length a word can be
pub const MAX_WORD_LENGTH: usize = MAX_LMDB_KEY_LENGTH / 2;

pub fn valid_lmdb_key(key: impl AsRef<[u8]>) -> bool {
    key.as_ref().len() <= MAX_WORD_LENGTH * 2 && !key.as_ref().is_empty()
}

/// Divides one slice into two at an index, returns `None` if mid is out of bounds.
pub fn try_split_at<T>(slice: &[T], mid: usize) -> Option<(&[T], &[T])> {
    if mid <= slice.len() {
        Some(slice.split_at(mid))
    } else {
        None
    }
}

/// Divides one slice into an array and the tail at an index,
/// returns `None` if `N` is out of bounds.
pub fn try_split_array_at<T, const N: usize>(slice: &[T]) -> Option<([T; N], &[T])>
where
    [T; N]: for<'a> TryFrom<&'a [T]>,
{
    let (head, tail) = try_split_at(slice, N)?;
    let head = head.try_into().ok()?;
    Some((head, tail))
}

pub fn read_u32_ne_bytes(bytes: &[u8]) -> impl Iterator<Item = u32> + '_ {
    bytes.chunks_exact(4).flat_map(TryInto::try_into).map(u32::from_ne_bytes)
}

/// Converts an fst Stream into an HashSet of Strings.
pub fn fst_stream_into_hashset<'f, I, S>(stream: I) -> HashSet<Vec<u8>>
where
    I: for<'a> IntoStreamer<'a, Into = S, Item = &'a [u8]>,
    S: 'f + for<'a> Streamer<'a, Item = &'a [u8]>,
{
    let mut hashset = HashSet::new();
    let mut stream = stream.into_stream();
    while let Some(value) = stream.next() {
        hashset.insert(value.to_owned());
    }
    hashset
}

// Converts an fst Stream into a Vec of Strings.
pub fn fst_stream_into_vec<'f, I, S>(stream: I) -> Vec<String>
where
    I: for<'a> IntoStreamer<'a, Into = S, Item = &'a [u8]>,
    S: 'f + for<'a> Streamer<'a, Item = &'a [u8]>,
{
    let mut strings = Vec::new();
    let mut stream = stream.into_stream();
    while let Some(word) = stream.next() {
        let s = std::str::from_utf8(word).unwrap();
        strings.push(s.to_owned());
    }
    strings
}

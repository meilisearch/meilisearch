mod clonable_mmap;
mod grenad_helpers;
mod merge_functions;

use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};

pub use clonable_mmap::{ClonableMmap, CursorClonableMmap};
use fst::{IntoStreamer, Streamer};
pub use grenad_helpers::*;
pub use merge_functions::*;

use crate::MAX_LMDB_KEY_LENGTH;

pub fn valid_lmdb_key(key: impl AsRef<[u8]>) -> bool {
    key.as_ref().len() <= MAX_LMDB_KEY_LENGTH - 3 && !key.as_ref().is_empty()
}

pub fn valid_facet_value(facet_value: impl AsRef<[u8]>) -> bool {
    facet_value.as_ref().len() <= MAX_LMDB_KEY_LENGTH - 3 && !facet_value.as_ref().is_empty()
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

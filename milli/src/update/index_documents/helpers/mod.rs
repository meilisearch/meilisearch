mod clonable_mmap;
mod grenad_helpers;
mod merge_functions;

use std::convert::{TryFrom, TryInto};

pub use clonable_mmap::{ClonableMmap, CursorClonableMmap};
pub use grenad_helpers::{
    create_sorter, create_writer, grenad_obkv_into_chunks, into_clonable_grenad, merge_readers,
    sorter_into_lmdb_database, sorter_into_reader, write_into_lmdb_database, writer_into_reader,
    GrenadParameters,
};
pub use merge_functions::{
    concat_u32s_array, keep_first, keep_first_prefix_value_merge_roaring_bitmaps, keep_latest_obkv,
    merge_cbo_roaring_bitmaps, merge_obkvs, merge_roaring_bitmaps, merge_two_obkvs,
    roaring_bitmap_from_u32s_array, serialize_roaring_bitmap, MergeFn,
};

pub fn valid_lmdb_key(key: impl AsRef<[u8]>) -> bool {
    key.as_ref().len() <= 511
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

// pub fn pretty_thousands<A: Borrow<T>, T: fmt::Display>(number: A) -> String {
//     thousands::Separable::separate_with_spaces(number.borrow())
// }

pub fn read_u32_ne_bytes(bytes: &[u8]) -> impl Iterator<Item = u32> + '_ {
    bytes.chunks_exact(4).flat_map(TryInto::try_into).map(u32::from_ne_bytes)
}

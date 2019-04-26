mod doc_indexes;
mod shared_data;

use std::slice::from_raw_parts;
use std::mem::size_of;

pub use self::doc_indexes::{DocIndexes, DocIndexesBuilder};
pub use self::shared_data::SharedData;

unsafe fn into_u8_slice<T: Sized>(slice: &[T]) -> &[u8] {
    let ptr = slice.as_ptr() as *const u8;
    let len = slice.len() * size_of::<T>();
    from_raw_parts(ptr, len)
}

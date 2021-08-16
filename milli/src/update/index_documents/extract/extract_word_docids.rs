use std::fs::File;
use std::io;
use std::iter::FromIterator;

use roaring::RoaringBitmap;

use super::helpers::{
    create_sorter, merge_roaring_bitmaps, serialize_roaring_bitmap, sorter_into_reader,
    try_split_array_at, GrenadParameters,
};
use crate::Result;

/// Extracts the word and the documents ids where this word appear.
///
/// Returns a grenad reader with the list of extracted words and
/// documents ids from the given chunk of docid word positions.
pub fn extract_word_docids<R: io::Read>(
    mut docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let max_memory = indexer.max_memory_by_thread();

    let mut word_docids_sorter = create_sorter(
        merge_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut value_buffer = Vec::new();
    while let Some((key, _value)) = docid_word_positions.next()? {
        let (document_id_bytes, word_bytes) = try_split_array_at(key).unwrap();
        let document_id = u32::from_be_bytes(document_id_bytes);

        let bitmap = RoaringBitmap::from_iter(Some(document_id));
        serialize_roaring_bitmap(&bitmap, &mut value_buffer)?;
        word_docids_sorter.insert(word_bytes, &value_buffer)?;
    }

    sorter_into_reader(word_docids_sorter, indexer)
}

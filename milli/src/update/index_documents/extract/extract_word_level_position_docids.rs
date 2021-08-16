use std::fs::File;
use std::io;

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, read_u32_ne_bytes, sorter_into_reader,
    try_split_array_at, GrenadParameters,
};
use crate::{DocumentId, Result};
/// Extracts the word positions and the documents ids where this word appear.
///
/// Returns a grenad reader with the list of extracted words at positions and
/// documents ids from the given chunk of docid word positions.
pub fn extract_word_level_position_docids<R: io::Read>(
    mut docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let max_memory = indexer.max_memory_by_thread();

    let mut word_level_position_docids_sorter = create_sorter(
        merge_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut key_buffer = Vec::new();
    while let Some((key, value)) = docid_word_positions.next()? {
        let (document_id_bytes, word_bytes) = try_split_array_at(key).unwrap();
        let document_id = DocumentId::from_be_bytes(document_id_bytes);

        for position in read_u32_ne_bytes(value) {
            key_buffer.clear();
            key_buffer.extend_from_slice(word_bytes);
            key_buffer.push(0); // tree level

            // Levels are composed of left and right bounds.
            key_buffer.extend_from_slice(&position.to_be_bytes());
            key_buffer.extend_from_slice(&position.to_be_bytes());

            word_level_position_docids_sorter.insert(&key_buffer, &document_id.to_ne_bytes())?;
        }
    }

    sorter_into_reader(word_level_position_docids_sorter, indexer)
}

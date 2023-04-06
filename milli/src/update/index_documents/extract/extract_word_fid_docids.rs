use std::fs::File;
use std::io;

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, read_u32_ne_bytes, sorter_into_reader,
    try_split_array_at, GrenadParameters,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::{relative_from_absolute_position, DocumentId, Result};

/// Extracts the word, field id, and the documents ids where this word appear at this field id.
#[logging_timer::time]
pub fn extract_word_fid_docids<R: io::Read + io::Seek>(
    docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let max_memory = indexer.max_memory_by_thread();

    let mut word_fid_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut key_buffer = Vec::new();
    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let (document_id_bytes, word_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = DocumentId::from_be_bytes(document_id_bytes);

        for position in read_u32_ne_bytes(value) {
            key_buffer.clear();
            key_buffer.extend_from_slice(word_bytes);
            let (fid, _) = relative_from_absolute_position(position);
            key_buffer.extend_from_slice(&fid.to_be_bytes());
            word_fid_docids_sorter.insert(&key_buffer, document_id.to_ne_bytes())?;
        }
    }

    let word_fid_docids_reader = sorter_into_reader(word_fid_docids_sorter, indexer)?;

    Ok(word_fid_docids_reader)
}

use std::fs::File;
use std::io;

use obkv::KvReaderU16;

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_reader, try_split_array_at,
    GrenadParameters,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::Result;

const MAX_COUNTED_WORDS: usize = 30;

/// Extracts the field id word count and the documents ids where
/// this field id with this amount of words appear.
///
/// Returns a grenad reader with the list of extracted field id word counts
/// and documents ids from the given chunk of docid word positions.
#[logging_timer::time]
pub fn extract_fid_word_count_docids<R: io::Read + io::Seek>(
    docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    puffin::profile_function!();

    let max_memory = indexer.max_memory_by_thread();

    let mut fid_word_count_docids_sorter = create_sorter(
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
        let (document_id_bytes, fid_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = u32::from_be_bytes(document_id_bytes);

        let word_count = KvReaderU16::new(&value).iter().take(MAX_COUNTED_WORDS + 1).count();
        if word_count <= MAX_COUNTED_WORDS {
            key_buffer.clear();
            key_buffer.extend_from_slice(fid_bytes);
            key_buffer.push(word_count as u8);
            fid_word_count_docids_sorter.insert(&key_buffer, document_id.to_ne_bytes())?;
        }
    }

    sorter_into_reader(fid_word_count_docids_sorter, indexer)
}

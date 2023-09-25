use std::collections::HashSet;
use std::fs::File;
use std::io;

use obkv::KvReaderU16;

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_reader, try_split_array_at,
    GrenadParameters,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::{bucketed_position, DocumentId, Result};

/// Extracts the word positions and the documents ids where this word appear.
///
/// Returns a grenad reader with the list of extracted words at positions and
/// documents ids from the given chunk of docid word positions.
#[logging_timer::time]
pub fn extract_word_position_docids<R: io::Read + io::Seek>(
    docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    puffin::profile_function!();

    let max_memory = indexer.max_memory_by_thread();

    let mut word_position_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut word_positions: HashSet<(u16, Vec<u8>)> = HashSet::new();
    let mut current_document_id: Option<u32> = None;
    let mut key_buffer = Vec::new();
    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let (document_id_bytes, _fid_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = DocumentId::from_be_bytes(document_id_bytes);

        if current_document_id.map_or(false, |id| document_id != id) {
            for (position, word_bytes) in word_positions.iter() {
                key_buffer.clear();
                key_buffer.extend_from_slice(word_bytes);
                key_buffer.push(0);
                key_buffer.extend_from_slice(&position.to_be_bytes());
                word_position_docids_sorter
                    .insert(&key_buffer, current_document_id.unwrap().to_ne_bytes())?;
            }
            word_positions.clear();
        }

        current_document_id = Some(document_id);

        for (position, word_bytes) in KvReaderU16::new(&value).iter() {
            let position = bucketed_position(position);
            word_positions.insert((position, word_bytes.to_vec()));
        }
    }

    if let Some(document_id) = current_document_id {
        for (position, word_bytes) in word_positions {
            key_buffer.clear();
            key_buffer.extend_from_slice(&word_bytes);
            key_buffer.push(0);
            key_buffer.extend_from_slice(&position.to_be_bytes());
            word_position_docids_sorter.insert(&key_buffer, document_id.to_ne_bytes())?;
        }
    }

    let word_position_docids_reader = sorter_into_reader(word_position_docids_sorter, indexer)?;

    Ok(word_position_docids_reader)
}

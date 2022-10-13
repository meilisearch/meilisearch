use std::collections::HashMap;
use std::fs::File;
use std::{cmp, io};

use grenad::Sorter;

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, read_u32_ne_bytes, sorter_into_reader,
    try_split_array_at, GrenadParameters, MergeFn,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::{relative_from_absolute_position, DocumentId, FieldId, Result};

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
    let max_memory = indexer.max_memory_by_thread();

    let mut fid_word_count_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    // This map is assumed to not consume a lot of memory.
    let mut document_fid_wordcount = HashMap::new();
    let mut current_document_id = None;

    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let (document_id_bytes, _word_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = u32::from_be_bytes(document_id_bytes);

        let curr_document_id = *current_document_id.get_or_insert(document_id);
        if curr_document_id != document_id {
            drain_document_fid_wordcount_into_sorter(
                &mut fid_word_count_docids_sorter,
                &mut document_fid_wordcount,
                curr_document_id,
            )?;
            current_document_id = Some(document_id);
        }

        for position in read_u32_ne_bytes(value) {
            let (field_id, position) = relative_from_absolute_position(position);
            let word_count = position as u32 + 1;

            let value = document_fid_wordcount.entry(field_id as FieldId).or_insert(0);
            *value = cmp::max(*value, word_count);
        }
    }

    if let Some(document_id) = current_document_id {
        // We must make sure that don't lose the current document field id
        // word count map if we break because we reached the end of the chunk.
        drain_document_fid_wordcount_into_sorter(
            &mut fid_word_count_docids_sorter,
            &mut document_fid_wordcount,
            document_id,
        )?;
    }

    sorter_into_reader(fid_word_count_docids_sorter, indexer)
}

fn drain_document_fid_wordcount_into_sorter(
    fid_word_count_docids_sorter: &mut Sorter<MergeFn>,
    document_fid_wordcount: &mut HashMap<FieldId, u32>,
    document_id: DocumentId,
) -> Result<()> {
    let mut key_buffer = Vec::new();

    for (fid, count) in document_fid_wordcount.drain() {
        if count <= 10 {
            key_buffer.clear();
            key_buffer.extend_from_slice(&fid.to_be_bytes());
            key_buffer.push(count as u8);

            fid_word_count_docids_sorter.insert(&key_buffer, document_id.to_ne_bytes())?;
        }
    }

    Ok(())
}

use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::iter::FromIterator;

use roaring::RoaringBitmap;

use super::helpers::{
    create_sorter, merge_roaring_bitmaps, serialize_roaring_bitmap, sorter_into_reader,
    try_split_array_at, GrenadParameters,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::update::index_documents::helpers::read_u32_ne_bytes;
use crate::{relative_from_absolute_position, FieldId, Result};

/// Extracts the word and the documents ids where this word appear.
///
/// Returns a grenad reader with the list of extracted words and
/// documents ids from the given chunk of docid word positions.
///
/// The first returned reader is the one for normal word_docids, and the second one is for
/// exact_word_docids
#[logging_timer::time]
pub fn extract_word_docids<R: io::Read + io::Seek>(
    docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
    exact_attributes: &HashSet<FieldId>,
) -> Result<(grenad::Reader<File>, grenad::Reader<File>)> {
    let max_memory = indexer.max_memory_by_thread();

    let mut word_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|x| x / 2),
    );

    let mut exact_word_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|x| x / 2),
    );

    let mut value_buffer = Vec::new();
    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, positions)) = cursor.move_on_next()? {
        let (document_id_bytes, word_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = u32::from_be_bytes(document_id_bytes);

        let bitmap = RoaringBitmap::from_iter(Some(document_id));
        serialize_roaring_bitmap(&bitmap, &mut value_buffer)?;

        // If there are no exact attributes, we do not need to iterate over positions.
        if exact_attributes.is_empty() {
            word_docids_sorter.insert(word_bytes, &value_buffer)?;
        } else {
            let mut added_to_exact = false;
            let mut added_to_word_docids = false;
            for position in read_u32_ne_bytes(positions) {
                // as soon as we know that this word had been to both readers, we don't need to
                // iterate over the positions.
                if added_to_exact && added_to_word_docids {
                    break;
                }
                let (fid, _) = relative_from_absolute_position(position);
                if exact_attributes.contains(&fid) && !added_to_exact {
                    exact_word_docids_sorter.insert(word_bytes, &value_buffer)?;
                    added_to_exact = true;
                } else if !added_to_word_docids {
                    word_docids_sorter.insert(word_bytes, &value_buffer)?;
                    added_to_word_docids = true;
                }
            }
        }
    }

    Ok((
        sorter_into_reader(word_docids_sorter, indexer)?,
        sorter_into_reader(exact_word_docids_sorter, indexer)?,
    ))
}

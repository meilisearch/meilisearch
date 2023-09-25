use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::{cmp, io};

use obkv::KvReaderU16;

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_reader, try_split_array_at,
    GrenadParameters, MergeFn,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::proximity::{positions_proximity, MAX_DISTANCE};
use crate::{DocumentId, Result};

/// Extracts the best proximity between pairs of words and the documents ids where this pair appear.
///
/// Returns a grenad reader with the list of extracted word pairs proximities and
/// documents ids from the given chunk of docid word positions.
#[logging_timer::time]
pub fn extract_word_pair_proximity_docids<R: io::Read + io::Seek>(
    docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    puffin::profile_function!();

    let max_memory = indexer.max_memory_by_thread();

    let mut word_pair_proximity_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 2),
    );

    let mut word_positions: Vec<(String, u16)> = Vec::with_capacity(MAX_DISTANCE as usize);
    let mut word_pair_proximity = HashMap::new();
    let mut current_document_id = None;

    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let (document_id_bytes, _fid_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = u32::from_be_bytes(document_id_bytes);

        // if we change document, we fill the sorter
        if current_document_id.map_or(false, |id| id != document_id) {
            while !word_positions.is_empty() {
                word_positions_into_word_pair_proximity(
                    &mut word_positions,
                    &mut word_pair_proximity,
                )?;
            }

            document_word_positions_into_sorter(
                current_document_id.unwrap(),
                &word_pair_proximity,
                &mut word_pair_proximity_docids_sorter,
            )?;
            word_pair_proximity.clear();
            word_positions.clear();
        }

        current_document_id = Some(document_id);

        for (position, word) in KvReaderU16::new(&value).iter() {
            // drain the proximity window until the head word is considered close to the word we are inserting.
            while word_positions.get(0).map_or(false, |(_w, p)| {
                positions_proximity(*p as u32, position as u32) > MAX_DISTANCE
            }) {
                word_positions_into_word_pair_proximity(
                    &mut word_positions,
                    &mut word_pair_proximity,
                )?;
            }

            // insert the new word.
            let word = std::str::from_utf8(word)?;
            word_positions.push((word.to_string(), position));
        }
    }

    if let Some(document_id) = current_document_id {
        while !word_positions.is_empty() {
            word_positions_into_word_pair_proximity(&mut word_positions, &mut word_pair_proximity)?;
        }

        document_word_positions_into_sorter(
            document_id,
            &word_pair_proximity,
            &mut word_pair_proximity_docids_sorter,
        )?;
    }

    sorter_into_reader(word_pair_proximity_docids_sorter, indexer)
}

/// Fills the list of all pairs of words with the shortest proximity between 1 and 7 inclusive.
///
/// This list is used by the engine to calculate the documents containing words that are
/// close to each other.
fn document_word_positions_into_sorter(
    document_id: DocumentId,
    word_pair_proximity: &HashMap<(String, String), u8>,
    word_pair_proximity_docids_sorter: &mut grenad::Sorter<MergeFn>,
) -> Result<()> {
    let mut key_buffer = Vec::new();
    for ((w1, w2), prox) in word_pair_proximity {
        key_buffer.clear();
        key_buffer.push(*prox as u8);
        key_buffer.extend_from_slice(w1.as_bytes());
        key_buffer.push(0);
        key_buffer.extend_from_slice(w2.as_bytes());

        word_pair_proximity_docids_sorter.insert(&key_buffer, document_id.to_ne_bytes())?;
    }

    Ok(())
}

fn word_positions_into_word_pair_proximity(
    word_positions: &mut Vec<(String, u16)>,
    word_pair_proximity: &mut HashMap<(String, String), u8>,
) -> Result<()> {
    let (head_word, head_position) = word_positions.remove(0);
    for (word, position) in word_positions.iter() {
        let prox = positions_proximity(head_position as u32, *position as u32) as u8;
        word_pair_proximity
            .entry((head_word.clone(), word.clone()))
            .and_modify(|p| {
                *p = cmp::min(*p, prox);
            })
            .or_insert(prox);
    }
    Ok(())
}

struct PeekedWordPosition<I> {
    word: String,
    position: u32,
    iter: I,
}

impl<I> Ord for PeekedWordPosition<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.position.cmp(&other.position).reverse()
    }
}

impl<I> PartialOrd for PeekedWordPosition<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> Eq for PeekedWordPosition<I> {}

impl<I> PartialEq for PeekedWordPosition<I> {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position
    }
}

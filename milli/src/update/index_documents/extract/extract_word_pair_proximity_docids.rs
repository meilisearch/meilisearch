use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::time::{Duration, Instant};
use std::{cmp, io, mem, str, vec};

use log::debug;

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, read_u32_ne_bytes, sorter_into_reader,
    try_split_array_at, GrenadParameters, MergeFn,
};
use crate::proximity::{positions_proximity, MAX_DISTANCE};
use crate::{DocumentId, Result};

/// Extracts the best proximity between pairs of words and the documents ids where this pair appear.
///
/// Returns a grenad reader with the list of extracted word pairs proximities and
/// documents ids from the given chunk of docid word positions.
pub fn extract_word_pair_proximity_docids<R: io::Read>(
    mut docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let max_memory = indexer.max_memory_by_thread();

    let mut word_pair_proximity_docids_sorter = create_sorter(
        merge_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut number_of_documents = 0;
    let mut total_time_aggregation = Duration::default();
    let mut total_time_grenad_insert = Duration::default();

    // This map is assumed to not consume a lot of memory.
    let mut document_word_positions_heap = BinaryHeap::new();
    let mut current_document_id = None;

    while let Some((key, value)) = docid_word_positions.next()? {
        let (document_id_bytes, word_bytes) = try_split_array_at(key).unwrap();
        let document_id = u32::from_be_bytes(document_id_bytes);
        let word = str::from_utf8(word_bytes)?;

        let curr_document_id = *current_document_id.get_or_insert(document_id);
        if curr_document_id != document_id {
            let document_word_positions_heap = mem::take(&mut document_word_positions_heap);
            document_word_positions_into_sorter(
                curr_document_id,
                document_word_positions_heap,
                &mut word_pair_proximity_docids_sorter,
                &mut total_time_aggregation,
                &mut total_time_grenad_insert,
            )?;
            number_of_documents += 1;
            current_document_id = Some(document_id);
        }

        let word = word.to_string();
        let mut iter = read_u32_ne_bytes(value).collect::<Vec<_>>().into_iter();
        if let Some(position) = iter.next() {
            document_word_positions_heap.push(PeekedWordPosition { word, position, iter });
        }
    }

    if let Some(document_id) = current_document_id {
        // We must make sure that don't lose the current document field id
        // word count map if we break because we reached the end of the chunk.
        let document_word_positions_heap = mem::take(&mut document_word_positions_heap);
        document_word_positions_into_sorter(
            document_id,
            document_word_positions_heap,
            &mut word_pair_proximity_docids_sorter,
            &mut total_time_aggregation,
            &mut total_time_grenad_insert,
        )?;
    }

    debug!(
        "Number of documents {}
        - we took {:02?} to aggregate proximities
        - we took {:02?} to grenad insert those proximities",
        number_of_documents, total_time_aggregation, total_time_grenad_insert,
    );

    sorter_into_reader(word_pair_proximity_docids_sorter, indexer)
}

/// Fills the list of all pairs of words with the shortest proximity between 1 and 7 inclusive.
///
/// This list is used by the engine to calculate the documents containing words that are
/// close to each other.
fn document_word_positions_into_sorter<'b>(
    document_id: DocumentId,
    mut word_positions_heap: BinaryHeap<PeekedWordPosition<vec::IntoIter<u32>>>,
    word_pair_proximity_docids_sorter: &mut grenad::Sorter<MergeFn>,
    total_time_aggregation: &mut Duration,
    total_time_grenad_insert: &mut Duration,
) -> Result<()> {
    let before_aggregating = Instant::now();
    let mut word_pair_proximity = HashMap::new();
    let mut ordered_peeked_word_positions = Vec::new();
    while !word_positions_heap.is_empty() {
        while let Some(peeked_word_position) = word_positions_heap.pop() {
            ordered_peeked_word_positions.push(peeked_word_position);
            if ordered_peeked_word_positions.len() == 7 {
                break;
            }
        }

        if let Some((head, tail)) = ordered_peeked_word_positions.split_first() {
            for PeekedWordPosition { word, position, .. } in tail {
                let prox = positions_proximity(head.position, *position);
                if prox > 0 && prox < MAX_DISTANCE {
                    word_pair_proximity
                        .entry((head.word.clone(), word.clone()))
                        .and_modify(|p| {
                            *p = cmp::min(*p, prox);
                        })
                        .or_insert(prox);

                    // We also compute the inverse proximity.
                    let prox = prox + 1;
                    if prox < MAX_DISTANCE {
                        word_pair_proximity
                            .entry((word.clone(), head.word.clone()))
                            .and_modify(|p| {
                                *p = cmp::min(*p, prox);
                            })
                            .or_insert(prox);
                    }
                }
            }

            // Push the tail in the heap.
            let tail_iter = ordered_peeked_word_positions.drain(1..);
            word_positions_heap.extend(tail_iter);

            // Advance the head and push it in the heap.
            if let Some(mut head) = ordered_peeked_word_positions.pop() {
                if let Some(next_position) = head.iter.next() {
                    word_positions_heap.push(PeekedWordPosition {
                        word: head.word,
                        position: next_position,
                        iter: head.iter,
                    });
                }
            }
        }
    }

    *total_time_aggregation += before_aggregating.elapsed();

    let mut key_buffer = Vec::new();
    for ((w1, w2), prox) in word_pair_proximity {
        key_buffer.clear();
        key_buffer.extend_from_slice(w1.as_bytes());
        key_buffer.push(0);
        key_buffer.extend_from_slice(w2.as_bytes());
        key_buffer.push(prox as u8);

        let before_grenad_insert = Instant::now();
        word_pair_proximity_docids_sorter.insert(&key_buffer, &document_id.to_ne_bytes())?;
        *total_time_grenad_insert += before_grenad_insert.elapsed();
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

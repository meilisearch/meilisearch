use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::BufReader;
use std::num::NonZeroUsize;
use std::{cmp, io};

use obkv::KvReaderU16;

use super::helpers::{
    create_sorter, create_writer, merge_deladd_cbo_roaring_bitmaps, try_split_array_at,
    writer_into_reader, GrenadParameters, MergeFn,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::proximity::{index_proximity, ProximityPrecision, MAX_DISTANCE};
use crate::update::del_add::{DelAdd, KvReaderDelAdd};
use crate::update::index_documents::cache::SorterCacheDelAddCboRoaringBitmap;
use crate::update::settings::InnerIndexSettingsDiff;
use crate::{DocumentId, Result};

/// Extracts the best proximity between pairs of words and the documents ids where this pair appear.
///
/// Returns a grenad reader with the list of extracted word pairs proximities and
/// documents ids from the given chunk of docid word positions.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_word_pair_proximity_docids<R: io::Read + io::Seek>(
    docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
    settings_diff: &InnerIndexSettingsDiff,
) -> Result<grenad::Reader<BufReader<File>>> {
    // early return if the data shouldn't be deleted nor created.
    if settings_diff.settings_update_only && !settings_diff.reindex_proximities() {
        let writer = create_writer(
            indexer.chunk_compression_type,
            indexer.chunk_compression_level,
            tempfile::tempfile()?,
        );
        return writer_into_reader(writer);
    }

    let any_deletion = settings_diff.old.proximity_precision == ProximityPrecision::ByWord;
    let any_addition = settings_diff.new.proximity_precision == ProximityPrecision::ByWord;

    let max_memory = indexer.max_memory_by_thread();
    let mut cached_word_pair_proximity_docids_sorters: Vec<_> = (1..MAX_DISTANCE)
        .map(|_| {
            let sorter = create_sorter(
                grenad::SortAlgorithm::Unstable,
                merge_deladd_cbo_roaring_bitmaps,
                indexer.chunk_compression_type,
                indexer.chunk_compression_level,
                indexer.max_nb_chunks,
                max_memory.map(|m| m / MAX_DISTANCE as usize),
            );
            SorterCacheDelAddCboRoaringBitmap::<20, MergeFn>::new(
                NonZeroUsize::new(100).unwrap(),
                sorter,
                super::REDIS_CLIENT.get_connection().unwrap(),
            )
        })
        .collect();

    let mut del_word_positions: VecDeque<(String, u16)> =
        VecDeque::with_capacity(MAX_DISTANCE as usize);
    let mut add_word_positions: VecDeque<(String, u16)> =
        VecDeque::with_capacity(MAX_DISTANCE as usize);
    let mut del_word_pair_proximity = BTreeMap::new();
    let mut add_word_pair_proximity = BTreeMap::new();
    let mut current_document_id = None;

    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let (document_id_bytes, _fid_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = u32::from_be_bytes(document_id_bytes);

        // if we change document, we fill the sorter
        if current_document_id.map_or(false, |id| id != document_id) {
            // FIXME: span inside of a hot loop might degrade performance and create big reports
            let span = tracing::trace_span!(target: "indexing::details", "document_into_sorter");
            let _entered = span.enter();

            document_word_positions_into_sorter(
                current_document_id.unwrap(),
                &del_word_pair_proximity,
                &add_word_pair_proximity,
                &mut cached_word_pair_proximity_docids_sorters,
            )?;
            del_word_pair_proximity.clear();
            add_word_pair_proximity.clear();
        }

        current_document_id = Some(document_id);

        let (del, add): (Result<_>, Result<_>) = rayon::join(
            || {
                if !any_deletion {
                    return Ok(());
                }

                // deletions
                if let Some(deletion) = KvReaderDelAdd::new(value).get(DelAdd::Deletion) {
                    for (position, word) in KvReaderU16::new(deletion).iter() {
                        // drain the proximity window until the head word is considered close to the word we are inserting.
                        while del_word_positions.front().map_or(false, |(_w, p)| {
                            index_proximity(*p as u32, position as u32) >= MAX_DISTANCE
                        }) {
                            word_positions_into_word_pair_proximity(
                                &mut del_word_positions,
                                &mut del_word_pair_proximity,
                            )?;
                        }

                        // insert the new word.
                        let word = std::str::from_utf8(word)?;
                        del_word_positions.push_back((word.to_string(), position));
                    }

                    while !del_word_positions.is_empty() {
                        word_positions_into_word_pair_proximity(
                            &mut del_word_positions,
                            &mut del_word_pair_proximity,
                        )?;
                    }
                }

                Ok(())
            },
            || {
                if !any_addition {
                    return Ok(());
                }

                // additions
                if let Some(addition) = KvReaderDelAdd::new(value).get(DelAdd::Addition) {
                    for (position, word) in KvReaderU16::new(addition).iter() {
                        // drain the proximity window until the head word is considered close to the word we are inserting.
                        while add_word_positions.front().map_or(false, |(_w, p)| {
                            index_proximity(*p as u32, position as u32) >= MAX_DISTANCE
                        }) {
                            word_positions_into_word_pair_proximity(
                                &mut add_word_positions,
                                &mut add_word_pair_proximity,
                            )?;
                        }

                        // insert the new word.
                        let word = std::str::from_utf8(word)?;
                        add_word_positions.push_back((word.to_string(), position));
                    }

                    while !add_word_positions.is_empty() {
                        word_positions_into_word_pair_proximity(
                            &mut add_word_positions,
                            &mut add_word_pair_proximity,
                        )?;
                    }
                }

                Ok(())
            },
        );

        del?;
        add?;
    }

    if let Some(document_id) = current_document_id {
        // FIXME: span inside of a hot loop might degrade performance and create big reports
        let span = tracing::trace_span!(target: "indexing::details", "final_document_into_sorter");
        let _entered = span.enter();

        document_word_positions_into_sorter(
            document_id,
            &del_word_pair_proximity,
            &add_word_pair_proximity,
            &mut cached_word_pair_proximity_docids_sorters,
        )?;
    }
    {
        // FIXME: span inside of a hot loop might degrade performance and create big reports
        let span = tracing::trace_span!(target: "indexing::details", "sorter_into_reader");
        let _entered = span.enter();

        let mut writer = create_writer(
            indexer.chunk_compression_type,
            indexer.chunk_compression_level,
            tempfile::tempfile()?,
        );

        for cached_sorter in cached_word_pair_proximity_docids_sorters {
            cached_sorter.into_sorter()?.write_into_stream_writer(&mut writer)?;
        }

        writer_into_reader(writer)
    }
}

/// Fills the list of all pairs of words with the shortest proximity between 1 and 7 inclusive.
///
/// This list is used by the engine to calculate the documents containing words that are
/// close to each other.
fn document_word_positions_into_sorter(
    document_id: DocumentId,
    del_word_pair_proximity: &BTreeMap<(String, String), u8>,
    add_word_pair_proximity: &BTreeMap<(String, String), u8>,
    cached_word_pair_proximity_docids_sorters: &mut [SorterCacheDelAddCboRoaringBitmap<
        20,
        MergeFn,
    >],
) -> Result<()> {
    use itertools::merge_join_by;
    use itertools::EitherOrBoth::{Both, Left, Right};

    let mut key_buffer = Vec::new();
    for eob in
        merge_join_by(del_word_pair_proximity.iter(), add_word_pair_proximity.iter(), |d, a| {
            d.cmp(a)
        })
    {
        let ((w1, w2), prox) = match eob {
            Left(key_value) => key_value,
            Right(key_value) => key_value,
            Both(key_value, _) => key_value,
        };

        key_buffer.clear();
        key_buffer.push(*prox);
        key_buffer.extend_from_slice(w1.as_bytes());
        key_buffer.push(0);
        key_buffer.extend_from_slice(w2.as_bytes());

        match eob {
            Left(_) => {
                cached_word_pair_proximity_docids_sorters[*prox as usize - 1]
                    .insert_del_u32(&key_buffer, document_id)?;
            }
            Right(_) => {
                cached_word_pair_proximity_docids_sorters[*prox as usize - 1]
                    .insert_add_u32(&key_buffer, document_id)?;
            }
            Both(_, _) => {
                cached_word_pair_proximity_docids_sorters[*prox as usize - 1]
                    .insert_del_add_u32(&key_buffer, document_id)?;
            }
        }
    }

    Ok(())
}

fn word_positions_into_word_pair_proximity(
    word_positions: &mut VecDeque<(String, u16)>,
    word_pair_proximity: &mut BTreeMap<(String, String), u8>,
) -> Result<()> {
    let (head_word, head_position) = word_positions.pop_front().unwrap();
    for (word, position) in word_positions.iter() {
        let prox = index_proximity(head_position as u32, *position as u32) as u8;
        if prox > 0 && prox < MAX_DISTANCE as u8 {
            word_pair_proximity
                .entry((head_word.clone(), word.clone()))
                .and_modify(|p| {
                    *p = cmp::min(*p, prox);
                })
                .or_insert(prox);
        }
    }
    Ok(())
}

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufReader};

use obkv::KvReaderU16;

use super::helpers::{
    create_sorter, merge_deladd_cbo_roaring_bitmaps, sorter_into_reader, try_split_array_at,
    GrenadParameters,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::settings::InnerIndexSettingsDiff;
use crate::update::MergeFn;
use crate::{bucketed_position, DocumentId, Result};

/// Extracts the word positions and the documents ids where this word appear.
///
/// Returns a grenad reader with the list of extracted words at positions and
/// documents ids from the given chunk of docid word positions.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_word_position_docids<R: io::Read + io::Seek>(
    docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
    _settings_diff: &InnerIndexSettingsDiff,
) -> Result<grenad::Reader<BufReader<File>>> {
    let mut conn = super::REDIS_CLIENT.get_connection().unwrap();
    let max_memory = indexer.max_memory_by_thread();

    let mut word_position_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut del_word_positions: BTreeSet<(u16, Vec<u8>)> = BTreeSet::new();
    let mut add_word_positions: BTreeSet<(u16, Vec<u8>)> = BTreeSet::new();
    let mut current_document_id: Option<u32> = None;
    let mut key_buffer = Vec::new();
    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let (document_id_bytes, _fid_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = DocumentId::from_be_bytes(document_id_bytes);

        if current_document_id.map_or(false, |id| document_id != id) {
            words_position_into_sorter(
                current_document_id.unwrap(),
                &mut key_buffer,
                &del_word_positions,
                &add_word_positions,
                &mut word_position_docids_sorter,
                &mut conn,
            )?;
            del_word_positions.clear();
            add_word_positions.clear();
        }

        current_document_id = Some(document_id);

        let del_add_reader = KvReaderDelAdd::new(value);
        // extract all unique words to remove.
        if let Some(deletion) = del_add_reader.get(DelAdd::Deletion) {
            for (position, word_bytes) in KvReaderU16::new(deletion).iter() {
                let position = bucketed_position(position);
                del_word_positions.insert((position, word_bytes.to_vec()));
            }
        }

        // extract all unique additional words.
        if let Some(addition) = del_add_reader.get(DelAdd::Addition) {
            for (position, word_bytes) in KvReaderU16::new(addition).iter() {
                let position = bucketed_position(position);
                add_word_positions.insert((position, word_bytes.to_vec()));
            }
        }
    }

    if let Some(document_id) = current_document_id {
        words_position_into_sorter(
            document_id,
            &mut key_buffer,
            &del_word_positions,
            &add_word_positions,
            &mut word_position_docids_sorter,
            &mut conn,
        )?;
    }

    // TODO remove noop DelAdd OBKV
    let word_position_docids_reader = sorter_into_reader(word_position_docids_sorter, indexer)?;

    Ok(word_position_docids_reader)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
fn words_position_into_sorter(
    document_id: DocumentId,
    key_buffer: &mut Vec<u8>,
    del_word_positions: &BTreeSet<(u16, Vec<u8>)>,
    add_word_positions: &BTreeSet<(u16, Vec<u8>)>,
    word_position_docids_sorter: &mut grenad::Sorter<MergeFn>,
    conn: &mut redis::Connection,
) -> Result<()> {
    use itertools::merge_join_by;
    use itertools::EitherOrBoth::{Both, Left, Right};

    let mut buffer = Vec::new();
    for eob in merge_join_by(del_word_positions.iter(), add_word_positions.iter(), |d, a| d.cmp(a))
    {
        buffer.clear();
        let mut value_writer = KvWriterDelAdd::new(&mut buffer);
        let (position, word_bytes) = match eob {
            Left(key) => {
                value_writer.insert(DelAdd::Deletion, document_id.to_ne_bytes()).unwrap();
                key
            }
            Right(key) => {
                value_writer.insert(DelAdd::Addition, document_id.to_ne_bytes()).unwrap();
                key
            }
            Both(key, _) => {
                // both values needs to be kept because it will be used in other extractors.
                value_writer.insert(DelAdd::Deletion, document_id.to_ne_bytes()).unwrap();
                value_writer.insert(DelAdd::Addition, document_id.to_ne_bytes()).unwrap();
                key
            }
        };

        key_buffer.clear();
        key_buffer.extend_from_slice(word_bytes);
        key_buffer.push(0);
        key_buffer.extend_from_slice(&position.to_be_bytes());
        redis::cmd("INCR").arg(key_buffer.as_slice()).query::<usize>(conn).unwrap();
        word_position_docids_sorter.insert(&key_buffer, value_writer.into_inner().unwrap())?;
    }

    Ok(())
}

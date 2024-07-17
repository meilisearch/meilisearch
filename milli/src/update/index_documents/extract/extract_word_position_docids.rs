use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufReader};
use std::num::NonZeroUsize;

use obkv::KvReaderU16;

use super::helpers::{
    create_sorter, merge_deladd_cbo_roaring_bitmaps, sorter_into_reader, try_split_array_at,
    GrenadParameters,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::update::del_add::{DelAdd, KvReaderDelAdd};
use crate::update::index_documents::cache::SorterCacheDelAddCboRoaringBitmap;
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
    let max_memory = indexer.max_memory_by_thread();

    let word_position_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );
    let mut cached_word_position_docids_sorter =
        SorterCacheDelAddCboRoaringBitmap::<20, MergeFn>::new(
            NonZeroUsize::new(300).unwrap(),
            word_position_docids_sorter,
            super::REDIS_CLIENT.get_connection().unwrap(),
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
                &mut cached_word_position_docids_sorter,
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
            &mut cached_word_position_docids_sorter,
        )?;
    }

    // TODO remove noop DelAdd OBKV
    let word_position_docids_reader =
        sorter_into_reader(cached_word_position_docids_sorter.into_sorter()?, indexer)?;

    Ok(word_position_docids_reader)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
fn words_position_into_sorter(
    document_id: DocumentId,
    key_buffer: &mut Vec<u8>,
    del_word_positions: &BTreeSet<(u16, Vec<u8>)>,
    add_word_positions: &BTreeSet<(u16, Vec<u8>)>,
    cached_word_position_docids_sorter: &mut SorterCacheDelAddCboRoaringBitmap<20, MergeFn>,
) -> Result<()> {
    use itertools::merge_join_by;
    use itertools::EitherOrBoth::{Both, Left, Right};

    for eob in merge_join_by(del_word_positions.iter(), add_word_positions.iter(), |d, a| d.cmp(a))
    {
        let (position, word_bytes) = match eob {
            Left(key) => key,
            Right(key) => key,
            Both(key, _) => key,
        };

        key_buffer.clear();
        key_buffer.extend_from_slice(word_bytes);
        key_buffer.push(0);
        key_buffer.extend_from_slice(&position.to_be_bytes());

        match eob {
            Left(_) => {
                cached_word_position_docids_sorter
                    .insert_del_u32(key_buffer.as_slice(), document_id)?;
            }
            Right(_) => {
                cached_word_position_docids_sorter
                    .insert_add_u32(key_buffer.as_slice(), document_id)?;
            }
            Both(_, _) => {
                cached_word_position_docids_sorter
                    .insert_del_add_u32(key_buffer.as_slice(), document_id)?;
            }
        }
    }

    Ok(())
}

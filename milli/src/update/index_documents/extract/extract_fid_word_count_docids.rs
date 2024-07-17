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
use crate::Result;

const MAX_COUNTED_WORDS: usize = 30;

/// Extracts the field id word count and the documents ids where
/// this field id with this amount of words appear.
///
/// Returns a grenad reader with the list of extracted field id word counts
/// and documents ids from the given chunk of docid word positions.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_fid_word_count_docids<R: io::Read + io::Seek>(
    docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
    _settings_diff: &InnerIndexSettingsDiff,
) -> Result<grenad::Reader<BufReader<File>>> {
    let max_memory = indexer.max_memory_by_thread();

    let fid_word_count_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );
    let mut cached_fid_word_count_docids_sorter =
        SorterCacheDelAddCboRoaringBitmap::<20, MergeFn>::new(
            NonZeroUsize::new(300).unwrap(),
            fid_word_count_docids_sorter,
            super::REDIS_CLIENT.get_connection().unwrap(),
        );

    let mut key_buffer = Vec::new();
    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let (document_id_bytes, fid_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = u32::from_be_bytes(document_id_bytes);

        let del_add_reader = KvReaderDelAdd::new(value);
        let deletion = del_add_reader
            // get deleted words
            .get(DelAdd::Deletion)
            // count deleted words
            .map(|deletion| KvReaderU16::new(deletion).iter().take(MAX_COUNTED_WORDS + 1).count())
            // keep the count if under or equal to MAX_COUNTED_WORDS
            .filter(|&word_count| word_count <= MAX_COUNTED_WORDS);
        let addition = del_add_reader
            // get added words
            .get(DelAdd::Addition)
            // count added words
            .map(|addition| KvReaderU16::new(addition).iter().take(MAX_COUNTED_WORDS + 1).count())
            // keep the count if under or equal to MAX_COUNTED_WORDS
            .filter(|&word_count| word_count <= MAX_COUNTED_WORDS);

        if deletion != addition {
            // Insert deleted word count in sorter if exist.
            if let Some(word_count) = deletion {
                key_buffer.clear();
                key_buffer.extend_from_slice(fid_bytes);
                key_buffer.push(word_count as u8);
                cached_fid_word_count_docids_sorter.insert_del_u32(&key_buffer, document_id)?;
            }
            // Insert added word count in sorter if exist.
            if let Some(word_count) = addition {
                key_buffer.clear();
                key_buffer.extend_from_slice(fid_bytes);
                key_buffer.push(word_count as u8);
                cached_fid_word_count_docids_sorter.insert_add_u32(&key_buffer, document_id)?;
            }
        }
    }

    sorter_into_reader(cached_fid_word_count_docids_sorter.into_sorter()?, indexer)
}

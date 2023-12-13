use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{self, BufReader};

use heed::BytesDecode;
use obkv::KvReaderU16;

use super::helpers::{
    create_sorter, create_writer, merge_deladd_cbo_roaring_bitmaps, sorter_into_reader,
    try_split_array_at, writer_into_reader, GrenadParameters,
};
use crate::error::SerializationError;
use crate::heed_codec::StrBEU16Codec;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::update::del_add::{is_noop_del_add_obkv, DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::MergeFn;
use crate::{DocumentId, FieldId, Result};

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
) -> Result<(
    grenad::Reader<BufReader<File>>,
    grenad::Reader<BufReader<File>>,
    grenad::Reader<BufReader<File>>,
)> {
    puffin::profile_function!();

    let max_memory = indexer.max_memory_by_thread();

    let mut word_fid_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|x| x / 3),
    );
    let mut key_buffer = Vec::new();
    let mut del_words = BTreeSet::new();
    let mut add_words = BTreeSet::new();
    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let (document_id_bytes, fid_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let (fid_bytes, _) = try_split_array_at(fid_bytes)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = u32::from_be_bytes(document_id_bytes);
        let fid = u16::from_be_bytes(fid_bytes);

        let del_add_reader = KvReaderDelAdd::new(value);
        // extract all unique words to remove.
        if let Some(deletion) = del_add_reader.get(DelAdd::Deletion) {
            for (_pos, word) in KvReaderU16::new(deletion).iter() {
                del_words.insert(word.to_vec());
            }
        }

        // extract all unique additional words.
        if let Some(addition) = del_add_reader.get(DelAdd::Addition) {
            for (_pos, word) in KvReaderU16::new(addition).iter() {
                add_words.insert(word.to_vec());
            }
        }

        words_into_sorter(
            document_id,
            fid,
            &mut key_buffer,
            &del_words,
            &add_words,
            &mut word_fid_docids_sorter,
        )?;

        del_words.clear();
        add_words.clear();
    }

    let mut word_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|x| x / 3),
    );

    let mut exact_word_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|x| x / 3),
    );

    let mut word_fid_docids_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut iter = word_fid_docids_sorter.into_stream_merger_iter()?;
    // TODO: replace sorters by writers by accumulating values into a buffer before inserting them.
    while let Some((key, value)) = iter.next()? {
        // only keep the value if their is a change to apply in the DB.
        if !is_noop_del_add_obkv(KvReaderDelAdd::new(value)) {
            word_fid_docids_writer.insert(key, value)?;
        }

        let (word, fid) = StrBEU16Codec::bytes_decode(key)
            .map_err(|_| SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;

        // every words contained in an attribute set to exact must be pushed in the exact_words list.
        if exact_attributes.contains(&fid) {
            exact_word_docids_sorter.insert(word.as_bytes(), value)?;
        } else {
            word_docids_sorter.insert(word.as_bytes(), value)?;
        }
    }

    Ok((
        sorter_into_reader(word_docids_sorter, indexer)?,
        sorter_into_reader(exact_word_docids_sorter, indexer)?,
        writer_into_reader(word_fid_docids_writer)?,
    ))
}

fn words_into_sorter(
    document_id: DocumentId,
    fid: FieldId,
    key_buffer: &mut Vec<u8>,
    del_words: &BTreeSet<Vec<u8>>,
    add_words: &BTreeSet<Vec<u8>>,
    word_fid_docids_sorter: &mut grenad::Sorter<MergeFn>,
) -> Result<()> {
    puffin::profile_function!();

    use itertools::merge_join_by;
    use itertools::EitherOrBoth::{Both, Left, Right};

    let mut buffer = Vec::new();
    for eob in merge_join_by(del_words.iter(), add_words.iter(), |d, a| d.cmp(a)) {
        buffer.clear();
        let mut value_writer = KvWriterDelAdd::new(&mut buffer);
        let word_bytes = match eob {
            Left(word_bytes) => {
                value_writer.insert(DelAdd::Deletion, document_id.to_ne_bytes()).unwrap();
                word_bytes
            }
            Right(word_bytes) => {
                value_writer.insert(DelAdd::Addition, document_id.to_ne_bytes()).unwrap();
                word_bytes
            }
            Both(word_bytes, _) => {
                value_writer.insert(DelAdd::Deletion, document_id.to_ne_bytes()).unwrap();
                value_writer.insert(DelAdd::Addition, document_id.to_ne_bytes()).unwrap();
                word_bytes
            }
        };

        key_buffer.clear();
        key_buffer.extend_from_slice(word_bytes);
        key_buffer.push(0);
        key_buffer.extend_from_slice(&fid.to_be_bytes());
        word_fid_docids_sorter.insert(&key_buffer, value_writer.into_inner().unwrap())?;
    }

    Ok(())
}

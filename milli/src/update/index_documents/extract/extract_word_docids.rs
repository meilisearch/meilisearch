use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io;
use std::iter::FromIterator;

use obkv::KvReaderU16;
use roaring::RoaringBitmap;

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, merge_roaring_bitmaps, serialize_roaring_bitmap,
    sorter_into_reader, try_split_array_at, GrenadParameters,
};
use crate::error::SerializationError;
use crate::index::db_name::DOCID_WORD_POSITIONS;
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
) -> Result<(grenad::Reader<File>, grenad::Reader<File>, grenad::Reader<File>)> {
    puffin::profile_function!();

    let max_memory = indexer.max_memory_by_thread();

    let mut word_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|x| x / 3),
    );

    let mut exact_word_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|x| x / 3),
    );

    let mut word_fid_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|x| x / 3),
    );

    let mut current_document_id = None;
    let mut fid = 0;
    let mut key_buffer = Vec::new();
    let mut value_buffer = Vec::new();
    let mut words = BTreeSet::new();
    let mut exact_words = BTreeSet::new();
    let mut cursor = docid_word_positions.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let (document_id_bytes, fid_bytes) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let (fid_bytes, _) = try_split_array_at(key)
            .ok_or(SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;
        let document_id = u32::from_be_bytes(document_id_bytes);
        fid = u16::from_be_bytes(fid_bytes);

        // drain the btreemaps when we change document.
        if current_document_id.map_or(false, |id| id != document_id) {
            words_into_sorters(
                document_id,
                fid,
                &mut key_buffer,
                &mut value_buffer,
                &mut exact_words,
                &mut words,
                &mut exact_word_docids_sorter,
                &mut word_docids_sorter,
                &mut word_fid_docids_sorter,
            )?;
        }

        current_document_id = Some(document_id);

        // every words contained in an attribute set to exact must be pushed in the exact_words list.
        if exact_attributes.contains(&fid) {
            for (_pos, word) in KvReaderU16::new(&value).iter() {
                exact_words.insert(word.to_vec());
            }
        } else {
            for (_pos, word) in KvReaderU16::new(&value).iter() {
                words.insert(word.to_vec());
            }
        }
    }

    // We must make sure that don't lose the current document field id
    if let Some(document_id) = current_document_id {
        words_into_sorters(
            document_id,
            fid,
            &mut key_buffer,
            &mut value_buffer,
            &mut exact_words,
            &mut words,
            &mut exact_word_docids_sorter,
            &mut word_docids_sorter,
            &mut word_fid_docids_sorter,
        )?;
    }

    Ok((
        sorter_into_reader(word_docids_sorter, indexer)?,
        sorter_into_reader(exact_word_docids_sorter, indexer)?,
        sorter_into_reader(word_fid_docids_sorter, indexer)?,
    ))
}

fn words_into_sorters(
    document_id: DocumentId,
    fid: FieldId,
    key_buffer: &mut Vec<u8>,
    value_buffer: &mut Vec<u8>,
    exact_words: &mut BTreeSet<Vec<u8>>,
    words: &mut BTreeSet<Vec<u8>>,
    exact_word_docids_sorter: &mut grenad::Sorter<MergeFn>,
    word_docids_sorter: &mut grenad::Sorter<MergeFn>,
    word_fid_docids_sorter: &mut grenad::Sorter<MergeFn>,
) -> Result<()> {
    puffin::profile_function!();
    let bitmap = RoaringBitmap::from_iter(Some(document_id));
    serialize_roaring_bitmap(&bitmap, value_buffer)?;
    for word_bytes in exact_words.iter() {
        exact_word_docids_sorter.insert(word_bytes, &mut *value_buffer)?;
    }

    for word_bytes in words.iter() {
        word_docids_sorter.insert(word_bytes, &value_buffer)?;
    }

    for word_bytes in (&*words | &*exact_words).iter() {
        key_buffer.clear();
        key_buffer.extend_from_slice(&word_bytes);
        key_buffer.push(0);
        key_buffer.extend_from_slice(&fid.to_be_bytes());
        word_fid_docids_sorter.insert(word_bytes, &value_buffer)?;
    }

    exact_words.clear();
    words.clear();

    Ok(())
}

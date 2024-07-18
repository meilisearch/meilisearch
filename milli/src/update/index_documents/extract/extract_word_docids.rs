use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufReader};
use std::num::NonZeroUsize;

use heed::{BytesDecode, BytesEncode};
use obkv::KvReaderU16;
use roaring::RoaringBitmap;

use super::helpers::{
    create_sorter, create_writer, merge_deladd_cbo_roaring_bitmaps, try_split_array_at,
    writer_into_reader, GrenadParameters,
};
use super::REDIS_CLIENT;
use crate::error::SerializationError;
use crate::heed_codec::StrBEU16Codec;
use crate::index::db_name::DOCID_WORD_POSITIONS;
use crate::update::del_add::{is_noop_del_add_obkv, DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::cache::SorterCacheDelAddCboRoaringBitmap;
use crate::update::index_documents::helpers::sorter_into_reader;
use crate::update::settings::InnerIndexSettingsDiff;
use crate::update::MergeFn;
use crate::{CboRoaringBitmapCodec, DocumentId, FieldId, Result};

/// Extracts the word and the documents ids where this word appear.
///
/// Returns a grenad reader with the list of extracted words and
/// documents ids from the given chunk of docid word positions.
///
/// The first returned reader is the one for normal word_docids, and the second one is for
/// exact_word_docids
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_word_docids<R: io::Read + io::Seek>(
    docid_word_positions: grenad::Reader<R>,
    indexer: GrenadParameters,
    settings_diff: &InnerIndexSettingsDiff,
) -> Result<(
    grenad::Reader<BufReader<File>>,
    grenad::Reader<BufReader<File>>,
    grenad::Reader<BufReader<File>>,
)> {
    let max_memory = indexer.max_memory_by_thread();

    let word_fid_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 3),
    );
    let mut cached_word_fid_docids_sorter = SorterCacheDelAddCboRoaringBitmap::<20, _>::new(
        NonZeroUsize::new(300).unwrap(),
        word_fid_docids_sorter,
        b"wfd",
        REDIS_CLIENT.get_connection().unwrap(),
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
            &mut cached_word_fid_docids_sorter,
        )?;

        del_words.clear();
        add_words.clear();
    }

    let mut word_fid_docids_writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let word_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 3),
    );
    let mut cached_word_docids_sorter = SorterCacheDelAddCboRoaringBitmap::<20, MergeFn>::new(
        NonZeroUsize::new(100).unwrap(),
        word_docids_sorter,
        b"wdi",
        REDIS_CLIENT.get_connection().unwrap(),
    );

    let exact_word_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 3),
    );
    let mut cached_exact_word_docids_sorter = SorterCacheDelAddCboRoaringBitmap::<20, MergeFn>::new(
        NonZeroUsize::new(100).unwrap(),
        exact_word_docids_sorter,
        b"ewd",
        REDIS_CLIENT.get_connection().unwrap(),
    );

    let mut iter = cached_word_fid_docids_sorter.into_sorter()?.into_stream_merger_iter()?;
    // NOTE: replacing sorters by bitmap merging is less efficient, so, use sorters.
    while let Some((key, value)) = iter.next()? {
        // only keep the value if their is a change to apply in the DB.
        if !is_noop_del_add_obkv(KvReaderDelAdd::new(value)) {
            word_fid_docids_writer.insert(key, value)?;
        }

        let (w, fid) = StrBEU16Codec::bytes_decode(key)
            .map_err(|_| SerializationError::Decoding { db_name: Some(DOCID_WORD_POSITIONS) })?;

        // merge all deletions
        let obkv = KvReaderDelAdd::new(value);
        let w = w.as_bytes();
        if let Some(value) = obkv.get(DelAdd::Deletion) {
            let delete_from_exact = settings_diff.old.exact_attributes.contains(&fid);
            let bitmap = CboRoaringBitmapCodec::deserialize_from(value)?;
            if delete_from_exact {
                cached_exact_word_docids_sorter.insert_del(w, bitmap)?;
            } else {
                cached_word_docids_sorter.insert_del(w, bitmap)?;
            }
        }
        // merge all additions
        if let Some(value) = obkv.get(DelAdd::Addition) {
            let add_in_exact = settings_diff.new.exact_attributes.contains(&fid);
            let bitmap = CboRoaringBitmapCodec::deserialize_from(value)?;
            if add_in_exact {
                cached_exact_word_docids_sorter.insert_add(w, bitmap)?;
            } else {
                cached_word_docids_sorter.insert_add(w, bitmap)?;
            }
        }
    }

    Ok((
        sorter_into_reader(cached_word_docids_sorter.into_sorter()?, indexer)?,
        sorter_into_reader(cached_exact_word_docids_sorter.into_sorter()?, indexer)?,
        writer_into_reader(word_fid_docids_writer)?,
    ))
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
fn words_into_sorter(
    document_id: DocumentId,
    fid: FieldId,
    key_buffer: &mut Vec<u8>,
    del_words: &BTreeSet<Vec<u8>>,
    add_words: &BTreeSet<Vec<u8>>,
    cached_word_fid_docids_sorter: &mut SorterCacheDelAddCboRoaringBitmap<20, MergeFn>,
) -> Result<()> {
    use itertools::merge_join_by;
    use itertools::EitherOrBoth::{Both, Left, Right};

    for eob in merge_join_by(del_words.iter(), add_words.iter(), |d, a| d.cmp(a)) {
        let word_bytes = match eob {
            Left(word_bytes) => word_bytes,
            Right(word_bytes) => word_bytes,
            Both(word_bytes, _) => word_bytes,
        };

        key_buffer.clear();
        key_buffer.extend_from_slice(word_bytes);
        key_buffer.push(0);
        key_buffer.extend_from_slice(&fid.to_be_bytes());

        match eob {
            Left(_) => {
                cached_word_fid_docids_sorter.insert_del_u32(key_buffer, document_id)?;
            }
            Right(_) => {
                cached_word_fid_docids_sorter.insert_add_u32(key_buffer, document_id)?;
            }
            Both(_, _) => {
                cached_word_fid_docids_sorter.insert_del_add_u32(key_buffer, document_id)?;
            }
        }
    }

    Ok(())
}

// TODO do we still use this?
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
fn docids_into_writers<W>(
    word: &str,
    deletions: &RoaringBitmap,
    additions: &RoaringBitmap,
    writer: &mut grenad::Writer<W>,
    conn: &mut redis::Connection,
) -> Result<()>
where
    W: std::io::Write,
{
    if deletions == additions {
        // if the same value is deleted and added, do nothing.
        return Ok(());
    }

    // Write each value in the same KvDelAdd before inserting it in the final writer.
    let mut obkv = KvWriterDelAdd::memory();
    // deletions:
    if !deletions.is_empty() && !deletions.is_subset(additions) {
        obkv.insert(
            DelAdd::Deletion,
            CboRoaringBitmapCodec::bytes_encode(deletions).map_err(|_| {
                SerializationError::Encoding { db_name: Some(DOCID_WORD_POSITIONS) }
            })?,
        )?;
    }
    // additions:
    if !additions.is_empty() {
        obkv.insert(
            DelAdd::Addition,
            CboRoaringBitmapCodec::bytes_encode(additions).map_err(|_| {
                SerializationError::Encoding { db_name: Some(DOCID_WORD_POSITIONS) }
            })?,
        )?;
    }

    // insert everything in the same writer.
    redis::cmd("INCR").arg(word.as_bytes()).query::<usize>(conn).unwrap();
    writer.insert(word.as_bytes(), obkv.into_inner().unwrap())?;

    Ok(())
}

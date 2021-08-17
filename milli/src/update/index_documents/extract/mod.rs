mod extract_docid_word_positions;
mod extract_facet_number_docids;
mod extract_facet_string_docids;
mod extract_fid_docid_facet_values;
mod extract_fid_word_count_docids;
mod extract_word_docids;
mod extract_word_level_position_docids;
mod extract_word_pair_proximity_docids;

use std::collections::HashSet;
use std::fs::File;

use crossbeam_channel::Sender;
use log::debug;
use rayon::prelude::*;

use self::extract_docid_word_positions::extract_docid_word_positions;
use self::extract_facet_number_docids::extract_facet_number_docids;
use self::extract_facet_string_docids::extract_facet_string_docids;
use self::extract_fid_docid_facet_values::extract_fid_docid_facet_values;
use self::extract_fid_word_count_docids::extract_fid_word_count_docids;
use self::extract_word_docids::extract_word_docids;
use self::extract_word_level_position_docids::extract_word_level_position_docids;
use self::extract_word_pair_proximity_docids::extract_word_pair_proximity_docids;
use super::helpers::{
    into_clonable_grenad, keep_first_prefix_value_merge_roaring_bitmaps, merge_cbo_roaring_bitmaps,
    merge_readers, merge_roaring_bitmaps, CursorClonableMmap, GrenadParameters, MergeFn,
};
use super::{helpers, TypedChunk};
use crate::{FieldId, Result};

/// Extract data for each databases from obkv documents in parallel.
/// Send data in grenad file over provided Sender.
pub(crate) fn data_from_obkv_documents(
    obkv_chunks: impl Iterator<Item = Result<grenad::Reader<File>>> + Send,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<TypedChunk>,
    searchable_fields: Option<HashSet<FieldId>>,
    faceted_fields: HashSet<FieldId>,
    stop_words: Option<fst::Set<&[u8]>>,
) -> Result<()> {
    let result: Result<(Vec<_>, (Vec<_>, Vec<_>))> = obkv_chunks
        .par_bridge()
        .map(|result| {
            let documents_chunk = result.and_then(|c| unsafe { into_clonable_grenad(c) }).unwrap();

            lmdb_writer_sx.send(TypedChunk::Documents(documents_chunk.clone())).unwrap();

            let (docid_word_positions_chunk, docid_fid_facet_values_chunks): (
                Result<_>,
                Result<_>,
            ) = rayon::join(
                || {
                    let (documents_ids, docid_word_positions_chunk) = extract_docid_word_positions(
                        documents_chunk.clone(),
                        indexer.clone(),
                        &searchable_fields,
                        stop_words.as_ref(),
                    )?;

                    // send documents_ids to DB writer
                    lmdb_writer_sx.send(TypedChunk::NewDocumentsIds(documents_ids)).unwrap();

                    // send docid_word_positions_chunk to DB writer
                    let docid_word_positions_chunk =
                        unsafe { into_clonable_grenad(docid_word_positions_chunk)? };
                    lmdb_writer_sx
                        .send(TypedChunk::DocidWordPositions(docid_word_positions_chunk.clone()))
                        .unwrap();
                    Ok(docid_word_positions_chunk)
                },
                || {
                    let (docid_fid_facet_numbers_chunk, docid_fid_facet_strings_chunk) =
                        extract_fid_docid_facet_values(
                            documents_chunk.clone(),
                            indexer.clone(),
                            &faceted_fields,
                        )?;

                    // send docid_fid_facet_numbers_chunk to DB writer
                    let docid_fid_facet_numbers_chunk =
                        unsafe { into_clonable_grenad(docid_fid_facet_numbers_chunk)? };
                    lmdb_writer_sx
                        .send(TypedChunk::FieldIdDocidFacetNumbers(
                            docid_fid_facet_numbers_chunk.clone(),
                        ))
                        .unwrap();

                    // send docid_fid_facet_strings_chunk to DB writer
                    let docid_fid_facet_strings_chunk =
                        unsafe { into_clonable_grenad(docid_fid_facet_strings_chunk)? };
                    lmdb_writer_sx
                        .send(TypedChunk::FieldIdDocidFacetStrings(
                            docid_fid_facet_strings_chunk.clone(),
                        ))
                        .unwrap();

                    Ok((docid_fid_facet_numbers_chunk, docid_fid_facet_strings_chunk))
                },
            );
            Ok((docid_word_positions_chunk?, docid_fid_facet_values_chunks?))
        })
        .collect();

    let (
        docid_word_positions_chunks,
        (docid_fid_facet_numbers_chunks, docid_fid_facet_strings_chunks),
    ) = result?;

    spawn_extraction_task(
        docid_word_positions_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_word_pair_proximity_docids,
        merge_cbo_roaring_bitmaps,
        TypedChunk::WordPairProximityDocids,
        "word-pair-proximity-docids",
    );

    spawn_extraction_task(
        docid_word_positions_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_fid_word_count_docids,
        merge_cbo_roaring_bitmaps,
        TypedChunk::FieldIdWordcountDocids,
        "field-id-wordcount-docids",
    );

    spawn_extraction_task(
        docid_word_positions_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_word_docids,
        merge_roaring_bitmaps,
        TypedChunk::WordDocids,
        "word-docids",
    );

    spawn_extraction_task(
        docid_word_positions_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_word_level_position_docids,
        merge_cbo_roaring_bitmaps,
        TypedChunk::WordLevelPositionDocids,
        "word-level-position-docids",
    );

    spawn_extraction_task(
        docid_fid_facet_strings_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_facet_string_docids,
        keep_first_prefix_value_merge_roaring_bitmaps,
        TypedChunk::FieldIdFacetStringDocids,
        "field-id-facet-string-docids",
    );

    spawn_extraction_task(
        docid_fid_facet_numbers_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_facet_number_docids,
        merge_cbo_roaring_bitmaps,
        TypedChunk::FieldIdFacetNumberDocids,
        "field-id-facet-number-docids",
    );

    Ok(())
}

/// Spawn a new task to extract data for a specific DB using extract_fn.
/// Generated grenad chunks are merged using the merge_fn.
/// The result of merged chunks is serialized as TypedChunk using the serialize_fn
/// and sent into lmdb_writer_sx.
fn spawn_extraction_task<FE, FS>(
    chunks: Vec<grenad::Reader<CursorClonableMmap>>,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<TypedChunk>,
    extract_fn: FE,
    merge_fn: MergeFn,
    serialize_fn: FS,
    name: &'static str,
) where
    FE: Fn(grenad::Reader<CursorClonableMmap>, GrenadParameters) -> Result<grenad::Reader<File>>
        + Sync
        + Send
        + 'static,
    FS: Fn(grenad::Reader<File>) -> TypedChunk + Sync + Send + 'static,
{
    rayon::spawn(move || {
        let chunks: Vec<_> = chunks
            .into_par_iter()
            .map(|chunk| extract_fn(chunk, indexer.clone()).unwrap())
            .collect();
        rayon::spawn(move || {
            debug!("merge {} database", name);
            let reader = merge_readers(chunks, merge_fn, indexer).unwrap();
            lmdb_writer_sx.send(serialize_fn(reader)).unwrap();
        });
    });
}

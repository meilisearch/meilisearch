mod extract_docid_word_positions;
mod extract_facet_exists_docids;
mod extract_facet_number_docids;
mod extract_facet_string_docids;
mod extract_fid_docid_facet_values;
mod extract_fid_word_count_docids;
mod extract_geo_points;
mod extract_word_docids;
mod extract_word_pair_proximity_docids;
mod extract_word_position_docids;

use std::collections::HashSet;
use std::fs::File;

use crossbeam_channel::Sender;
use log::debug;
use rayon::prelude::*;

use self::extract_docid_word_positions::extract_docid_word_positions;
use self::extract_facet_exists_docids::extract_facet_exists_docids;
use self::extract_facet_number_docids::extract_facet_number_docids;
use self::extract_facet_string_docids::extract_facet_string_docids;
use self::extract_fid_docid_facet_values::extract_fid_docid_facet_values;
use self::extract_fid_word_count_docids::extract_fid_word_count_docids;
use self::extract_geo_points::extract_geo_points;
use self::extract_word_docids::extract_word_docids;
use self::extract_word_pair_proximity_docids::extract_word_pair_proximity_docids;
use self::extract_word_position_docids::extract_word_position_docids;
use super::helpers::{
    as_cloneable_grenad, keep_first_prefix_value_merge_roaring_bitmaps, merge_cbo_roaring_bitmaps,
    merge_roaring_bitmaps, CursorClonableMmap, GrenadParameters, MergeFn, MergeableReader,
};
use super::{helpers, TypedChunk};
use crate::{FieldId, Result};

/// Extract data for each databases from obkv documents in parallel.
/// Send data in grenad file over provided Sender.
pub(crate) fn data_from_obkv_documents(
    original_obkv_chunks: impl Iterator<Item = Result<grenad::Reader<File>>> + Send,
    flattened_obkv_chunks: impl Iterator<Item = Result<grenad::Reader<File>>> + Send,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    searchable_fields: Option<HashSet<FieldId>>,
    faceted_fields: HashSet<FieldId>,
    primary_key_id: FieldId,
    geo_fields_ids: Option<(FieldId, FieldId)>,
    stop_words: Option<fst::Set<&[u8]>>,
    max_positions_per_attributes: Option<u32>,
    exact_attributes: HashSet<FieldId>,
) -> Result<()> {
    original_obkv_chunks
        .par_bridge()
        .map(|original_documents_chunk| {
            send_original_documents_data(original_documents_chunk, lmdb_writer_sx.clone())
        })
        .collect::<Result<()>>()?;

    let result: Result<(Vec<_>, (Vec<_>, (Vec<_>, Vec<_>)))> = flattened_obkv_chunks
        .par_bridge()
        .map(|flattened_obkv_chunks| {
            send_and_extract_flattened_documents_data(
                flattened_obkv_chunks,
                indexer,
                lmdb_writer_sx.clone(),
                &searchable_fields,
                &faceted_fields,
                primary_key_id,
                geo_fields_ids,
                &stop_words,
                max_positions_per_attributes,
            )
        })
        .collect();

    let (
        docid_word_positions_chunks,
        (
            docid_fid_facet_numbers_chunks,
            (docid_fid_facet_strings_chunks, docid_fid_facet_exists_chunks),
        ),
    ) = result?;

    spawn_extraction_task::<_, _, Vec<grenad::Reader<File>>>(
        docid_word_positions_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_word_pair_proximity_docids,
        merge_cbo_roaring_bitmaps,
        TypedChunk::WordPairProximityDocids,
        "word-pair-proximity-docids",
    );

    spawn_extraction_task::<_, _, Vec<grenad::Reader<File>>>(
        docid_word_positions_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_fid_word_count_docids,
        merge_cbo_roaring_bitmaps,
        TypedChunk::FieldIdWordcountDocids,
        "field-id-wordcount-docids",
    );

    spawn_extraction_task::<_, _, Vec<(grenad::Reader<File>, grenad::Reader<File>)>>(
        docid_word_positions_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        move |doc_word_pos, indexer| extract_word_docids(doc_word_pos, indexer, &exact_attributes),
        merge_roaring_bitmaps,
        |(word_docids_reader, exact_word_docids_reader)| TypedChunk::WordDocids {
            word_docids_reader,
            exact_word_docids_reader,
        },
        "word-docids",
    );

    spawn_extraction_task::<_, _, Vec<grenad::Reader<File>>>(
        docid_word_positions_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_word_position_docids,
        merge_cbo_roaring_bitmaps,
        TypedChunk::WordPositionDocids,
        "word-position-docids",
    );

    spawn_extraction_task::<_, _, Vec<grenad::Reader<File>>>(
        docid_fid_facet_strings_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_facet_string_docids,
        keep_first_prefix_value_merge_roaring_bitmaps,
        TypedChunk::FieldIdFacetStringDocids,
        "field-id-facet-string-docids",
    );

    spawn_extraction_task::<_, _, Vec<grenad::Reader<File>>>(
        docid_fid_facet_numbers_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_facet_number_docids,
        merge_cbo_roaring_bitmaps,
        TypedChunk::FieldIdFacetNumberDocids,
        "field-id-facet-number-docids",
    );
    spawn_extraction_task::<_, _, Vec<grenad::Reader<File>>>(
        docid_fid_facet_exists_chunks.clone(),
        indexer.clone(),
        lmdb_writer_sx.clone(),
        extract_facet_exists_docids,
        merge_cbo_roaring_bitmaps,
        TypedChunk::FieldIdFacetExistsDocids,
        "field-id-facet-exists-docids",
    );

    Ok(())
}

/// Spawn a new task to extract data for a specific DB using extract_fn.
/// Generated grenad chunks are merged using the merge_fn.
/// The result of merged chunks is serialized as TypedChunk using the serialize_fn
/// and sent into lmdb_writer_sx.
fn spawn_extraction_task<FE, FS, M>(
    chunks: Vec<grenad::Reader<CursorClonableMmap>>,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    extract_fn: FE,
    merge_fn: MergeFn,
    serialize_fn: FS,
    name: &'static str,
) where
    FE: Fn(grenad::Reader<CursorClonableMmap>, GrenadParameters) -> Result<M::Output>
        + Sync
        + Send
        + 'static,
    FS: Fn(M::Output) -> TypedChunk + Sync + Send + 'static,
    M: MergeableReader + FromParallelIterator<M::Output> + Send + 'static,
    M::Output: Send,
{
    rayon::spawn(move || {
        let chunks: Result<M> =
            chunks.into_par_iter().map(|chunk| extract_fn(chunk, indexer.clone())).collect();
        rayon::spawn(move || match chunks {
            Ok(chunks) => {
                debug!("merge {} database", name);
                let reader = chunks.merge(merge_fn, &indexer);
                let _ = lmdb_writer_sx.send(reader.map(|r| serialize_fn(r)));
            }
            Err(e) => {
                let _ = lmdb_writer_sx.send(Err(e));
            }
        })
    });
}

/// Extract chunked data and send it into lmdb_writer_sx sender:
/// - documents
fn send_original_documents_data(
    original_documents_chunk: Result<grenad::Reader<File>>,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
) -> Result<()> {
    let original_documents_chunk =
        original_documents_chunk.and_then(|c| unsafe { as_cloneable_grenad(&c) })?;

    // TODO: create a custom internal error
    lmdb_writer_sx.send(Ok(TypedChunk::Documents(original_documents_chunk))).unwrap();
    Ok(())
}

/// Extract chunked data and send it into lmdb_writer_sx sender:
/// - documents_ids
/// - docid_word_positions
/// - docid_fid_facet_numbers
/// - docid_fid_facet_strings
/// - docid_fid_facet_exists
fn send_and_extract_flattened_documents_data(
    flattened_documents_chunk: Result<grenad::Reader<File>>,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    searchable_fields: &Option<HashSet<FieldId>>,
    faceted_fields: &HashSet<FieldId>,
    primary_key_id: FieldId,
    geo_fields_ids: Option<(FieldId, FieldId)>,
    stop_words: &Option<fst::Set<&[u8]>>,
    max_positions_per_attributes: Option<u32>,
) -> Result<(
    grenad::Reader<CursorClonableMmap>,
    (
        grenad::Reader<CursorClonableMmap>,
        (grenad::Reader<CursorClonableMmap>, grenad::Reader<CursorClonableMmap>),
    ),
)> {
    let flattened_documents_chunk =
        flattened_documents_chunk.and_then(|c| unsafe { as_cloneable_grenad(&c) })?;

    if let Some(geo_fields_ids) = geo_fields_ids {
        let documents_chunk_cloned = flattened_documents_chunk.clone();
        let lmdb_writer_sx_cloned = lmdb_writer_sx.clone();
        rayon::spawn(move || {
            let result =
                extract_geo_points(documents_chunk_cloned, indexer, primary_key_id, geo_fields_ids);
            let _ = match result {
                Ok(geo_points) => lmdb_writer_sx_cloned.send(Ok(TypedChunk::GeoPoints(geo_points))),
                Err(error) => lmdb_writer_sx_cloned.send(Err(error)),
            };
        });
    }

    let (docid_word_positions_chunk, docid_fid_facet_values_chunks): (Result<_>, Result<_>) =
        rayon::join(
            || {
                let (documents_ids, docid_word_positions_chunk) = extract_docid_word_positions(
                    flattened_documents_chunk.clone(),
                    indexer.clone(),
                    searchable_fields,
                    stop_words.as_ref(),
                    max_positions_per_attributes,
                )?;

                // send documents_ids to DB writer
                let _ = lmdb_writer_sx.send(Ok(TypedChunk::NewDocumentsIds(documents_ids)));

                // send docid_word_positions_chunk to DB writer
                let docid_word_positions_chunk =
                    unsafe { as_cloneable_grenad(&docid_word_positions_chunk)? };
                let _ = lmdb_writer_sx
                    .send(Ok(TypedChunk::DocidWordPositions(docid_word_positions_chunk.clone())));

                Ok(docid_word_positions_chunk)
            },
            || {
                let (
                    docid_fid_facet_numbers_chunk,
                    docid_fid_facet_strings_chunk,
                    docid_fid_facet_exists_chunk,
                ) = extract_fid_docid_facet_values(
                    flattened_documents_chunk.clone(),
                    indexer.clone(),
                    faceted_fields,
                )?;

                // send docid_fid_facet_numbers_chunk to DB writer
                let docid_fid_facet_numbers_chunk =
                    unsafe { as_cloneable_grenad(&docid_fid_facet_numbers_chunk)? };

                let _ = lmdb_writer_sx.send(Ok(TypedChunk::FieldIdDocidFacetNumbers(
                    docid_fid_facet_numbers_chunk.clone(),
                )));

                // send docid_fid_facet_strings_chunk to DB writer
                let docid_fid_facet_strings_chunk =
                    unsafe { as_cloneable_grenad(&docid_fid_facet_strings_chunk)? };

                let _ = lmdb_writer_sx.send(Ok(TypedChunk::FieldIdDocidFacetStrings(
                    docid_fid_facet_strings_chunk.clone(),
                )));

                let docid_fid_facet_exists_chunk =
                    unsafe { as_cloneable_grenad(&docid_fid_facet_exists_chunk)? };

                Ok((
                    docid_fid_facet_numbers_chunk,
                    (docid_fid_facet_strings_chunk, docid_fid_facet_exists_chunk),
                ))
            },
        );

    Ok((docid_word_positions_chunk?, docid_fid_facet_values_chunks?))
}

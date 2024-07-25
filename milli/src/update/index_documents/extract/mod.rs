mod extract_docid_word_positions;
mod extract_facet_number_docids;
mod extract_facet_string_docids;
mod extract_fid_docid_facet_values;
mod extract_fid_word_count_docids;
mod extract_geo_points;
mod extract_vector_points;
mod extract_word_docids;
mod extract_word_pair_proximity_docids;
mod extract_word_position_docids;

use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, OnceLock};

use crossbeam_channel::Sender;
use rayon::prelude::*;

use self::extract_docid_word_positions::extract_docid_word_positions;
use self::extract_facet_number_docids::extract_facet_number_docids;
use self::extract_facet_string_docids::extract_facet_string_docids;
use self::extract_fid_docid_facet_values::{extract_fid_docid_facet_values, ExtractedFacetValues};
use self::extract_fid_word_count_docids::extract_fid_word_count_docids;
use self::extract_geo_points::extract_geo_points;
use self::extract_vector_points::{
    extract_embeddings, extract_vector_points, ExtractedVectorPoints,
};
use self::extract_word_docids::extract_word_docids;
use self::extract_word_pair_proximity_docids::extract_word_pair_proximity_docids;
use self::extract_word_position_docids::extract_word_position_docids;
use super::helpers::{as_cloneable_grenad, CursorClonableMmap, GrenadParameters};
use super::{helpers, TypedChunk};
use crate::index::IndexEmbeddingConfig;
use crate::update::settings::InnerIndexSettingsDiff;
use crate::vector::error::PossibleEmbeddingMistakes;
use crate::{FieldId, Result, ThreadPoolNoAbort, ThreadPoolNoAbortBuilder};

/// Extract data for each databases from obkv documents in parallel.
/// Send data in grenad file over provided Sender.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub(crate) fn data_from_obkv_documents(
    original_obkv_chunks: impl Iterator<Item = Result<grenad::Reader<BufReader<File>>>> + Send,
    flattened_obkv_chunks: impl Iterator<Item = Result<grenad::Reader<BufReader<File>>>> + Send,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    primary_key_id: FieldId,
    embedders_configs: Arc<Vec<IndexEmbeddingConfig>>,
    settings_diff: Arc<InnerIndexSettingsDiff>,
    max_positions_per_attributes: Option<u32>,
    possible_embedding_mistakes: Arc<PossibleEmbeddingMistakes>,
) -> Result<()> {
    let (original_pipeline_result, flattened_pipeline_result): (Result<_>, Result<_>) = rayon::join(
        || {
            original_obkv_chunks
                .par_bridge()
                .map(|original_documents_chunk| {
                    send_original_documents_data(
                        original_documents_chunk,
                        indexer,
                        lmdb_writer_sx.clone(),
                        embedders_configs.clone(),
                        settings_diff.clone(),
                        possible_embedding_mistakes.clone(),
                    )
                })
                .collect::<Result<()>>()
        },
        || {
            flattened_obkv_chunks
                .par_bridge()
                .map(|flattened_obkv_chunks| {
                    send_and_extract_flattened_documents_data(
                        flattened_obkv_chunks,
                        indexer,
                        lmdb_writer_sx.clone(),
                        primary_key_id,
                        settings_diff.clone(),
                        max_positions_per_attributes,
                    )
                })
                .map(|result| {
                    if let Ok((
                        ref docid_word_positions_chunk,
                        (ref fid_docid_facet_numbers_chunk, ref fid_docid_facet_strings_chunk),
                    )) = result
                    {
                        run_extraction_task::<_, _, grenad::Reader<BufReader<File>>>(
                            docid_word_positions_chunk.clone(),
                            indexer,
                            settings_diff.clone(),
                            lmdb_writer_sx.clone(),
                            extract_fid_word_count_docids,
                            TypedChunk::FieldIdWordCountDocids,
                        );
                        run_extraction_task::<
                            _,
                            _,
                            (
                                grenad::Reader<BufReader<File>>,
                                grenad::Reader<BufReader<File>>,
                                grenad::Reader<BufReader<File>>,
                            ),
                        >(
                            docid_word_positions_chunk.clone(),
                            indexer,
                            settings_diff.clone(),
                            lmdb_writer_sx.clone(),
                            extract_word_docids,
                            |(
                                word_docids_reader,
                                exact_word_docids_reader,
                                word_fid_docids_reader,
                            )| {
                                TypedChunk::WordDocids {
                                    word_docids_reader,
                                    exact_word_docids_reader,
                                    word_fid_docids_reader,
                                }
                            },
                        );

                        run_extraction_task::<_, _, grenad::Reader<BufReader<File>>>(
                            docid_word_positions_chunk.clone(),
                            indexer,
                            settings_diff.clone(),
                            lmdb_writer_sx.clone(),
                            extract_word_position_docids,
                            TypedChunk::WordPositionDocids,
                        );

                        run_extraction_task::<
                            _,
                            _,
                            (grenad::Reader<BufReader<File>>, grenad::Reader<BufReader<File>>),
                        >(
                            fid_docid_facet_strings_chunk.clone(),
                            indexer,
                            settings_diff.clone(),
                            lmdb_writer_sx.clone(),
                            extract_facet_string_docids,
                            TypedChunk::FieldIdFacetStringDocids,
                        );

                        run_extraction_task::<_, _, grenad::Reader<BufReader<File>>>(
                            fid_docid_facet_numbers_chunk.clone(),
                            indexer,
                            settings_diff.clone(),
                            lmdb_writer_sx.clone(),
                            extract_facet_number_docids,
                            TypedChunk::FieldIdFacetNumberDocids,
                        );

                        run_extraction_task::<_, _, grenad::Reader<BufReader<File>>>(
                            docid_word_positions_chunk.clone(),
                            indexer,
                            settings_diff.clone(),
                            lmdb_writer_sx.clone(),
                            extract_word_pair_proximity_docids,
                            TypedChunk::WordPairProximityDocids,
                        );
                    }

                    Ok(())
                })
                .collect::<Result<()>>()
        },
    );

    original_pipeline_result.and(flattened_pipeline_result)
}

/// Spawn a new task to extract data for a specific DB using extract_fn.
/// Generated grenad chunks are merged using the merge_fn.
/// The result of merged chunks is serialized as TypedChunk using the serialize_fn
/// and sent into lmdb_writer_sx.
fn run_extraction_task<FE, FS, M>(
    chunk: grenad::Reader<CursorClonableMmap>,
    indexer: GrenadParameters,
    settings_diff: Arc<InnerIndexSettingsDiff>,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    extract_fn: FE,
    serialize_fn: FS,
) where
    FE: Fn(
            grenad::Reader<CursorClonableMmap>,
            GrenadParameters,
            &InnerIndexSettingsDiff,
        ) -> Result<M>
        + Sync
        + Send
        + 'static,
    FS: Fn(M) -> TypedChunk + Sync + Send + 'static,
    M: Send,
{
    let current_span = tracing::Span::current();

    rayon::spawn(move || {
        let child_span = tracing::trace_span!(target: "indexing::extract::details", parent: &current_span, "extract_multiple_chunks");
        let _entered = child_span.enter();

        match extract_fn(chunk, indexer, &settings_diff) {
            Ok(chunk) => {
                let _ = lmdb_writer_sx.send(Ok(serialize_fn(chunk)));
            }
            Err(e) => {
                let _ = lmdb_writer_sx.send(Err(e));
            }
        }
    })
}

fn request_threads() -> &'static ThreadPoolNoAbort {
    static REQUEST_THREADS: OnceLock<ThreadPoolNoAbort> = OnceLock::new();

    REQUEST_THREADS.get_or_init(|| {
        ThreadPoolNoAbortBuilder::new()
            .num_threads(crate::vector::REQUEST_PARALLELISM)
            .thread_name(|index| format!("embedding-request-{index}"))
            .build()
            .unwrap()
    })
}

/// Extract chunked data and send it into lmdb_writer_sx sender:
/// - documents
fn send_original_documents_data(
    original_documents_chunk: Result<grenad::Reader<BufReader<File>>>,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    embedders_configs: Arc<Vec<IndexEmbeddingConfig>>,
    settings_diff: Arc<InnerIndexSettingsDiff>,
    possible_embedding_mistakes: Arc<PossibleEmbeddingMistakes>,
) -> Result<()> {
    let original_documents_chunk =
        original_documents_chunk.and_then(|c| unsafe { as_cloneable_grenad(&c) })?;

    let index_vectors = (settings_diff.reindex_vectors() || !settings_diff.settings_update_only())
        // no point in indexing vectors without embedders
        && (!settings_diff.new.embedding_configs.inner_as_ref().is_empty());

    if index_vectors {
        let settings_diff = settings_diff.clone();
        let embedders_configs = embedders_configs.clone();

        let original_documents_chunk = original_documents_chunk.clone();
        let lmdb_writer_sx = lmdb_writer_sx.clone();
        rayon::spawn(move || {
            match extract_vector_points(
                original_documents_chunk.clone(),
                indexer,
                &embedders_configs,
                &settings_diff,
                &possible_embedding_mistakes,
            ) {
                Ok((extracted_vectors, unused_vectors_distribution)) => {
                    for ExtractedVectorPoints {
                        manual_vectors,
                        remove_vectors,
                        prompts,
                        embedder_name,
                        embedder,
                        add_to_user_provided,
                        remove_from_user_provided,
                    } in extracted_vectors
                    {
                        let embeddings = match extract_embeddings(
                            prompts,
                            indexer,
                            embedder.clone(),
                            &embedder_name,
                            &possible_embedding_mistakes,
                            &unused_vectors_distribution,
                            request_threads(),
                        ) {
                            Ok(results) => Some(results),
                            Err(error) => {
                                let _ = lmdb_writer_sx.send(Err(error));
                                None
                            }
                        };
                        if !(remove_vectors.is_empty()
                            && manual_vectors.is_empty()
                            && embeddings.as_ref().map_or(true, |e| e.is_empty()))
                        {
                            let _ = lmdb_writer_sx.send(Ok(TypedChunk::VectorPoints {
                                remove_vectors,
                                embeddings,
                                expected_dimension: embedder.dimensions(),
                                manual_vectors,
                                embedder_name,
                                add_to_user_provided,
                                remove_from_user_provided,
                            }));
                        }
                    }
                }
                Err(error) => {
                    let _ = lmdb_writer_sx.send(Err(error));
                }
            }
        });
    }

    // TODO: create a custom internal error
    let _ = lmdb_writer_sx.send(Ok(TypedChunk::Documents(original_documents_chunk)));
    Ok(())
}

/// Extract chunked data and send it into lmdb_writer_sx sender:
/// - documents_ids
/// - docid_word_positions
/// - docid_fid_facet_numbers
/// - docid_fid_facet_strings
/// - docid_fid_facet_exists
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
fn send_and_extract_flattened_documents_data(
    flattened_documents_chunk: Result<grenad::Reader<BufReader<File>>>,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    primary_key_id: FieldId,
    settings_diff: Arc<InnerIndexSettingsDiff>,
    max_positions_per_attributes: Option<u32>,
) -> Result<(
    grenad::Reader<CursorClonableMmap>,
    (grenad::Reader<CursorClonableMmap>, grenad::Reader<CursorClonableMmap>),
)> {
    let flattened_documents_chunk =
        flattened_documents_chunk.and_then(|c| unsafe { as_cloneable_grenad(&c) })?;

    if settings_diff.run_geo_indexing() {
        let documents_chunk_cloned = flattened_documents_chunk.clone();
        let lmdb_writer_sx_cloned = lmdb_writer_sx.clone();
        let settings_diff = settings_diff.clone();
        rayon::spawn(move || {
            let result =
                extract_geo_points(documents_chunk_cloned, indexer, primary_key_id, &settings_diff);
            let _ = match result {
                Ok(geo_points) => lmdb_writer_sx_cloned.send(Ok(TypedChunk::GeoPoints(geo_points))),
                Err(error) => lmdb_writer_sx_cloned.send(Err(error)),
            };
        });
    }

    let (docid_word_positions_chunk, fid_docid_facet_values_chunks): (Result<_>, Result<_>) =
        rayon::join(
            || {
                let docid_word_positions_chunk = extract_docid_word_positions(
                    flattened_documents_chunk.clone(),
                    indexer,
                    &settings_diff,
                    max_positions_per_attributes,
                )?;

                // send docid_word_positions_chunk to DB writer
                let docid_word_positions_chunk =
                    unsafe { as_cloneable_grenad(&docid_word_positions_chunk)? };

                Ok(docid_word_positions_chunk)
            },
            || {
                let ExtractedFacetValues {
                    fid_docid_facet_numbers_chunk,
                    fid_docid_facet_strings_chunk,
                    fid_facet_is_null_docids_chunk,
                    fid_facet_is_empty_docids_chunk,
                    fid_facet_exists_docids_chunk,
                } = extract_fid_docid_facet_values(
                    flattened_documents_chunk.clone(),
                    indexer,
                    &settings_diff,
                )?;

                // send fid_docid_facet_numbers_chunk to DB writer
                let fid_docid_facet_numbers_chunk =
                    unsafe { as_cloneable_grenad(&fid_docid_facet_numbers_chunk)? };

                let _ = lmdb_writer_sx.send(Ok(TypedChunk::FieldIdDocidFacetNumbers(
                    fid_docid_facet_numbers_chunk.clone(),
                )));

                // send fid_docid_facet_strings_chunk to DB writer
                let fid_docid_facet_strings_chunk =
                    unsafe { as_cloneable_grenad(&fid_docid_facet_strings_chunk)? };

                let _ = lmdb_writer_sx.send(Ok(TypedChunk::FieldIdDocidFacetStrings(
                    fid_docid_facet_strings_chunk.clone(),
                )));

                let _ = lmdb_writer_sx
                    .send(Ok(TypedChunk::FieldIdFacetIsNullDocids(fid_facet_is_null_docids_chunk)));

                let _ = lmdb_writer_sx.send(Ok(TypedChunk::FieldIdFacetIsEmptyDocids(
                    fid_facet_is_empty_docids_chunk,
                )));

                let _ = lmdb_writer_sx
                    .send(Ok(TypedChunk::FieldIdFacetExistsDocids(fid_facet_exists_docids_chunk)));

                Ok((fid_docid_facet_numbers_chunk, fid_docid_facet_strings_chunk))
            },
        );

    Ok((docid_word_positions_chunk?, fid_docid_facet_values_chunks?))
}

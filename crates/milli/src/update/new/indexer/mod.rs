use std::sync::atomic::AtomicBool;
use std::sync::{Once, RwLock};
use std::thread::{self, Builder};

use big_s::S;
use document_changes::{DocumentChanges, IndexingContext};
pub use document_deletion::DocumentDeletion;
pub use document_operation::{DocumentOperation, PayloadStats};
use hashbrown::HashMap;
use heed::RwTxn;
pub use partial_dump::PartialDump;
pub use update_by_function::UpdateByFunction;
pub use write::ChannelCongestion;
use write::{build_vectors, update_index, write_to_db};

use super::channel::*;
use super::steps::IndexingStep;
use super::thread_local::ThreadLocal;
use crate::documents::PrimaryKey;
use crate::fields_ids_map::metadata::{FieldIdMapWithMetadata, MetadataBuilder};
use crate::progress::Progress;
use crate::update::GrenadParameters;
use crate::vector::{ArroyWrapper, EmbeddingConfigs};
use crate::{FieldsIdsMap, GlobalFieldsIdsMap, Index, InternalError, Result, ThreadPoolNoAbort};

pub(crate) mod de;
pub mod document_changes;
mod document_deletion;
mod document_operation;
mod extract;
mod guess_primary_key;
mod partial_dump;
mod post_processing;
mod update_by_function;
mod write;

static LOG_MEMORY_METRICS_ONCE: Once = Once::new();

/// This is the main function of this crate.
///
/// Give it the output of the [`Indexer::document_changes`] method and it will execute it in the [`rayon::ThreadPool`].
///
/// TODO return stats
#[allow(clippy::too_many_arguments)] // clippy: 😝
pub fn index<'pl, 'indexer, 'index, DC, MSP>(
    wtxn: &mut RwTxn,
    index: &'index Index,
    pool: &ThreadPoolNoAbort,
    grenad_parameters: GrenadParameters,
    db_fields_ids_map: &'indexer FieldsIdsMap,
    new_fields_ids_map: FieldsIdsMap,
    new_primary_key: Option<PrimaryKey<'pl>>,
    document_changes: &DC,
    embedders: EmbeddingConfigs,
    must_stop_processing: &'indexer MSP,
    progress: &'indexer Progress,
) -> Result<ChannelCongestion>
where
    DC: DocumentChanges<'pl>,
    MSP: Fn() -> bool + Sync,
{
    let mut bbbuffers = Vec::new();
    let finished_extraction = AtomicBool::new(false);

    let arroy_memory = grenad_parameters.max_memory;

    // We reduce the actual memory used to 5%. The reason we do this here and not in Meilisearch
    // is because we still use the old indexer for the settings and it is highly impacted by the
    // max memory. So we keep the changes here and will remove these changes once we use the new
    // indexer to also index settings. Related to #5125 and #5141.
    let grenad_parameters = GrenadParameters {
        max_memory: grenad_parameters.max_memory.map(|mm| mm * 5 / 100),
        ..grenad_parameters
    };

    // 5% percent of the allocated memory for the extractors, or min 100MiB
    // 5% percent of the allocated memory for the bbqueues, or min 50MiB
    //
    // Minimum capacity for bbqueues
    let minimum_total_bbbuffer_capacity = 50 * 1024 * 1024 * pool.current_num_threads(); // 50 MiB
    let minimum_total_extractors_capacity = minimum_total_bbbuffer_capacity * 2;

    let (grenad_parameters, total_bbbuffer_capacity) = grenad_parameters.max_memory.map_or(
        (
            GrenadParameters {
                max_memory: Some(minimum_total_extractors_capacity),
                ..grenad_parameters
            },
            minimum_total_bbbuffer_capacity,
        ), // 100 MiB by thread by default
        |max_memory| {
            let total_bbbuffer_capacity = max_memory.max(minimum_total_bbbuffer_capacity);
            let new_grenad_parameters = GrenadParameters {
                max_memory: Some(max_memory.max(minimum_total_extractors_capacity)),
                ..grenad_parameters
            };
            (new_grenad_parameters, total_bbbuffer_capacity)
        },
    );

    LOG_MEMORY_METRICS_ONCE.call_once(|| {
        tracing::debug!(
            "Indexation allocated memory metrics - \
            Total BBQueue size: {total_bbbuffer_capacity}, \
            Total extractor memory: {:?}",
            grenad_parameters.max_memory,
        );
    });

    let (extractor_sender, writer_receiver) = pool
        .install(|| extractor_writer_bbqueue(&mut bbbuffers, total_bbbuffer_capacity, 1000))
        .unwrap();

    let metadata_builder = MetadataBuilder::from_index(index, wtxn)?;
    let new_fields_ids_map = FieldIdMapWithMetadata::new(new_fields_ids_map, metadata_builder);
    let new_fields_ids_map = RwLock::new(new_fields_ids_map);
    let fields_ids_map_store = ThreadLocal::with_capacity(rayon::current_num_threads());
    let mut extractor_allocs = ThreadLocal::with_capacity(rayon::current_num_threads());
    let doc_allocs = ThreadLocal::with_capacity(rayon::current_num_threads());

    let indexing_context = IndexingContext {
        index,
        db_fields_ids_map,
        new_fields_ids_map: &new_fields_ids_map,
        doc_allocs: &doc_allocs,
        fields_ids_map_store: &fields_ids_map_store,
        must_stop_processing,
        progress,
        grenad_parameters: &grenad_parameters,
    };

    let index_embeddings = index.embedding_configs(wtxn)?;
    let mut field_distribution = index.field_distribution(wtxn)?;
    let mut document_ids = index.documents_ids(wtxn)?;
    let mut modified_docids = roaring::RoaringBitmap::new();

    let congestion = thread::scope(|s| -> Result<ChannelCongestion> {
        let indexer_span = tracing::Span::current();
        let embedders = &embedders;
        let finished_extraction = &finished_extraction;
        // prevent moving the field_distribution and document_ids in the inner closure...
        let field_distribution = &mut field_distribution;
        let document_ids = &mut document_ids;
        let modified_docids = &mut modified_docids;
        let extractor_handle =
            Builder::new().name(S("indexer-extractors")).spawn_scoped(s, move || {
                pool.install(move || {
                    extract::extract_all(
                        document_changes,
                        indexing_context,
                        indexer_span,
                        extractor_sender,
                        embedders,
                        &mut extractor_allocs,
                        finished_extraction,
                        field_distribution,
                        index_embeddings,
                        document_ids,
                        modified_docids,
                    )
                })
                .unwrap()
            })?;

        let global_fields_ids_map = GlobalFieldsIdsMap::new(&new_fields_ids_map);

        let vector_arroy = index.vector_arroy;
        let arroy_writers: Result<HashMap<_, _>> = embedders
            .inner_as_ref()
            .iter()
            .map(|(embedder_name, (embedder, _, was_quantized))| {
                let embedder_index = index.embedder_category_id.get(wtxn, embedder_name)?.ok_or(
                    InternalError::DatabaseMissingEntry {
                        db_name: "embedder_category_id",
                        key: None,
                    },
                )?;

                let dimensions = embedder.dimensions();
                let writer = ArroyWrapper::new(vector_arroy, embedder_index, *was_quantized);

                Ok((
                    embedder_index,
                    (embedder_name.as_str(), embedder.as_ref(), writer, dimensions),
                ))
            })
            .collect();

        let mut arroy_writers = arroy_writers?;

        let congestion =
            write_to_db(writer_receiver, finished_extraction, index, wtxn, &arroy_writers)?;

        indexing_context.progress.update_progress(IndexingStep::WaitingForExtractors);

        let (facet_field_ids_delta, index_embeddings) = extractor_handle.join().unwrap()?;

        indexing_context.progress.update_progress(IndexingStep::WritingEmbeddingsToDatabase);

        pool.install(|| {
            build_vectors(
                index,
                wtxn,
                indexing_context.progress,
                index_embeddings,
                arroy_memory,
                &mut arroy_writers,
                &indexing_context.must_stop_processing,
            )
        })
        .unwrap()?;

        post_processing::post_process(
            indexing_context,
            wtxn,
            global_fields_ids_map,
            facet_field_ids_delta,
        )?;

        indexing_context.progress.update_progress(IndexingStep::Finalizing);

        Ok(congestion) as Result<_>
    })?;

    // required to into_inner the new_fields_ids_map
    drop(fields_ids_map_store);

    let new_fields_ids_map = new_fields_ids_map.into_inner().unwrap();
    update_index(
        index,
        wtxn,
        new_fields_ids_map,
        new_primary_key,
        embedders,
        field_distribution,
        document_ids,
    )?;

    Ok(congestion)
}

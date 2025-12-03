use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::OnceLock;

use bumpalo::Bump;
use roaring::RoaringBitmap;
use tracing::Span;

use super::super::channel::*;
use super::super::extract::*;
use super::super::steps::IndexingStep;
use super::super::thread_local::{FullySend, ThreadLocal};
use super::super::FacetFieldIdsDelta;
use super::document_changes::{extract, DocumentChanges, IndexingContext};
use super::settings_changes::settings_change_extract;
use crate::documents::{FieldIdMapper, PrimaryKey};
use crate::progress::{EmbedderStats, MergingWordCache};
use crate::proximity::ProximityPrecision;
use crate::update::new::extract::cellulite::GeoJsonExtractor;
use crate::update::new::extract::EmbeddingExtractor;
use crate::update::new::indexer::settings_changes::DocumentsIndentifiers;
use crate::update::new::merger::{merge_and_send_cellulite, merge_and_send_rtree};
use crate::update::new::{merge_and_send_docids, merge_and_send_facet_docids, FacetDatabases};
use crate::update::settings::SettingsDelta;
use crate::vector::db::{EmbedderInfo, IndexEmbeddingConfig};
use crate::vector::RuntimeEmbedders;
use crate::{Index, InternalError, Result, ThreadPoolNoAbort, ThreadPoolNoAbortBuilder};

#[allow(clippy::too_many_arguments)]
pub(super) fn extract_all<'pl, 'extractor, DC, MSP>(
    document_changes: &DC,
    indexing_context: IndexingContext<MSP>,
    indexer_span: Span,
    extractor_sender: ExtractorBbqueueSender,
    embedders: &RuntimeEmbedders,
    extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
    finished_extraction: &AtomicBool,
    field_distribution: &mut BTreeMap<String, u64>,
    mut index_embeddings: Vec<IndexEmbeddingConfig>,
    document_ids: &mut RoaringBitmap,
    modified_docids: &mut RoaringBitmap,
    embedder_stats: &EmbedderStats,
) -> Result<(FacetFieldIdsDelta, Vec<IndexEmbeddingConfig>)>
where
    DC: DocumentChanges<'pl>,
    MSP: Fn() -> bool + Sync,
{
    let span =
        tracing::trace_span!(target: "indexing::documents", parent: &indexer_span, "extract");
    let _entered = span.enter();

    let index = indexing_context.index;
    let rtxn = index.read_txn()?;

    // document but we need to create a function that collects and compresses documents.
    let document_sender = extractor_sender.documents();
    let document_extractor = DocumentsExtractor::new(document_sender, embedders);
    let datastore = ThreadLocal::with_capacity(rayon::current_num_threads());
    {
        let span = tracing::trace_span!(target: "indexing::documents::extract", parent: &indexer_span, "documents");
        let _entered = span.enter();
        extract(
            document_changes,
            &document_extractor,
            indexing_context,
            extractor_allocs,
            &datastore,
            IndexingStep::ExtractingDocuments,
        )?;
    }
    {
        let span = tracing::trace_span!(target: "indexing::documents::merge", parent: &indexer_span, "documents");
        let _entered = span.enter();
        for document_extractor_data in datastore {
            let document_extractor_data = document_extractor_data.0.into_inner();
            for (field, delta) in document_extractor_data.field_distribution_delta {
                let current = field_distribution.entry(field).or_default();
                // adding the delta should never cause a negative result, as we are removing fields that previously existed.
                *current = current.saturating_add_signed(delta);
            }
            document_extractor_data.docids_delta.apply_to(document_ids, modified_docids);
        }

        field_distribution.retain(|_, v| *v != 0);
    }

    let facet_field_ids_delta;

    {
        let caches = {
            let span = tracing::trace_span!(target: "indexing::documents::extract", parent: &indexer_span, "faceted");
            let _entered = span.enter();

            FacetedDocidsExtractor::run_extraction(
                document_changes,
                indexing_context,
                extractor_allocs,
                &extractor_sender.field_id_docid_facet_sender(),
                IndexingStep::ExtractingFacets,
            )?
        };

        {
            let span = tracing::trace_span!(target: "indexing::documents::merge", parent: &indexer_span, "faceted");
            let _entered = span.enter();
            indexing_context.progress.update_progress(IndexingStep::MergingFacetCaches);

            facet_field_ids_delta = merge_and_send_facet_docids(
                caches,
                FacetDatabases::new(index),
                index,
                &rtxn,
                extractor_sender.facet_docids(),
            )?;
        }
    }

    {
        let WordDocidsCaches {
            word_docids,
            word_fid_docids,
            exact_word_docids,
            word_position_docids,
            fid_word_count_docids,
        } = {
            let span = tracing::trace_span!(target: "indexing::documents::extract", "word_docids");
            let _entered = span.enter();
            WordDocidsExtractors::run_extraction(
                document_changes,
                indexing_context,
                extractor_allocs,
                IndexingStep::ExtractingWords,
            )?
        };

        indexing_context.progress.update_progress(IndexingStep::MergingWordCaches);

        {
            let span = tracing::trace_span!(target: "indexing::documents::merge", "word_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::WordDocids);

            merge_and_send_docids(
                word_docids,
                index.word_docids.remap_types(),
                index,
                extractor_sender.docids::<WordDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::merge", "word_fid_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::WordFieldIdDocids);

            merge_and_send_docids(
                word_fid_docids,
                index.word_fid_docids.remap_types(),
                index,
                extractor_sender.docids::<WordFidDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::merge", "exact_word_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::ExactWordDocids);

            merge_and_send_docids(
                exact_word_docids,
                index.exact_word_docids.remap_types(),
                index,
                extractor_sender.docids::<ExactWordDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::merge", "word_position_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::WordPositionDocids);

            merge_and_send_docids(
                word_position_docids,
                index.word_position_docids.remap_types(),
                index,
                extractor_sender.docids::<WordPositionDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::merge", "fid_word_count_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::FieldIdWordCountDocids);

            merge_and_send_docids(
                fid_word_count_docids,
                index.field_id_word_count_docids.remap_types(),
                index,
                extractor_sender.docids::<FidWordCountDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }
    }

    // run the proximity extraction only if the precision is by word
    // this works only if the settings didn't change during this transaction.
    let proximity_precision = index.proximity_precision(&rtxn)?.unwrap_or_default();
    if proximity_precision == ProximityPrecision::ByWord {
        let caches = {
            let span = tracing::trace_span!(target: "indexing::documents::extract", "word_pair_proximity_docids");
            let _entered = span.enter();

            WordPairProximityDocidsExtractor::run_extraction(
                document_changes,
                indexing_context,
                extractor_allocs,
                IndexingStep::ExtractingWordProximity,
            )?
        };

        {
            let span = tracing::trace_span!(target: "indexing::documents::merge", "word_pair_proximity_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(IndexingStep::MergingWordProximity);

            merge_and_send_docids(
                caches,
                index.word_pair_proximity_docids.remap_types(),
                index,
                extractor_sender.docids::<WordPairProximityDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }
    }

    'vectors: {
        if index_embeddings.is_empty() {
            break 'vectors;
        }

        let embedding_sender = extractor_sender.embeddings();
        let extractor = EmbeddingExtractor::new(
            embedders,
            embedding_sender,
            field_distribution,
            embedder_stats,
            request_threads(),
        );
        let mut datastore = ThreadLocal::with_capacity(rayon::current_num_threads());
        {
            let span = tracing::debug_span!(target: "indexing::documents::extract", "vectors");
            let _entered = span.enter();

            extract(
                document_changes,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
                IndexingStep::ExtractingEmbeddings,
            )?;
        }
        {
            let span = tracing::debug_span!(target: "indexing::documents::merge", "vectors");
            let _entered = span.enter();

            let embedder_configs = index.embedding_configs();
            for config in &mut index_embeddings {
                let mut infos = embedder_configs.embedder_info(&rtxn, &config.name)?.unwrap();

                'data: for data in datastore.iter_mut() {
                    let data = &mut data.get_mut().0;
                    let Some(delta) = data.remove(&config.name) else {
                        continue 'data;
                    };
                    delta.apply_to(&mut infos.embedding_status);
                }

                extractor_sender.embeddings().embedding_status(&config.name, infos).unwrap();
            }
        }
    }

    'geo: {
        let Some(extractor) = GeoExtractor::new(&rtxn, index, *indexing_context.grenad_parameters)?
        else {
            break 'geo;
        };
        let datastore = ThreadLocal::with_capacity(rayon::current_num_threads());

        {
            let span = tracing::trace_span!(target: "indexing::documents::extract", "geo");
            let _entered = span.enter();

            extract(
                document_changes,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
                IndexingStep::WritingGeoPoints,
            )?;
        }

        merge_and_send_rtree(
            datastore,
            &rtxn,
            index,
            extractor_sender.geo(),
            &indexing_context.must_stop_processing,
        )?;
    }

    'cellulite: {
        let Some(extractor) =
            GeoJsonExtractor::new(&rtxn, index, *indexing_context.grenad_parameters)?
        else {
            break 'cellulite;
        };
        let datastore = ThreadLocal::with_capacity(rayon::current_num_threads());

        {
            let span = tracing::trace_span!(target: "indexing::documents::extract", "cellulite");
            let _entered = span.enter();

            extract(
                document_changes,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
                IndexingStep::WritingGeoJson,
            )?;
        }

        merge_and_send_cellulite(
            datastore,
            &rtxn,
            index,
            extractor_sender.geojson(),
            &indexing_context.must_stop_processing,
        )?;
    }
    indexing_context.progress.update_progress(IndexingStep::WaitingForDatabaseWrites);
    finished_extraction.store(true, std::sync::atomic::Ordering::Relaxed);

    Result::Ok((facet_field_ids_delta, index_embeddings))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn extract_all_settings_changes<MSP, SD>(
    indexing_context: IndexingContext<MSP>,
    indexer_span: Span,
    extractor_sender: ExtractorBbqueueSender,
    settings_delta: &SD,
    extractor_allocs: &mut ThreadLocal<FullySend<Bump>>,
    finished_extraction: &AtomicBool,
    field_distribution: &mut BTreeMap<String, u64>,
    mut index_embeddings: Vec<IndexEmbeddingConfig>,
    embedder_stats: &EmbedderStats,
) -> Result<Vec<IndexEmbeddingConfig>>
where
    MSP: Fn() -> bool + Sync,
    SD: SettingsDelta + Sync,
{
    // Create the list of document ids to extract
    let index = indexing_context.index;
    let rtxn = index.read_txn()?;
    let all_document_ids = index.documents_ids(&rtxn)?.into_iter().collect::<Vec<_>>();
    let primary_key = primary_key_from_db(index, &rtxn, &indexing_context.db_fields_ids_map)?;
    let documents = DocumentsIndentifiers::new(&all_document_ids, primary_key);

    let span =
        tracing::trace_span!(target: "indexing::documents", parent: &indexer_span, "extract");
    let _entered = span.enter();

    update_database_documents(
        &documents,
        indexing_context,
        &extractor_sender,
        settings_delta,
        extractor_allocs,
    )?;

    {
        let WordDocidsCaches {
            word_docids,
            word_fid_docids,
            exact_word_docids,
            word_position_docids,
            fid_word_count_docids,
        } = {
            let span = tracing::trace_span!(target: "indexing::documents::extract", "word_docids");
            let _entered = span.enter();
            SettingsChangeWordDocidsExtractors::run_extraction(
                settings_delta,
                &documents,
                indexing_context,
                extractor_allocs,
                IndexingStep::ExtractingWords,
            )?
        };

        indexing_context.progress.update_progress(IndexingStep::MergingWordCaches);

        {
            let span = tracing::trace_span!(target: "indexing::documents::merge", "word_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::WordDocids);

            merge_and_send_docids(
                word_docids,
                index.word_docids.remap_types(),
                index,
                extractor_sender.docids::<WordDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::merge", "word_fid_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::WordFieldIdDocids);

            merge_and_send_docids(
                word_fid_docids,
                index.word_fid_docids.remap_types(),
                index,
                extractor_sender.docids::<WordFidDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::merge", "exact_word_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::ExactWordDocids);

            merge_and_send_docids(
                exact_word_docids,
                index.exact_word_docids.remap_types(),
                index,
                extractor_sender.docids::<ExactWordDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::merge", "word_position_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::WordPositionDocids);

            merge_and_send_docids(
                word_position_docids,
                index.word_position_docids.remap_types(),
                index,
                extractor_sender.docids::<WordPositionDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::merge", "fid_word_count_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(MergingWordCache::FieldIdWordCountDocids);

            merge_and_send_docids(
                fid_word_count_docids,
                index.field_id_word_count_docids.remap_types(),
                index,
                extractor_sender.docids::<FidWordCountDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }
    }

    // Run the proximity extraction only if the precision is ByWord.
    let new_proximity_precision = settings_delta.new_proximity_precision();
    if *new_proximity_precision == ProximityPrecision::ByWord {
        let caches = {
            let span = tracing::trace_span!(target: "indexing::documents::extract", "word_pair_proximity_docids");
            let _entered = span.enter();

            SettingsChangeWordPairProximityDocidsExtractors::run_extraction(
                settings_delta,
                &documents,
                indexing_context,
                extractor_allocs,
                IndexingStep::ExtractingWordProximity,
            )?
        };

        {
            let span = tracing::trace_span!(target: "indexing::documents::merge", "word_pair_proximity_docids");
            let _entered = span.enter();
            indexing_context.progress.update_progress(IndexingStep::MergingWordProximity);

            merge_and_send_docids(
                caches,
                index.word_pair_proximity_docids.remap_types(),
                index,
                extractor_sender.docids::<WordPairProximityDocids>(),
                &indexing_context.must_stop_processing,
            )?;
        }
    }

    'vectors: {
        if settings_delta.embedder_actions().is_empty() {
            break 'vectors;
        }

        let embedding_sender = extractor_sender.embeddings();

        // extract the remaining embeddings
        let extractor = SettingsChangeEmbeddingExtractor::new(
            settings_delta,
            embedder_stats,
            embedding_sender,
            field_distribution,
            request_threads(),
        );
        let mut datastore = ThreadLocal::with_capacity(rayon::current_num_threads());
        {
            let span = tracing::debug_span!(target: "indexing::documents::extract", "vectors");
            let _entered = span.enter();

            settings_change_extract(
                &documents,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
                IndexingStep::ExtractingEmbeddings,
            )?;
        }
        {
            let span = tracing::debug_span!(target: "indexing::documents::merge", "vectors");
            let _entered = span.enter();

            let embedder_configs = indexing_context.index.embedding_configs();
            for config in &mut index_embeddings {
                // retrieve infos for existing embedder or create a fresh one
                let mut infos =
                    embedder_configs.embedder_info(&rtxn, &config.name)?.unwrap_or_else(|| {
                        let embedder_id =
                            *settings_delta.new_embedder_category_id().get(&config.name).unwrap();
                        EmbedderInfo { embedder_id, embedding_status: Default::default() }
                    });

                'data: for data in datastore.iter_mut() {
                    let data = &mut data.get_mut().0;
                    let Some(delta) = data.remove(&config.name) else {
                        continue 'data;
                    };
                    delta.apply_to(&mut infos.embedding_status);
                }

                extractor_sender.embeddings().embedding_status(&config.name, infos).unwrap();
            }
        }
    }

    indexing_context.progress.update_progress(IndexingStep::WaitingForDatabaseWrites);
    finished_extraction.store(true, std::sync::atomic::Ordering::Relaxed);

    Result::Ok(index_embeddings)
}

fn primary_key_from_db<'indexer>(
    index: &'indexer Index,
    rtxn: &'indexer heed::RoTxn<'_>,
    fields: &'indexer impl FieldIdMapper,
) -> Result<PrimaryKey<'indexer>> {
    let Some(primary_key) = index.primary_key(rtxn)? else {
        return Err(InternalError::DatabaseMissingEntry {
            db_name: crate::index::db_name::MAIN,
            key: Some(crate::index::main_key::PRIMARY_KEY_KEY),
        }
        .into());
    };
    let Some(primary_key) = PrimaryKey::new(primary_key, fields) else {
        unreachable!("Primary key must exist at this point");
    };
    Ok(primary_key)
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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bumpalo::collections::CollectIn;
use bumpalo::Bump;
use meilisearch_types::dynamic_search_rules::{DynamicSearchRule, RuleUid};
use meilisearch_types::error::Code;
use meilisearch_types::heed::RwTxn;
use meilisearch_types::index_uid::DsrIndex;
use meilisearch_types::milli::documents::PrimaryKey;
use meilisearch_types::milli::dynamic_search_rules::DynamicSearchRulesView;
use meilisearch_types::milli::progress::{EmbedderStats, Progress};
use meilisearch_types::milli::update::new::indexer::{
    self, IndexOperations, Payload, UpdateByFunction,
};
use meilisearch_types::milli::update::{DocumentAdditionResult, Setting};
use meilisearch_types::milli::vector::RuntimeEmbedders;
use meilisearch_types::milli::{
    self, ChannelCongestion, FaultSource, FilterFeatures, FilterableAttributesFeatures,
    FilterableAttributesPatterns, FilterableAttributesRule, MustStopProcessing,
};
use meilisearch_types::network::Network;
use meilisearch_types::settings::{apply_settings_to_builder, Settings, TypoSettings};
use meilisearch_types::tasks::{Details, DsrUpdate, KindWithContent, Status, Task};
use meilisearch_types::Index;
use roaring::RoaringBitmap;

use super::create_batch::{DocumentOperation, IndexOperation};
use crate::filter::parse_local_index_filter;
use crate::processing::{
    DocumentDeletionProgress, DocumentEditionProgress, DocumentOperationProgress, SettingsProgress,
};
use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    /// Process the index operation on the given index.
    ///
    /// ## Return
    /// The list of processed tasks.
    #[tracing::instrument(
        level = "trace",
        skip(self, index_wtxn, index, progress, embedder_stats),
        target = "indexing::scheduler"
    )]
    pub(crate) fn apply_index_operation<'i>(
        &self,
        index_wtxn: &mut RwTxn<'i>,
        index: &'i Index,
        operation: IndexOperation,
        progress: &Progress,
        embedder_stats: Arc<EmbedderStats>,
        network: &Network,
    ) -> Result<(Vec<Task>, Option<ChannelCongestion>)> {
        let indexer_alloc = Bump::new();
        let started_processing_at = std::time::Instant::now();
        let must_stop_processing = self.scheduler.must_stop_processing.clone();

        match operation {
            IndexOperation::DocumentClear { index_uid, mut tasks } => {
                let count = milli::update::ClearDocuments::new(index_wtxn, index)
                    .execute()
                    .map_err(|e| Error::from_milli(e, Some(index_uid)))?;

                let mut first_clear_found = false;
                for task in &mut tasks {
                    task.status = Status::Succeeded;
                    // The first document clear will effectively delete every documents
                    // in the database but the next ones will clear 0 documents.
                    task.details = match &task.kind {
                        KindWithContent::DocumentClear { .. } => {
                            let count = if first_clear_found { 0 } else { count };
                            first_clear_found = true;
                            Some(Details::ClearAll { deleted_documents: Some(count) })
                        }
                        otherwise => otherwise.default_details(),
                    };
                }

                Ok((tasks, None))
            }
            IndexOperation::DocumentOperation { index_uid, primary_key, operations, mut tasks } => {
                progress.update_progress(DocumentOperationProgress::RetrievingConfig);

                let shards = network.shards();

                // TODO: at some point, for better efficiency we might want to reuse the bumpalo for successive batches.
                // this is made difficult by the fact we're doing private clones of the index scheduler and sending it
                // to a fresh thread.
                let mut content_files = Vec::new();
                for operation in &operations {
                    match operation {
                        DocumentOperation::Replace { content_file: content_uuid, .. }
                        | DocumentOperation::Update { content_file: content_uuid, .. } => {
                            let content_file = self.queue.file_store.get_update(*content_uuid)?;
                            let mmap = unsafe { memmap2::Mmap::map(&content_file)? };
                            content_files.push(mmap);
                        }
                        _ => (),
                    }
                }

                let rtxn = index.read_txn()?;
                let db_fields_ids_map = index.fields_ids_map(&rtxn)?;
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut content_files_iter = content_files.iter();
                let mut indexer = indexer::IndexOperations::new();
                let embedders = index
                    .embedding_configs()
                    .embedding_configs(index_wtxn)
                    .map_err(|e| Error::from_milli(e.into(), Some(index_uid.clone())))?;
                let embedders = self.embedders(index_uid.clone(), embedders)?;
                for operation in operations {
                    match operation {
                        DocumentOperation::Replace { content_file: _, on_missing_document } => {
                            let mmap = content_files_iter.next().unwrap();
                            indexer
                                .replace_documents(mmap, on_missing_document)
                                .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?;
                        }
                        DocumentOperation::Update { content_file: _, on_missing_document } => {
                            let mmap = content_files_iter.next().unwrap();
                            indexer
                                .update_documents(mmap, on_missing_document)
                                .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?;
                        }
                        DocumentOperation::Delete(document_ids) => {
                            let document_ids: bumpalo::collections::vec::Vec<_> = document_ids
                                .iter()
                                .map(|s| &*indexer_alloc.alloc_str(s))
                                .collect_in(&indexer_alloc);
                            indexer
                                .delete_documents_by_external_ids(document_ids.into_bump_slice());
                        }
                        DocumentOperation::DeleteByFilter { filter } => {
                            let filter = parse_local_index_filter(
                                &filter,
                                Some(index_uid.as_str()),
                                self.features(),
                                Code::InvalidDocumentFilter,
                            )?;
                            if let Some(filter) = filter {
                                let candidates =
                                    filter.evaluate(index_wtxn, index).map_err(|err| {
                                        Error::from_milli(err, Some(index_uid.clone()))
                                    })?;
                                indexer.delete_documents_by_internal_ids(candidates);
                            }
                        }
                    }
                }

                let indexer_config = self.index_mapper.indexer_config();
                let pool = &indexer_config.thread_pool;

                progress.update_progress(DocumentOperationProgress::ComputingDocumentChanges);
                let (document_changes, operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        index,
                        &rtxn,
                        primary_key.as_deref(),
                        &mut new_fields_ids_map,
                        &must_stop_processing,
                        progress.clone(),
                        shards.as_ref(),
                    )
                    .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?;

                progress.update_progress(DocumentOperationProgress::ReadingPayloadStats);
                let mut candidates_count = 0;
                for (stats, task) in operation_stats.into_iter().zip(&mut tasks) {
                    candidates_count += stats.document_count;
                    match stats.error {
                        Some(error) => {
                            task.status = Status::Failed;
                            task.error = Some(milli::Error::UserError(error).into());
                        }
                        None => task.status = Status::Succeeded,
                    }

                    task.details = match task.details {
                        Some(Details::DocumentAdditionOrUpdate { received_documents, .. }) => {
                            Some(Details::DocumentAdditionOrUpdate {
                                received_documents,
                                indexed_documents: Some(stats.document_count),
                            })
                        }
                        Some(Details::DocumentDeletion { provided_ids, .. }) => {
                            Some(Details::DocumentDeletion {
                                provided_ids,
                                deleted_documents: Some(stats.document_count),
                            })
                        }
                        Some(Details::DocumentDeletionByFilter { ref original_filter, .. }) => {
                            Some(Details::DocumentDeletionByFilter {
                                original_filter: original_filter.clone(),
                                deleted_documents: Some(stats.document_count),
                            })
                        }
                        _ => {
                            // In the case of a `documentAdditionOrUpdate` or `DocumentDeletion`
                            // the details MUST be set to either addition or deletion
                            unreachable!();
                        }
                    }
                }

                progress.update_progress(DocumentOperationProgress::Indexing);
                let mut congestion = None;
                if tasks.iter().any(|res| res.error.is_none()) {
                    congestion = Some(
                        indexer::index(
                            index_wtxn,
                            index,
                            pool,
                            indexer_config.grenad_parameters(),
                            &db_fields_ids_map,
                            new_fields_ids_map,
                            primary_key,
                            &document_changes,
                            embedders,
                            &must_stop_processing,
                            progress,
                            self.ip_policy(),
                            &embedder_stats,
                        )
                        .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?,
                    );

                    let addition = DocumentAdditionResult {
                        indexed_documents: candidates_count,
                        number_of_documents: index
                            .number_of_documents(index_wtxn)
                            .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?,
                    };

                    tracing::info!(indexing_result = ?addition, processed_in = ?started_processing_at.elapsed(), "document indexing done");
                }

                Ok((tasks, congestion))
            }
            IndexOperation::DocumentEdition { index_uid, mut task } => {
                progress.update_progress(DocumentEditionProgress::RetrievingConfig);

                let (filter, code) = if let KindWithContent::DocumentEdition {
                    filter_expr,
                    context: _,
                    function,
                    ..
                } = &task.kind
                {
                    (filter_expr, function)
                } else {
                    unreachable!()
                };

                let candidates = match filter
                    .as_ref()
                    .and_then(|f| {
                        parse_local_index_filter(
                            f,
                            Some(index_uid.as_str()),
                            self.features(),
                            Code::InvalidDocumentFilter,
                        )
                        .transpose()
                    })
                    .transpose()?
                {
                    Some(filter) => filter
                        .evaluate(index_wtxn, index)
                        .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?,
                    None => index.documents_ids(index_wtxn)?,
                };

                let (original_filter, context, function) = if let Some(Details::DocumentEdition {
                    original_filter,
                    context,
                    function,
                    ..
                }) = task.details
                {
                    (original_filter, context, function)
                } else {
                    // In the case of a `documentEdition` the details MUST be set
                    unreachable!();
                };

                if candidates.is_empty() {
                    task.status = Status::Succeeded;
                    task.details = Some(Details::DocumentEdition {
                        original_filter,
                        context,
                        function,
                        deleted_documents: Some(0),
                        edited_documents: Some(0),
                    });

                    return Ok((vec![task], None));
                }

                let rtxn = index.read_txn()?;
                let db_fields_ids_map = index.fields_ids_map(&rtxn)?;
                let mut new_fields_ids_map = db_fields_ids_map.clone();
                // candidates not empty => index not empty => a primary key is set
                let primary_key = index.primary_key(&rtxn)?.unwrap();

                let primary_key =
                    PrimaryKey::new_or_insert(primary_key, &mut new_fields_ids_map)
                        .map_err(|err| Error::from_milli(err.into(), Some(index_uid.clone())))?;

                let result_count = Ok((candidates.len(), candidates.len())) as Result<_>;

                let mut congestion = None;
                if task.error.is_none() {
                    let indexer_config = self.index_mapper.indexer_config();
                    let pool = &indexer_config.thread_pool;

                    let candidates_count = candidates.len();
                    progress.update_progress(DocumentEditionProgress::ComputingDocumentChanges);
                    let indexer = UpdateByFunction::new(candidates, context.clone(), code.clone());
                    let document_changes = pool
                        .install(|| {
                            indexer
                                .into_changes(&primary_key)
                                .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))
                        })
                        .unwrap()?;
                    let embedders = index
                        .embedding_configs()
                        .embedding_configs(index_wtxn)
                        .map_err(|err| Error::from_milli(err.into(), Some(index_uid.clone())))?;
                    let embedders = self.embedders(index_uid.clone(), embedders)?;

                    progress.update_progress(DocumentEditionProgress::Indexing);
                    congestion = Some(
                        indexer::index(
                            index_wtxn,
                            index,
                            pool,
                            indexer_config.grenad_parameters(),
                            &db_fields_ids_map,
                            new_fields_ids_map,
                            None, // cannot change primary key in DocumentEdition
                            &document_changes,
                            embedders,
                            &must_stop_processing,
                            progress,
                            self.ip_policy(),
                            &embedder_stats,
                        )
                        .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?,
                    );

                    let addition = DocumentAdditionResult {
                        indexed_documents: candidates_count,
                        number_of_documents: index
                            .number_of_documents(index_wtxn)
                            .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?,
                    };

                    tracing::info!(indexing_result = ?addition, processed_in = ?started_processing_at.elapsed(), "document indexing done");
                }

                match result_count {
                    Ok((deleted_documents, edited_documents)) => {
                        task.status = Status::Succeeded;
                        task.details = Some(Details::DocumentEdition {
                            original_filter,
                            context,
                            function,
                            deleted_documents: Some(deleted_documents),
                            edited_documents: Some(edited_documents),
                        });
                    }
                    Err(e) => {
                        task.status = Status::Failed;
                        task.details = Some(Details::DocumentEdition {
                            original_filter,
                            context,
                            function,
                            deleted_documents: Some(0),
                            edited_documents: Some(0),
                        });
                        task.error = Some(e.into());
                    }
                }

                Ok((vec![task], congestion))
            }
            IndexOperation::DocumentDeletion { mut tasks, index_uid } => {
                progress.update_progress(DocumentDeletionProgress::RetrievingConfig);

                let mut to_delete = RoaringBitmap::new();
                let external_documents_ids = index.external_documents_ids();

                for task in tasks.iter_mut() {
                    let before = to_delete.len();
                    task.status = Status::Succeeded;

                    match &task.kind {
                        KindWithContent::DocumentDeletion { index_uid: _, documents_ids } => {
                            for id in documents_ids {
                                if let Some(id) = external_documents_ids.get(index_wtxn, id)? {
                                    to_delete.insert(id);
                                }
                            }
                            let will_be_removed = to_delete.len() - before;
                            task.details = Some(Details::DocumentDeletion {
                                provided_ids: documents_ids.len(),
                                deleted_documents: Some(will_be_removed),
                            });
                        }
                        KindWithContent::DocumentDeletionByFilter { index_uid, filter_expr } => {
                            let before = to_delete.len();
                            let filter = match parse_local_index_filter(
                                filter_expr,
                                Some(index_uid.as_str()),
                                self.features(),
                                Code::InvalidDocumentFilter,
                            ) {
                                Ok(filter) => filter,
                                Err(err) => {
                                    // theorically, this should be caught by deserr before reaching the index-scheduler and cannot happens
                                    task.status = Status::Failed;
                                    task.error = Some(err.into());
                                    None
                                }
                            };
                            if let Some(filter) = filter {
                                let candidates = filter
                                    .evaluate(index_wtxn, index)
                                    .map_err(|err| Error::from_milli(err, Some(index_uid.clone())));
                                match candidates {
                                    Ok(candidates) => to_delete |= candidates,
                                    Err(err) => {
                                        task.status = Status::Failed;
                                        task.error = Some(err.into());
                                    }
                                };
                            }
                            let will_be_removed = to_delete.len() - before;
                            if let Some(Details::DocumentDeletionByFilter {
                                original_filter: _,
                                deleted_documents,
                            }) = &mut task.details
                            {
                                *deleted_documents = Some(will_be_removed);
                            } else {
                                // In the case of a `documentDeleteByFilter` the details MUST be set
                                unreachable!()
                            }
                        }
                        _ => unreachable!(),
                    }
                }

                if to_delete.is_empty() {
                    return Ok((tasks, None));
                }

                let rtxn = index.read_txn()?;
                let db_fields_ids_map = index.fields_ids_map(&rtxn)?;
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                // to_delete not empty => index not empty => primary key set
                let primary_key = index.primary_key(&rtxn)?.unwrap();

                let primary_key =
                    PrimaryKey::new_or_insert(primary_key, &mut new_fields_ids_map)
                        .map_err(|err| Error::from_milli(err.into(), Some(index_uid.clone())))?;

                let mut congestion = None;
                if !tasks.iter().all(|res| res.error.is_some()) {
                    let indexer_config = self.index_mapper.indexer_config();
                    let pool = &indexer_config.thread_pool;

                    progress.update_progress(DocumentDeletionProgress::DeleteDocuments);
                    let mut indexer = indexer::DocumentDeletion::new();
                    let candidates_count = to_delete.len();
                    indexer.delete_documents_by_docids(to_delete);
                    let document_changes = indexer.into_changes(&indexer_alloc, primary_key);
                    let embedders = index
                        .embedding_configs()
                        .embedding_configs(index_wtxn)
                        .map_err(|err| Error::from_milli(err.into(), Some(index_uid.clone())))?;
                    let embedders = self.embedders(index_uid.clone(), embedders)?;

                    progress.update_progress(DocumentDeletionProgress::Indexing);
                    congestion = Some(
                        indexer::index(
                            index_wtxn,
                            index,
                            pool,
                            indexer_config.grenad_parameters(),
                            &db_fields_ids_map,
                            new_fields_ids_map,
                            None, // document deletion never changes primary key
                            &document_changes,
                            embedders,
                            &must_stop_processing,
                            progress,
                            self.ip_policy(),
                            &embedder_stats,
                        )
                        .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?,
                    );

                    let addition = DocumentAdditionResult {
                        indexed_documents: candidates_count,
                        number_of_documents: index
                            .number_of_documents(index_wtxn)
                            .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?,
                    };

                    tracing::info!(indexing_result = ?addition, processed_in = ?started_processing_at.elapsed(), "document indexing done");
                }

                Ok((tasks, congestion))
            }
            IndexOperation::Settings { index_uid, settings, mut tasks } => {
                progress.update_progress(SettingsProgress::RetrievingAndMergingTheSettings);
                let indexer_config = self.index_mapper.indexer_config();
                let mut builder = milli::update::Settings::new(index_wtxn, index, indexer_config);

                for (task, (_, settings)) in tasks.iter_mut().zip(settings) {
                    let checked_settings = settings.clone().check();
                    task.details = Some(Details::SettingsUpdate { settings: Box::new(settings) });
                    apply_settings_to_builder(&checked_settings, &mut builder);

                    // We can apply the status right now and if an update fail later
                    // the whole batch will be marked as failed.
                    task.status = Status::Succeeded;
                }

                progress.update_progress(SettingsProgress::ApplyTheSettings);
                let congestion = builder
                    .execute(&must_stop_processing, progress, self.ip_policy(), embedder_stats)
                    .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?;

                Ok((tasks, congestion))
            }
            IndexOperation::DocumentClearAndSetting {
                index_uid,
                cleared_tasks,
                settings,
                settings_tasks,
            } => {
                let (mut import_tasks, _congestion) = self.apply_index_operation(
                    index_wtxn,
                    index,
                    IndexOperation::DocumentClear {
                        index_uid: index_uid.clone(),
                        tasks: cleared_tasks,
                    },
                    progress,
                    embedder_stats.clone(),
                    network,
                )?;

                let (settings_tasks, _congestion) = self.apply_index_operation(
                    index_wtxn,
                    index,
                    IndexOperation::Settings { index_uid, settings, tasks: settings_tasks },
                    progress,
                    embedder_stats,
                    network,
                )?;

                let mut tasks = settings_tasks;
                tasks.append(&mut import_tasks);
                Ok((tasks, None))
            }
        }
    }

    pub(crate) fn apply_dsr_settings<'i>(
        &self,
        index_wtxn: &mut RwTxn<'i>,
        index: &'i Index,
        progress: &Progress,
        must_stop_processing: &MustStopProcessing,
        embedder_stats: Arc<EmbedderStats>,
    ) -> Result<Option<ChannelCongestion>> {
        use milli::dynamic_search_rules::fields as dsr_fields;

        progress.update_progress(SettingsProgress::RetrievingAndMergingTheSettings);
        let indexer_config = self.index_mapper.indexer_config();
        let mut builder = milli::update::Settings::new(index_wtxn, index, indexer_config);

        let checked_settings = Settings {
            displayed_attributes: Setting::Set(vec!["*".to_string()]).into(),
            searchable_attributes: Setting::Set(vec![
                // used to find query word constraints
                dsr_fields::CONDITIONS_QUERY_WORDS.to_string(),
                // used in list rules
                dsr_fields::DESCRIPTION.to_string(),
            ])
            .into(),
            filterable_attributes: Setting::Set(vec![
                // filter by active or inactive rules
                eq_attr_pattern(dsr_fields::ACTIVE.into()),
                // used to find time constraints
                cmp_attr_pattern(dsr_fields::CONDITIONS_TIME_START.into()),
                // used to find time constraints
                cmp_attr_pattern(dsr_fields::CONDITIONS_TIME_END.into()),
                // used to find query isEmpty constraints
                eq_attr_pattern(dsr_fields::CONDITIONS_QUERY_IS_EMPTY.into()),
                // used to find filter constraints
                cmp_attr_pattern(dsr_fields::CONDITIONS_FILTER_VALUES.into()),
                // use to count filter constraints
                cmp_attr_pattern(dsr_fields::CONDITIONS_FILTER_NB_CONSTRAINTS.into()),
            ]),
            sortable_attributes: {
                let mut sortable_attributes: BTreeSet<_> = Default::default();
                // used to sort rules by precedence in responses
                sortable_attributes.insert(dsr_fields::PRECEDENCE.to_string());
                // used to sort rules by last update when listing rules
                sortable_attributes.insert(dsr_fields::LAST_UPDATED_AT.to_string());
                Setting::Set(sortable_attributes)
            },
            foreign_keys: Setting::NotSet,
            ranking_rules: Setting::NotSet,
            stop_words: Setting::NotSet,
            non_separator_tokens: Setting::NotSet,
            separator_tokens: Setting::NotSet,
            dictionary: Setting::NotSet,
            synonyms: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            proximity_precision: Setting::Set(
                meilisearch_types::settings::ProximityPrecisionView::ByAttribute,
            ),
            typo_tolerance: Setting::Set(TypoSettings {
                enabled: Setting::Set(false),
                min_word_size_for_typos: Setting::NotSet,
                disable_on_words: Setting::NotSet,
                disable_on_attributes: Setting::NotSet,
                disable_on_numbers: Setting::Set(true),
            }),
            faceting: Setting::NotSet,
            pagination: Setting::NotSet,
            embedders: Setting::NotSet,
            search_cutoff_ms: Setting::NotSet,
            localized_attributes: Setting::NotSet,
            facet_search: Setting::Set(false),
            prefix_search: Setting::Set(
                meilisearch_types::settings::PrefixSearchSettings::Disabled,
            ),
            chat: Setting::NotSet,
            _kind: std::marker::PhantomData,
        };
        apply_settings_to_builder(&checked_settings, &mut builder);

        progress.update_progress(SettingsProgress::ApplyTheSettings);
        let congestion = builder
            .execute(must_stop_processing, progress, self.ip_policy(), embedder_stats)
            .map_err(|err| Error::from_milli(err, None))?;
        Ok(congestion)
    }

    pub(crate) fn apply_dsr_update<'i>(
        &self,
        index_wtxn: &mut RwTxn<'i>,
        index: &'i Index,
        updates: &'i [DsrUpdate],
        embedder_stats: Arc<EmbedderStats>,
        mut tasks: Vec<Task>,
        progress: &Progress,
    ) -> Result<(Vec<Task>, Option<ChannelCongestion>)> {
        use milli::dynamic_search_rules::fields as dsr_fields;

        let indexer_alloc = Bump::new();
        let from_milli = |err| Error::from_milli(err, Some(DsrIndex::dsr_uid().to_owned()));
        let started_processing_at = std::time::Instant::now();

        progress.update_progress(DocumentOperationProgress::RetrievingConfig);
        let must_stop_processing = self.scheduler.must_stop_processing.clone();

        let rtxn = index.read_txn()?;
        let db_fields_ids_map = index.fields_ids_map(&rtxn)?;
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        // 1. local enum
        // 2. only stored on the heap => extra level of indirection not warranted
        #[allow(clippy::large_enum_variant)]
        enum DsrPayload {
            Replace(DynamicSearchRule),
            Delete,
        }

        let mut dsr_payloads = BTreeMap::<&RuleUid, DsrPayload>::new();
        let view = DynamicSearchRulesView::new(index, &rtxn, &db_fields_ids_map);

        for (update, task) in updates.iter().zip(tasks.iter()) {
            match update {
                DsrUpdate::CreateOrUpdate { rule_id, update } => {
                    let last_payload = dsr_payloads
                        .remove(rule_id)
                        .map(Ok)
                        .unwrap_or_else(|| {
                            let existing_rule = view
                                .get(rule_id)?
                                .map(|rule| {
                                    DynamicSearchRule::try_from_meili_doc(
                                        rule,
                                        FaultSource::Runtime,
                                    )
                                })
                                .transpose()?
                                .unwrap_or_else(|| DynamicSearchRule::new(rule_id.clone()));
                            Ok(DsrPayload::Replace(existing_rule))
                        })
                        .map_err(from_milli)?;
                    let mut existing_rule = match last_payload {
                        DsrPayload::Replace(dynamic_search_rule) => dynamic_search_rule,
                        DsrPayload::Delete => DynamicSearchRule::new(rule_id.clone()),
                    };
                    existing_rule.apply_update(update.clone(), task.enqueued_at);
                    dsr_payloads.insert(rule_id, DsrPayload::Replace(existing_rule));
                }
                DsrUpdate::Deletion(rule_id) => {
                    dsr_payloads.insert(rule_id, DsrPayload::Delete);
                }
            }
        }

        let mut indexer = IndexOperations::new();

        for (rule_id, payload) in dsr_payloads {
            match payload {
                DsrPayload::Replace(rule) => {
                    let nb_constraints = rule.facet_count();

                    // unwrap: dynamic search rule always serializable
                    let mut rule = serde_json::to_value(rule).unwrap();

                    'inject_nb_constraints: {
                        let Some(conditions) = rule.get_mut(dsr_fields::CONDITIONS) else {
                            break 'inject_nb_constraints;
                        };
                        let Some(filter) = conditions.get_mut(dsr_fields::FILTER) else {
                            break 'inject_nb_constraints;
                        };
                        filter
                            .as_object_mut()
                            .unwrap()
                            .insert(dsr_fields::NB_CONSTRAINTS.into(), nb_constraints.into());
                    }

                    let mut vec = bumpalo::collections::Vec::new_in(&indexer_alloc);
                    // unwrap: vec writing cannot fail + dynamic search rule always serializable
                    serde_json::to_writer(&mut vec, &rule).unwrap();
                    let vec = vec.into_bump_slice();
                    indexer.push_raw_operation(Payload::Replace {
                        payload: vec,
                        on_missing_document: milli::update::MissingDocumentPolicy::Create,
                    });
                }
                DsrPayload::Delete => {
                    let to_delete = &*indexer_alloc.alloc_str(rule_id.as_str());
                    let mut vec = bumpalo::collections::Vec::new_in(&indexer_alloc);
                    vec.push(to_delete);
                    let vec = vec.into_bump_slice();
                    indexer.push_raw_operation(Payload::DeletionByExternalIds(vec));
                }
            }
        }

        let indexer_config = self.index_mapper.indexer_config();
        let pool = &indexer_config.thread_pool;

        progress.update_progress(DocumentOperationProgress::ComputingDocumentChanges);

        let (document_changes, operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                index,
                &rtxn,
                Some(dsr_fields::UID),
                &mut new_fields_ids_map,
                &must_stop_processing,
                progress.clone(),
                // no sharding for the DSR index: DSR rules are fully replicated on all remotes
                None,
            )
            .map_err(from_milli)?;

        progress.update_progress(DocumentOperationProgress::ReadingPayloadStats);
        let mut candidates_count = 0;
        for (stats, task) in operation_stats.into_iter().zip(&mut tasks) {
            candidates_count += stats.document_count;
            match stats.error {
                Some(error) => {
                    task.status = Status::Failed;
                    task.error = Some(milli::Error::UserError(error).into());
                }
                None => task.status = Status::Succeeded,
            }
        }

        progress.update_progress(DocumentOperationProgress::Indexing);
        let mut congestion = None;
        let embedders = RuntimeEmbedders::default();
        if tasks.iter().any(|res| res.error.is_none()) {
            congestion = Some(
                indexer::index(
                    index_wtxn,
                    index,
                    pool,
                    indexer_config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    embedders,
                    &must_stop_processing,
                    progress,
                    self.ip_policy(),
                    &embedder_stats,
                )
                .map_err(from_milli)?,
            );

            let addition = DocumentAdditionResult {
                indexed_documents: candidates_count,
                number_of_documents: index.number_of_documents(index_wtxn).map_err(from_milli)?,
            };

            tracing::info!(indexing_result = ?addition, processed_in = ?started_processing_at.elapsed(), "DSR update done");
        }

        Ok((tasks, congestion))
    }
}

fn eq_attr_pattern(pattern: String) -> FilterableAttributesRule {
    FilterableAttributesRule::Pattern(FilterableAttributesPatterns {
        attribute_patterns: vec![pattern].into(),
        features: FilterableAttributesFeatures {
            facet_search: false,
            filter: FilterFeatures { equality: true, comparison: false },
        },
    })
}

fn cmp_attr_pattern(pattern: String) -> FilterableAttributesRule {
    FilterableAttributesRule::Pattern(FilterableAttributesPatterns {
        attribute_patterns: vec![pattern].into(),
        features: FilterableAttributesFeatures {
            facet_search: false,
            filter: FilterFeatures { equality: true, comparison: true },
        },
    })
}

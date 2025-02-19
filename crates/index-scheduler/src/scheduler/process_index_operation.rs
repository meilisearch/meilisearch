use bumpalo::collections::CollectIn;
use bumpalo::Bump;
use meilisearch_types::heed::RwTxn;
use meilisearch_types::milli::documents::PrimaryKey;
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::update::new::indexer::{self, UpdateByFunction};
use meilisearch_types::milli::update::DocumentAdditionResult;
use meilisearch_types::milli::{self, ChannelCongestion, Filter, ThreadPoolNoAbortBuilder};
use meilisearch_types::settings::apply_settings_to_builder;
use meilisearch_types::tasks::{Details, KindWithContent, Status, Task};
use meilisearch_types::Index;
use roaring::RoaringBitmap;

use super::create_batch::{DocumentOperation, IndexOperation};
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
        skip(self, index_wtxn, index, progress),
        target = "indexing::scheduler"
    )]
    pub(crate) fn apply_index_operation<'i>(
        &self,
        index_wtxn: &mut RwTxn<'i>,
        index: &'i Index,
        operation: IndexOperation,
        progress: Progress,
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
                // TODO: at some point, for better efficiency we might want to reuse the bumpalo for successive batches.
                // this is made difficult by the fact we're doing private clones of the index scheduler and sending it
                // to a fresh thread.
                let mut content_files = Vec::new();
                for operation in &operations {
                    match operation {
                        DocumentOperation::Replace(content_uuid)
                        | DocumentOperation::Update(content_uuid) => {
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
                let mut indexer = indexer::DocumentOperation::new();
                let embedders = index
                    .embedding_configs(index_wtxn)
                    .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?;
                let embedders = self.embedders(index_uid.clone(), embedders)?;
                for operation in operations {
                    match operation {
                        DocumentOperation::Replace(_content_uuid) => {
                            let mmap = content_files_iter.next().unwrap();
                            indexer
                                .replace_documents(mmap)
                                .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?;
                        }
                        DocumentOperation::Update(_content_uuid) => {
                            let mmap = content_files_iter.next().unwrap();
                            indexer
                                .update_documents(mmap)
                                .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?;
                        }
                        DocumentOperation::Delete(document_ids) => {
                            let document_ids: bumpalo::collections::vec::Vec<_> = document_ids
                                .iter()
                                .map(|s| &*indexer_alloc.alloc_str(s))
                                .collect_in(&indexer_alloc);
                            indexer.delete_documents(document_ids.into_bump_slice());
                        }
                    }
                }

                let local_pool;
                let indexer_config = self.index_mapper.indexer_config();
                let pool = match &indexer_config.thread_pool {
                    Some(pool) => pool,
                    None => {
                        local_pool = ThreadPoolNoAbortBuilder::new()
                            .thread_name(|i| format!("indexing-thread-{i}"))
                            .build()
                            .unwrap();
                        &local_pool
                    }
                };

                progress.update_progress(DocumentOperationProgress::ComputingDocumentChanges);
                let (document_changes, operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        index,
                        &rtxn,
                        primary_key.as_deref(),
                        &mut new_fields_ids_map,
                        &|| must_stop_processing.get(),
                        progress.clone(),
                    )
                    .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?;

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
                            &|| must_stop_processing.get(),
                            &progress,
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

                let candidates = match filter.as_ref().map(Filter::from_json) {
                    Some(Ok(Some(filter))) => filter
                        .evaluate(index_wtxn, index)
                        .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?,
                    None | Some(Ok(None)) => index.documents_ids(index_wtxn)?,
                    Some(Err(e)) => return Err(Error::from_milli(e, Some(index_uid.clone()))),
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
                    let local_pool;
                    let indexer_config = self.index_mapper.indexer_config();
                    let pool = match &indexer_config.thread_pool {
                        Some(pool) => pool,
                        None => {
                            local_pool = ThreadPoolNoAbortBuilder::new()
                                .thread_name(|i| format!("indexing-thread-{i}"))
                                .build()
                                .unwrap();
                            &local_pool
                        }
                    };

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
                        .embedding_configs(index_wtxn)
                        .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?;
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
                            &|| must_stop_processing.get(),
                            &progress,
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
                            let filter = match Filter::from_json(filter_expr) {
                                Ok(filter) => filter,
                                Err(err) => {
                                    // theorically, this should be catched by deserr before reaching the index-scheduler and cannot happens
                                    task.status = Status::Failed;
                                    task.error = Some(
                                        Error::from_milli(err, Some(index_uid.clone())).into(),
                                    );
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
                    let local_pool;
                    let indexer_config = self.index_mapper.indexer_config();
                    let pool = match &indexer_config.thread_pool {
                        Some(pool) => pool,
                        None => {
                            local_pool = ThreadPoolNoAbortBuilder::new()
                                .thread_name(|i| format!("indexing-thread-{i}"))
                                .build()
                                .unwrap();
                            &local_pool
                        }
                    };

                    progress.update_progress(DocumentDeletionProgress::DeleteDocuments);
                    let mut indexer = indexer::DocumentDeletion::new();
                    let candidates_count = to_delete.len();
                    indexer.delete_documents_by_docids(to_delete);
                    let document_changes = indexer.into_changes(&indexer_alloc, primary_key);
                    let embedders = index
                        .embedding_configs(index_wtxn)
                        .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?;
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
                            &|| must_stop_processing.get(),
                            &progress,
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
                builder
                    .execute(
                        |indexing_step| tracing::debug!(update = ?indexing_step),
                        || must_stop_processing.get(),
                    )
                    .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?;

                Ok((tasks, None))
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
                    progress.clone(),
                )?;

                let (settings_tasks, _congestion) = self.apply_index_operation(
                    index_wtxn,
                    index,
                    IndexOperation::Settings { index_uid, settings, tasks: settings_tasks },
                    progress,
                )?;

                let mut tasks = settings_tasks;
                tasks.append(&mut import_tasks);
                Ok((tasks, None))
            }
        }
    }
}

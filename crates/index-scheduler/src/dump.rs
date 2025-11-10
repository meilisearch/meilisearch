#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::io;

use dump::{KindDump, TaskDump, UpdateFile};
use meilisearch_types::batches::{Batch, BatchId};
use meilisearch_types::heed::RwTxn;
use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::milli;
use meilisearch_types::tasks::{Kind, KindWithContent, Status, Task};
use roaring::RoaringBitmap;
use uuid::Uuid;

use crate::{utils, Error, IndexScheduler, Result};

pub struct Dump<'a> {
    index_scheduler: &'a IndexScheduler,
    wtxn: RwTxn<'a>,

    batch_to_task_mapping: HashMap<BatchId, RoaringBitmap>,

    indexes: HashMap<String, RoaringBitmap>,
    statuses: HashMap<Status, RoaringBitmap>,
    kinds: HashMap<Kind, RoaringBitmap>,

    batch_indexes: HashMap<String, RoaringBitmap>,
    batch_statuses: HashMap<Status, RoaringBitmap>,
    batch_kinds: HashMap<Kind, RoaringBitmap>,
}

impl<'a> Dump<'a> {
    pub(crate) fn new(index_scheduler: &'a mut IndexScheduler) -> Result<Self> {
        // While loading a dump no one should be able to access the scheduler thus I can block everything.
        let wtxn = index_scheduler.env.write_txn()?;

        Ok(Dump {
            index_scheduler,
            wtxn,
            batch_to_task_mapping: HashMap::new(),
            indexes: HashMap::new(),
            statuses: HashMap::new(),
            kinds: HashMap::new(),
            batch_indexes: HashMap::new(),
            batch_statuses: HashMap::new(),
            batch_kinds: HashMap::new(),
        })
    }

    /// Register a new batch coming from a dump in the scheduler.
    /// By taking a mutable ref we're pretty sure no one will ever import a dump while actix is running.
    pub fn register_dumped_batch(&mut self, batch: Batch) -> Result<()> {
        self.index_scheduler.queue.batches.all_batches.put(&mut self.wtxn, &batch.uid, &batch)?;
        if let Some(enqueued_at) = batch.enqueued_at {
            utils::insert_task_datetime(
                &mut self.wtxn,
                self.index_scheduler.queue.batches.enqueued_at,
                enqueued_at.earliest,
                batch.uid,
            )?;
            utils::insert_task_datetime(
                &mut self.wtxn,
                self.index_scheduler.queue.batches.enqueued_at,
                enqueued_at.oldest,
                batch.uid,
            )?;
        }
        utils::insert_task_datetime(
            &mut self.wtxn,
            self.index_scheduler.queue.batches.started_at,
            batch.started_at,
            batch.uid,
        )?;
        if let Some(finished_at) = batch.finished_at {
            utils::insert_task_datetime(
                &mut self.wtxn,
                self.index_scheduler.queue.batches.finished_at,
                finished_at,
                batch.uid,
            )?;
        }

        for index in batch.stats.index_uids.keys() {
            match self.batch_indexes.get_mut(index) {
                Some(bitmap) => {
                    bitmap.insert(batch.uid);
                }
                None => {
                    let mut bitmap = RoaringBitmap::new();
                    bitmap.insert(batch.uid);
                    self.batch_indexes.insert(index.to_string(), bitmap);
                }
            };
        }

        for status in batch.stats.status.keys() {
            self.batch_statuses.entry(*status).or_default().insert(batch.uid);
        }
        for kind in batch.stats.types.keys() {
            self.batch_kinds.entry(*kind).or_default().insert(batch.uid);
        }

        Ok(())
    }

    /// Register a new task coming from a dump in the scheduler.
    /// By taking a mutable ref we're pretty sure no one will ever import a dump while actix is running.
    pub fn register_dumped_task(
        &mut self,
        task: TaskDump,
        content_file: Option<Box<UpdateFile>>,
    ) -> Result<Task> {
        let task_has_no_docs = matches!(task.kind, KindDump::DocumentImport { documents_count, .. } if documents_count == 0);

        let content_uuid = match content_file {
            Some(content_file) if task.status == Status::Enqueued => {
                let (uuid, file) = self.index_scheduler.queue.create_update_file(false)?;
                let mut writer = io::BufWriter::new(file);
                for doc in content_file {
                    let doc = doc?;
                    serde_json::to_writer(&mut writer, &doc).map_err(|e| {
                        Error::from_milli(milli::InternalError::SerdeJson(e).into(), None)
                    })?;
                }
                let file = writer.into_inner().map_err(|e| e.into_error())?;
                file.persist()?;

                Some(uuid)
            }
            // If the task isn't `Enqueued` then just generate a recognisable `Uuid`
            // in case we try to open it later.
            _ if task.status != Status::Enqueued => Some(Uuid::nil()),
            None if task.status == Status::Enqueued && task_has_no_docs => {
                let (uuid, file) = self.index_scheduler.queue.create_update_file(false)?;
                file.persist()?;

                Some(uuid)
            }
            _ => None,
        };

        let task = Task {
            uid: task.uid,
            batch_uid: task.batch_uid,
            enqueued_at: task.enqueued_at,
            started_at: task.started_at,
            finished_at: task.finished_at,
            error: task.error,
            canceled_by: task.canceled_by,
            details: task.details,
            status: task.status,
            network: task.network,
            custom_metadata: task.custom_metadata,
            kind: match task.kind {
                KindDump::DocumentImport {
                    primary_key,
                    method,
                    documents_count,
                    allow_index_creation,
                } => KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                    method,
                    content_file: content_uuid.ok_or(Error::CorruptedDump)?,
                    documents_count,
                    allow_index_creation,
                },
                KindDump::DocumentDeletion { documents_ids } => KindWithContent::DocumentDeletion {
                    documents_ids,
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::DocumentDeletionByFilter { filter } => {
                    KindWithContent::DocumentDeletionByFilter {
                        filter_expr: filter,
                        index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    }
                }
                KindDump::DocumentEdition { filter, context, function } => {
                    KindWithContent::DocumentEdition {
                        index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                        filter_expr: filter,
                        context,
                        function,
                    }
                }
                KindDump::DocumentClear => KindWithContent::DocumentClear {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::Settings { settings, is_deletion, allow_index_creation } => {
                    KindWithContent::SettingsUpdate {
                        index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                        new_settings: settings,
                        is_deletion,
                        allow_index_creation,
                    }
                }
                KindDump::IndexDeletion => KindWithContent::IndexDeletion {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::IndexCreation { primary_key } => KindWithContent::IndexCreation {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                },
                KindDump::IndexUpdate { primary_key, uid } => KindWithContent::IndexUpdate {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                    new_index_uid: uid,
                },
                KindDump::IndexSwap { swaps } => KindWithContent::IndexSwap { swaps },
                KindDump::TaskCancelation { query, tasks } => {
                    KindWithContent::TaskCancelation { query, tasks }
                }
                KindDump::TasksDeletion { query, tasks } => {
                    KindWithContent::TaskDeletion { query, tasks }
                }
                KindDump::DumpCreation { keys, instance_uid } => {
                    KindWithContent::DumpCreation { keys, instance_uid }
                }
                KindDump::SnapshotCreation => KindWithContent::SnapshotCreation,
                KindDump::Export { url, api_key, payload_size, indexes } => {
                    KindWithContent::Export {
                        url,
                        api_key,
                        payload_size,
                        indexes: indexes
                            .into_iter()
                            .map(|(pattern, settings)| {
                                Ok((
                                    IndexUidPattern::try_from(pattern)
                                        .map_err(|_| Error::CorruptedDump)?,
                                    settings,
                                ))
                            })
                            .collect::<Result<_, Error>>()?,
                    }
                }
                KindDump::UpgradeDatabase { from } => KindWithContent::UpgradeDatabase { from },
                KindDump::IndexCompaction { index_uid } => {
                    KindWithContent::IndexCompaction { index_uid }
                }
            },
        };

        self.index_scheduler.queue.tasks.all_tasks.put(&mut self.wtxn, &task.uid, &task)?;
        if let Some(batch_id) = task.batch_uid {
            self.batch_to_task_mapping.entry(batch_id).or_default().insert(task.uid);
        }

        for index in task.indexes() {
            match self.indexes.get_mut(index) {
                Some(bitmap) => {
                    bitmap.insert(task.uid);
                }
                None => {
                    let mut bitmap = RoaringBitmap::new();
                    bitmap.insert(task.uid);
                    self.indexes.insert(index.to_string(), bitmap);
                }
            };
        }

        utils::insert_task_datetime(
            &mut self.wtxn,
            self.index_scheduler.queue.tasks.enqueued_at,
            task.enqueued_at,
            task.uid,
        )?;

        // we can't override the started_at & finished_at, so we must only set it if the tasks is finished and won't change
        if matches!(task.status, Status::Succeeded | Status::Failed | Status::Canceled) {
            if let Some(started_at) = task.started_at {
                utils::insert_task_datetime(
                    &mut self.wtxn,
                    self.index_scheduler.queue.tasks.started_at,
                    started_at,
                    task.uid,
                )?;
            }
            if let Some(finished_at) = task.finished_at {
                utils::insert_task_datetime(
                    &mut self.wtxn,
                    self.index_scheduler.queue.tasks.finished_at,
                    finished_at,
                    task.uid,
                )?;
            }
        }

        self.statuses.entry(task.status).or_default().insert(task.uid);
        self.kinds.entry(task.kind.as_kind()).or_default().insert(task.uid);

        Ok(task)
    }

    /// Commit all the changes and exit the importing dump state
    pub fn finish(mut self) -> Result<()> {
        for (batch_id, task_ids) in self.batch_to_task_mapping {
            self.index_scheduler.queue.batch_to_tasks_mapping.put(
                &mut self.wtxn,
                &batch_id,
                &task_ids,
            )?;
        }

        for (index, bitmap) in self.indexes {
            self.index_scheduler.queue.tasks.index_tasks.put(&mut self.wtxn, &index, &bitmap)?;
        }
        for (status, bitmap) in self.statuses {
            self.index_scheduler.queue.tasks.put_status(&mut self.wtxn, status, &bitmap)?;
        }
        for (kind, bitmap) in self.kinds {
            self.index_scheduler.queue.tasks.put_kind(&mut self.wtxn, kind, &bitmap)?;
        }

        for (index, bitmap) in self.batch_indexes {
            self.index_scheduler.queue.batches.index_tasks.put(&mut self.wtxn, &index, &bitmap)?;
        }
        for (status, bitmap) in self.batch_statuses {
            self.index_scheduler.queue.batches.put_status(&mut self.wtxn, status, &bitmap)?;
        }
        for (kind, bitmap) in self.batch_kinds {
            self.index_scheduler.queue.batches.put_kind(&mut self.wtxn, kind, &bitmap)?;
        }

        self.wtxn.commit()?;
        self.index_scheduler.scheduler.wake_up.signal();

        Ok(())
    }
}

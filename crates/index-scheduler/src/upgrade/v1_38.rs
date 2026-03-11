use std::collections::BTreeMap;

use meilisearch_types::heed::{Database, Env, RwTxn, WithoutTls};
use meilisearch_types::milli::{CboRoaringBitmapCodec, BEU32};
use meilisearch_types::tasks::Status;
use roaring::RoaringBitmap;
use tracing::info;

use super::UpgradeIndexScheduler;
use crate::queue::db_name::BATCH_TO_TASKS_MAPPING;
use crate::queue::{BatchQueue, TaskQueue};

pub struct RemoveOrphanBatches;

impl UpgradeIndexScheduler for RemoveOrphanBatches {
    fn upgrade(&self, env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> anyhow::Result<()> {
        let batch_queue = BatchQueue::new(env, wtxn)?;
        let all_batch_ids = batch_queue.all_batch_ids(wtxn)?;

        let batch_to_tasks_mapping: Database<BEU32, CboRoaringBitmapCodec> =
            env.create_database(wtxn, Some(BATCH_TO_TASKS_MAPPING))?;

        let all_batches = batch_queue.all_batches.lazily_decode_data();
        let iter = all_batches.iter(wtxn)?;
        let mut range_start = None;
        let mut count = 0;
        let mut ranges = Vec::new();
        for batch in iter {
            let (batch_id, _) = batch?;

            if !all_batch_ids.contains(batch_id) {
                count += 1;
                if range_start.is_none() {
                    range_start = Some(batch_id);
                }
            } else if let Some(start) = range_start.take() {
                ranges.push(start..batch_id);
            }
        }
        if let Some(start) = range_start {
            ranges.push(start..u32::MAX);
        }

        if !ranges.is_empty() {
            info!("Removing {count} batches that were not properly removed in previous versions due to #5827.");
        }

        for range in ranges {
            batch_queue.all_batches.delete_range(wtxn, &range)?;
            batch_to_tasks_mapping.delete_range(wtxn, &range)?;
        }

        Ok(())
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 38, 0)
    }

    fn description(&self) -> &'static str {
        "Remove orphan batches"
    }
}

pub struct FixupIndexTasks;

impl super::UpgradeIndexScheduler for FixupIndexTasks {
    fn upgrade(&self, env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> anyhow::Result<()> {
        let queue = TaskQueue::new(env, wtxn)?;
        let mut tasks_per_index: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        let enqueued = queue.get_status(wtxn, Status::Enqueued)?;
        for task_id in enqueued {
            let Some(task) = queue.get_task(wtxn, task_id)? else {
                continue;
            };
            let Some(&index_name) = task.indexes().first() else {
                continue;
            };
            let tasks_for_index = tasks_per_index.entry(index_name.to_string()).or_default();
            tasks_for_index.insert(task_id);
        }

        for (index, tasks_for_index) in tasks_per_index {
            queue.update_index(wtxn, &index, |tasks| {
                *tasks |= &tasks_for_index;
            })?;
        }

        Ok(())
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        ((1, 38, 0)..=(1, 38, 1)).contains(&initial_version)
    }

    fn description(&self) -> &'static str {
        "fixing up inverted index for index-tasks"
    }
}

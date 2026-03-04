use meilisearch_types::heed::{Database, Env, RwTxn, WithoutTls};
use meilisearch_types::milli::{CboRoaringBitmapCodec, BEU32};
use tracing::info;

use super::UpgradeIndexScheduler;
use crate::queue::db_name::BATCH_TO_TASKS_MAPPING;
use crate::queue::BatchQueue;

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

use std::collections::BTreeMap;

use meilisearch_types::heed::{Env, RwTxn, WithoutTls};
use meilisearch_types::tasks::Status;
use roaring::RoaringBitmap;

use crate::queue::TaskQueue;

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
            queue.update_index(wtxn, &index,  |tasks| {
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

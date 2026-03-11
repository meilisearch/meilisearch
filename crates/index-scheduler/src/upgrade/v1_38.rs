use std::collections::BTreeMap;

use meilisearch_types::heed::{Env, RwTxn, WithoutTls};
use meilisearch_types::tasks::{Kind, Status};
use roaring::RoaringBitmap;

use crate::queue::TaskQueue;

pub struct FixupIndexTasks;

impl super::UpgradeIndexScheduler for FixupIndexTasks {
    fn upgrade(&self, env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> anyhow::Result<()> {
        let queue = TaskQueue::new(env, wtxn)?;
        let mut tasks_per_index: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        let mut tasks_per_status: BTreeMap<Status, RoaringBitmap> = BTreeMap::new();
        let mut tasks_per_kind: BTreeMap<Kind, RoaringBitmap> = BTreeMap::new();
        for entry in queue.all_tasks.iter(wtxn)? {
            let (task_id, task) = entry?;
            let status = task.status;
            let tasks_for_status = tasks_per_status.entry(status).or_default();
            tasks_for_status.insert(task_id);

            let kind = task.kind.as_kind();
            let tasks_for_kind = tasks_per_kind.entry(kind).or_default();
            tasks_for_kind.insert(task_id);

            if let Some(index_name) = task.indexes().first() {
                let tasks_for_index = tasks_per_index.entry(index_name.to_string()).or_default();
                tasks_for_index.insert(task_id);
            }
        }

        for (index, tasks_for_index) in tasks_per_index {
            queue.update_index(wtxn, &index, |tasks| {
                *tasks |= &tasks_for_index;
            })?;
        }

        for (status, tasks_for_status) in tasks_per_status {
            queue.update_status(wtxn, status, |tasks| {
                *tasks |= &tasks_for_status;
            })?;
        }

        for (kind, tasks_for_kind) in tasks_per_kind {
            queue.update_kind(wtxn, kind, |tasks| {
                *tasks |= &tasks_for_kind;
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

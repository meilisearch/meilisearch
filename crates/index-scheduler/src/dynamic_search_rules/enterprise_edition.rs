// Copyright © 2026 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use meilisearch_types::dynamic_search_rules::{
    DynamicSearchRule, DynamicSearchRuleTarget, RuleUid, Targets,
};
use meilisearch_types::tasks::KindWithContent;

use crate::{Error, Result};

use super::DynamicSearchRules;

impl<'a> DynamicSearchRules<'a> {
    pub fn rule_target(
        &self,
        target_rule_uid: &RuleUid,
    ) -> Result<Option<DynamicSearchRuleTarget>> {
        let current = self.get(target_rule_uid)?;
        let rtxn = self.index_scheduler.env.read_txn()?;
        let tasks_by_status = self
            .index_scheduler
            .queue
            .tasks
            .get_status(&rtxn, meilisearch_types::tasks::Status::Enqueued)?;
        let tasks_by_type = self
            .index_scheduler
            .queue
            .tasks
            .get_kind(&rtxn, meilisearch_types::tasks::Kind::DsrUpdate)?
            | self
                .index_scheduler
                .queue
                .tasks
                .get_kind(&rtxn, meilisearch_types::tasks::Kind::DsrClear)?;
        let tasks = tasks_by_status & tasks_by_type;

        // `None` if the DSR doesn't currently exist and no pending task will change this
        if tasks.is_empty() && current.is_none() {
            return Ok(None);
        }

        let mut new = current.clone();
        let mut task_ids = Vec::new();
        for task_id in tasks {
            let Some(task) = self.index_scheduler.queue.tasks.get_task(&rtxn, task_id)? else {
                tracing::error!("ignoring irrecoverable task {task_id}");
                continue;
            };
            match task.kind {
                KindWithContent::DsrUpdate(
                    meilisearch_types::tasks::DsrUpdate::CreateOrUpdate { rule_id, update },
                ) if &rule_id == target_rule_uid => {
                    task_ids.push(task_id);
                    let mut updated = new.unwrap_or(DynamicSearchRule::new(rule_id));

                    updated.apply_update(update, task.enqueued_at);
                    new = Some(updated);
                }
                KindWithContent::DsrUpdate(meilisearch_types::tasks::DsrUpdate::Deletion(
                    rule_id,
                )) if &rule_id == target_rule_uid => {
                    task_ids.push(task_id);
                    new = None;
                }
                KindWithContent::DsrClear => {
                    task_ids.push(task_id);
                    new = None
                }
                KindWithContent::DsrUpdate(
                    meilisearch_types::tasks::DsrUpdate::Deletion(_)
                    | meilisearch_types::tasks::DsrUpdate::CreateOrUpdate { .. },
                ) => {}

                KindWithContent::DocumentAdditionOrUpdate { .. }
                | KindWithContent::DocumentDeletion { .. }
                | KindWithContent::DocumentDeletionByFilter { .. }
                | KindWithContent::DocumentEdition { .. }
                | KindWithContent::DocumentClear { .. }
                | KindWithContent::SettingsUpdate { .. }
                | KindWithContent::IndexDeletion { .. }
                | KindWithContent::IndexCreation { .. }
                | KindWithContent::IndexUpdate { .. }
                | KindWithContent::IndexSwap { .. }
                | KindWithContent::TaskCancelation { .. }
                | KindWithContent::TaskDeletion { .. }
                | KindWithContent::DumpCreation { .. }
                | KindWithContent::SnapshotCreation
                | KindWithContent::Export { .. }
                | KindWithContent::UpgradeDatabase { .. }
                | KindWithContent::IndexCompaction { .. }
                | KindWithContent::NetworkTopologyChange(_) => {
                    tracing::error!("ignoring unexpected kind for task {task_id}");
                    continue;
                }
            }
        }

        let current = current.map(|current| current.into_uid_update().1);
        let mut target = new.map(|new| new.into_uid_update().1);

        match (&current, &mut target) {
            // rule did not exist and is not created by pending tasks, so exit with `None`
            (None, None) => return Ok(None),
            // rule might get updated, so keep only the difference in target
            (Some(current), Some(target)) => {
                target.difference(current);
            }
            // rule is created
            (None, Some(_)) => {}
            // rule is deleted
            (Some(_), None) => {}
        }

        Ok(Some(DynamicSearchRuleTarget {
            current,
            target,
            needs_processing: !task_ids.is_empty(),
            task_uids: task_ids,
        }))
    }

    pub fn targets(&self) -> Result<Targets> {
        let rtxn = self.index_scheduler.env.read_txn()?;
        let tasks_by_status = self
            .index_scheduler
            .queue
            .tasks
            .get_status(&rtxn, meilisearch_types::tasks::Status::Enqueued)?;
        let tasks_by_type = self
            .index_scheduler
            .queue
            .tasks
            .get_kind(&rtxn, meilisearch_types::tasks::Kind::DsrUpdate)?
            | self
                .index_scheduler
                .queue
                .tasks
                .get_kind(&rtxn, meilisearch_types::tasks::Kind::DsrClear)?;
        let tasks = tasks_by_status & tasks_by_type;
        let mut targets = Targets::default();

        // No pending processing
        if tasks.is_empty() {
            return Ok(targets);
        }

        let milli = self.milli_dsrs()?;
        let from_milli = |err| Error::from_milli(err, None);

        let in_db = |rule_id: &str| -> Result<bool> {
            let Some(dsrs) = milli.as_ref() else {
                return Ok(false);
            };
            dsrs.exists(rule_id).map_err(from_milli)
        };

        for task_id in tasks {
            let Some(task) = self.index_scheduler.queue.tasks.get_task(&rtxn, task_id)? else {
                tracing::error!("ignoring irrecoverable task {task_id}");
                continue;
            };
            match task.kind {
                KindWithContent::DsrUpdate(
                    meilisearch_types::tasks::DsrUpdate::CreateOrUpdate { rule_id, update: _ },
                ) => {
                    targets.update_rule(rule_id.into_inner(), task_id, in_db)?;
                }
                KindWithContent::DsrUpdate(meilisearch_types::tasks::DsrUpdate::Deletion(
                    rule_id,
                )) => {
                    targets.delete_rule(rule_id.into_inner(), task_id, in_db)?;
                }
                KindWithContent::DsrClear => {
                    targets.clear_rules(task_id);
                }

                KindWithContent::DocumentAdditionOrUpdate { .. }
                | KindWithContent::DocumentDeletion { .. }
                | KindWithContent::DocumentDeletionByFilter { .. }
                | KindWithContent::DocumentEdition { .. }
                | KindWithContent::DocumentClear { .. }
                | KindWithContent::SettingsUpdate { .. }
                | KindWithContent::IndexDeletion { .. }
                | KindWithContent::IndexCreation { .. }
                | KindWithContent::IndexUpdate { .. }
                | KindWithContent::IndexSwap { .. }
                | KindWithContent::TaskCancelation { .. }
                | KindWithContent::TaskDeletion { .. }
                | KindWithContent::DumpCreation { .. }
                | KindWithContent::SnapshotCreation
                | KindWithContent::Export { .. }
                | KindWithContent::UpgradeDatabase { .. }
                | KindWithContent::IndexCompaction { .. }
                | KindWithContent::NetworkTopologyChange(_) => {
                    tracing::error!("ignoring unexpected kind for task {task_id}");
                    continue;
                }
            }
        }

        Ok(targets)
    }
}

use meilisearch_types::heed::{Env, RwTxn, WithoutTls};
use meilisearch_types::tasks::{KindWithContent, Status, Task};
use time::OffsetDateTime;

use super::UpgradeIndexScheduler;
use crate::queue::TaskQueue;
use crate::upgrade::v1_49::LegacyDynamicSearchRulesStore;
use crate::Result;

pub struct MigrateDynamicSearchRules;

impl UpgradeIndexScheduler for MigrateDynamicSearchRules {
    fn upgrade(&self, env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> anyhow::Result<()> {
        let queue = TaskQueue::new(env, wtxn)?;

        let store = LegacyDynamicSearchRulesStore::new(env, wtxn)?;

        // collect all existing rules into memory, sparing us borrow issues caused by iterating over the DB while writing tasks
        // with the same wtxn.
        // This is OK to do because in v1.49, all DSRs were in memory during every search request.

        let rules: Result<Vec<_>> = store
            .persisted
            .iter(wtxn)?
            .filter_map(|res| {
                let (uid, rule) = match res {
                    Ok(value) => value,
                    Err(err) => return Some(Err(err.into())),
                };

                let Some(rule) = rule.into_dynamic_search_rule() else {
                    tracing::debug!("Skipping rule `{uid}` which contains impossible conditions");
                    return None;
                };

                Some(Ok(rule))
            })
            .collect();

        let rules = rules?;

        for rule in rules {
            let (rule_id, update) = rule.into_uid_update();
            let uid = queue.next_task_id(wtxn)?;

            let kind =
                KindWithContent::DsrUpdate(meilisearch_types::tasks::DsrUpdate::CreateOrUpdate {
                    rule_id,
                    update,
                });

            let details = kind.default_details();
            queue.register(
                wtxn,
                &Task {
                    uid,
                    batch_uid: None,
                    enqueued_at: OffsetDateTime::now_utc(),
                    started_at: None,
                    finished_at: None,
                    error: None,
                    canceled_by: None,
                    details,
                    status: Status::Enqueued,
                    kind,
                    network: None,
                    custom_metadata: None,
                },
            )?;
        }

        store.persisted.clear(wtxn)?;
        Ok(())
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 50, 0)
    }

    fn description(&self) -> &'static str {
        "Migrate search rules to new schema"
    }
}

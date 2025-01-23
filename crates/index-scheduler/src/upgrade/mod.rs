use anyhow::bail;
use meilisearch_types::heed::{Env, RwTxn};
use meilisearch_types::tasks::{Details, KindWithContent, Status, Task};
use meilisearch_types::versioning::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use time::OffsetDateTime;
use tracing::info;

use crate::queue::TaskQueue;
use crate::versioning::Versioning;

trait UpgradeIndexScheduler {
    fn upgrade(&self, env: &Env, wtxn: &mut RwTxn, original: (u32, u32, u32))
        -> anyhow::Result<()>;
    fn target_version(&self) -> (u32, u32, u32);
}

pub fn upgrade_index_scheduler(
    env: &Env,
    versioning: &Versioning,
    from: (u32, u32, u32),
    to: (u32, u32, u32),
) -> anyhow::Result<()> {
    let current_major = to.0;
    let current_minor = to.1;
    let current_patch = to.2;

    let upgrade_functions: &[&dyn UpgradeIndexScheduler] = &[&V1_12_ToCurrent {}];

    let start = match from {
        (1, 12, _) => 0,
        (major, minor, patch) => {
            if major > current_major
                || (major == current_major && minor > current_minor)
                || (major == current_major && minor == current_minor && patch > current_patch)
            {
                bail!(
                "Database version {major}.{minor}.{patch} is higher than the Meilisearch version {current_major}.{current_minor}.{current_patch}. Downgrade is not supported",
                );
            } else if major < 1 || (major == current_major && minor < 12) {
                bail!(
                "Database version {major}.{minor}.{patch} is too old for the experimental dumpless upgrade feature. Please generate a dump using the v{major}.{minor}.{patch} and import it in the v{current_major}.{current_minor}.{current_patch}",
            );
            } else {
                bail!("Unknown database version: v{major}.{minor}.{patch}");
            }
        }
    };

    let mut current_version = from;

    info!("Upgrading the task queue");
    for upgrade in upgrade_functions[start..].iter() {
        let target = upgrade.target_version();
        info!(
            "Upgrading from v{}.{}.{} to v{}.{}.{}",
            from.0, from.1, from.2, current_version.0, current_version.1, current_version.2
        );
        let mut wtxn = env.write_txn()?;
        upgrade.upgrade(env, &mut wtxn, from)?;
        versioning.set_version(&mut wtxn, target)?;
        wtxn.commit()?;
        current_version = target;
    }

    let mut wtxn = env.write_txn()?;
    let queue = TaskQueue::new(env, &mut wtxn)?;
    let uid = queue.next_task_id(&wtxn)?;
    queue.register(
        &mut wtxn,
        &Task {
            uid,
            batch_uid: None,
            enqueued_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            error: None,
            canceled_by: None,
            details: Some(Details::UpgradeDatabase { from, to }),
            status: Status::Enqueued,
            kind: KindWithContent::UpgradeDatabase { from },
        },
    )?;
    wtxn.commit()?;

    Ok(())
}

#[allow(non_camel_case_types)]
struct V1_12_ToCurrent {}

impl UpgradeIndexScheduler for V1_12_ToCurrent {
    fn upgrade(
        &self,
        _env: &Env,
        _wtxn: &mut RwTxn,
        _original: (u32, u32, u32),
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (
            VERSION_MAJOR.parse().unwrap(),
            VERSION_MINOR.parse().unwrap(),
            VERSION_PATCH.parse().unwrap(),
        )
    }
}

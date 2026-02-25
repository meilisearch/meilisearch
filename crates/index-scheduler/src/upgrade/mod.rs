use anyhow::bail;
use meilisearch_types::heed::{Env, RwTxn, WithoutTls};
use meilisearch_types::tasks::{Details, KindWithContent, Status, Task};
use meilisearch_types::versioning;
use time::OffsetDateTime;
use tracing::info;

use crate::queue::TaskQueue;
use crate::versioning::Versioning;

mod v1_29;
mod v1_30;
mod v1_37;

trait UpgradeIndexScheduler {
    fn upgrade(&self, env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> anyhow::Result<()>;
    /// Whether the migration should be applied, depending on the initial version of the index scheduler before
    /// any migration was applied
    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool;
    /// A progress-centric description of the migration
    fn description(&self) -> &'static str;
}

/// Upgrade the index scheduler to the binary version.
///
/// # Warning
///
/// The current implementation uses a single wtxn to the index scheduler for the whole duration of the upgrade.
/// If migrations start taking take a long time, it might prevent tasks from being registered.
/// If this issue manifests, then it can be mitigated by adding a `fn target_version` to `UpgradeIndexScheduler`,
/// to be able to write intermediate versions and drop the wtxn between applying migrations.
pub fn upgrade_index_scheduler(
    env: &Env<WithoutTls>,
    versioning: &Versioning,
    initial_version: (u32, u32, u32),
) -> anyhow::Result<()> {
    let target_major: u32 = versioning::VERSION_MAJOR;
    let target_minor: u32 = versioning::VERSION_MINOR;
    let target_patch: u32 = versioning::VERSION_PATCH;
    let target_version = (target_major, target_minor, target_patch);

    if initial_version == target_version {
        return Ok(());
    }

    let upgrade_functions: &[&dyn UpgradeIndexScheduler] = &[
        // List all upgrade functions to apply in order here.
        &v1_30::MigrateNetwork,
        &v1_37::MigrateNetwork,
    ];

    let (initial_major, initial_minor, initial_patch) = initial_version;

    if initial_version > target_version {
        bail!(
                "Database version {initial_major}.{initial_minor}.{initial_patch} is higher than the Meilisearch version {target_major}.{target_minor}.{target_patch}. Downgrade is not supported",
            );
    }

    if initial_version < (1, 12, 0) {
        bail!(
                "Database version {initial_major}.{initial_minor}.{initial_patch} is too old for the experimental dumpless upgrade feature. Please generate a dump using the v{initial_major}.{initial_minor}.{initial_patch} and import it in the v{target_major}.{target_minor}.{target_patch}",
            );
    }

    info!("Upgrading the task queue");
    let mut wtxn = env.write_txn()?;
    let migration_count = upgrade_functions.len();
    for (migration_index, upgrade) in upgrade_functions.iter().enumerate() {
        if upgrade.must_upgrade(initial_version) {
            info!(
                "[{migration_index}/{migration_count}]Applying migration: {}",
                upgrade.description()
            );

            upgrade.upgrade(env, &mut wtxn)?;

            info!(
                "[{}/{migration_count}]Migration applied: {}",
                migration_index + 1,
                upgrade.description()
            )
        } else {
            info!(
                "[{migration_index}/{migration_count}]Skipping unnecessary migration: {}",
                upgrade.description()
            )
        }
    }

    versioning.set_version(&mut wtxn, target_version)?;
    info!("Task queue upgraded, spawning the upgrade database task");

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
            details: Some(Details::UpgradeDatabase { from: initial_version, to: target_version }),
            status: Status::Enqueued,
            kind: KindWithContent::UpgradeDatabase { from: initial_version },
            network: None,
            custom_metadata: None,
        },
    )?;
    wtxn.commit()?;

    Ok(())
}

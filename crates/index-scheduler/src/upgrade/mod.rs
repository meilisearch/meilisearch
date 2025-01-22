use std::path::Path;

use anyhow::bail;
use meilisearch_types::heed;
use meilisearch_types::tasks::{KindWithContent, Status, Task};
use meilisearch_types::versioning::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use time::OffsetDateTime;
use tracing::info;

use crate::queue::TaskQueue;
use crate::IndexSchedulerOptions;

pub fn upgrade_task_queue(
    opt: &IndexSchedulerOptions,
    from: (u32, u32, u32),
) -> anyhow::Result<()> {
    let current_major: u32 = VERSION_MAJOR.parse().unwrap();
    let current_minor: u32 = VERSION_MINOR.parse().unwrap();
    let current_patch: u32 = VERSION_PATCH.parse().unwrap();

    let upgrade_functions =
        [(v1_12_to_current as fn(&Path) -> anyhow::Result<()>, "Upgrading from v1.12 to v1.13")];

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

    info!("Upgrading the task queue");
    for (upgrade, upgrade_name) in upgrade_functions[start..].iter() {
        info!("{upgrade_name}");
        (upgrade)(&opt.tasks_path)?;
    }

    let env = unsafe {
        heed::EnvOpenOptions::new()
            .max_dbs(TaskQueue::nb_db())
            .map_size(opt.task_db_size)
            .open(&opt.tasks_path)
    }?;
    let mut wtxn = env.write_txn()?;
    let queue = TaskQueue::new(&env, &mut wtxn)?;
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
            details: None,
            status: Status::Enqueued,
            kind: KindWithContent::UpgradeDatabase { from },
        },
    )?;
    wtxn.commit()?;
    // Should be pretty much instantaneous since we're the only one reading this env
    env.prepare_for_closing().wait();
    Ok(())
}

/// The task queue is 100% compatible with the previous versions
fn v1_12_to_current(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

use std::path::Path;

use anyhow::bail;
use meilisearch_types::{
    heed,
    tasks::{KindWithContent, Status, Task},
    versioning::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH},
};
use time::OffsetDateTime;
use tracing::info;

use crate::queue::TaskQueue;

pub fn upgrade_task_queue(tasks_path: &Path, from: (u32, u32, u32)) -> anyhow::Result<()> {
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
                "Database version {major}.{minor}.{patch} is higher than the binary version {current_major}.{current_minor}.{current_patch}. Downgrade is not supported",
                );
            } else if major < 1 || (major == current_major && minor < 12) {
                bail!(
                "Database version {major}.{minor}.{patch} is too old for the experimental dumpless upgrade feature. Please generate a dump using the v{major}.{minor}.{patch} and imports it in the v{current_major}.{current_minor}.{current_patch}",
            );
            } else {
                bail!("Unknown database version: v{major}.{minor}.{patch}");
            }
        }
    };

    info!("Upgrading the task queue");
    for (upgrade, upgrade_name) in upgrade_functions[start..].iter() {
        info!("{upgrade_name}");
        (upgrade)(tasks_path)?;
    }

    let env = unsafe {
        heed::EnvOpenOptions::new()
            .max_dbs(19)
            // Since that's the only database memory-mapped currently we don't need to check the budget yet
            .map_size(100 * 1024 * 1024)
            .open(tasks_path)
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

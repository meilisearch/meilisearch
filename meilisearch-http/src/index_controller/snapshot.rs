use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use log::{error, info};
use tokio::fs;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

use crate::helpers::compression;
use super::update_actor::UpdateActorHandle;
use super::uuid_resolver::UuidResolverHandle;

#[allow(dead_code)]
pub struct SnapshotService<U, R> {
    uuid_resolver_handle: R,
    update_handle: U,
    snapshot_period: Duration,
    snapshot_path: PathBuf,
}

impl<U, R> SnapshotService<U, R>
where
    U: UpdateActorHandle,
    R: UuidResolverHandle
{
    pub fn new(
        uuid_resolver_handle: R,
        update_handle: U,
        snapshot_period: Duration,
        snapshot_path: PathBuf,
    ) -> Self {
        Self {
            uuid_resolver_handle,
            update_handle,
            snapshot_period,
            snapshot_path,
        }
    }

    pub async fn run(self) {

        loop {
            sleep(self.snapshot_period).await;
            if let Err(e) = self.perform_snapshot().await {
                error!("{}", e);
            }
        }
    }

    async fn perform_snapshot(&self) -> anyhow::Result<()> {
        if self.snapshot_path.file_name().is_none() {
            bail!("invalid snapshot file path");
        }

        let temp_snapshot_dir = spawn_blocking(move || tempfile::tempdir_in(".")).await??;
        let temp_snapshot_path = temp_snapshot_dir.path().to_owned();

        fs::create_dir_all(&temp_snapshot_path).await?;

        let uuids = self.uuid_resolver_handle.snapshot(temp_snapshot_path.clone()).await?;

        if uuids.is_empty() {
            return Ok(())
        }

        let tasks = uuids
            .iter()
            .map(|&uuid| self.update_handle.snapshot(uuid, temp_snapshot_path.clone()))
            .collect::<Vec<_>>();

        futures::future::try_join_all(tasks).await?;

        let temp_snapshot_file = temp_snapshot_path.with_extension("temp");

        let temp_snapshot_file_clone = temp_snapshot_file.clone();
        let temp_snapshot_path_clone = temp_snapshot_path.clone();
        spawn_blocking(move || compression::to_tar_gz(temp_snapshot_path_clone, temp_snapshot_file_clone)).await??;

        fs::rename(temp_snapshot_file, &self.snapshot_path).await?;

        info!("Created snapshot in {:?}.", self.snapshot_path);

        Ok(())
    }
}

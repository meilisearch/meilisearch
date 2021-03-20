use std::path::PathBuf;
use std::time::Duration;
use std::fs::create_dir_all;

use tokio::time::interval;
use uuid::Uuid;

use super::index_actor::IndexActorHandle;
use super::update_actor::UpdateActorHandle;
use super::uuid_resolver::UuidResolverHandle;

#[allow(dead_code)]
pub struct SnapshotService<B> {
    index_handle: IndexActorHandle,
    uuid_resolver_handle: UuidResolverHandle,
    update_handle: UpdateActorHandle<B>,
    snapshot_period: Duration,
    snapshot_path: PathBuf,
}

impl<B> SnapshotService<B> {
    pub fn new(
        index_handle: IndexActorHandle,
        uuid_resolver_handle: UuidResolverHandle,
        update_handle: UpdateActorHandle<B>,
        snapshot_period: Duration,
        snapshot_path: PathBuf,
    ) -> Self {
        Self {
            index_handle,
            uuid_resolver_handle,
            update_handle,
            snapshot_period,
            snapshot_path,
        }
    }

    pub async fn run(self) {
        let mut interval = interval(self.snapshot_period);

        loop {
            interval.tick().await;
            self.perform_snapshot().await.unwrap();
        }
    }

    async fn perform_snapshot(&self) -> anyhow::Result<()> {
        let temp_snapshot_path = self
            .snapshot_path
            .join(format!("tmp-{}", Uuid::new_v4()));
        create_dir_all(&temp_snapshot_path)?;
        let uuids = self.uuid_resolver_handle.snapshot(temp_snapshot_path.clone()).await?;
        for uuid in uuids {
            self.update_handle.snapshot(uuid, temp_snapshot_path.clone()).await?;
            println!("performed snapshot for index {}", uuid);
        }
        Ok(())
    }
}

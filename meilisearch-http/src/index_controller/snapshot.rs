use std::path::PathBuf;
use std::time::Duration;

use tokio::time::interval;

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
            self.perform_snapshot().await;
        }
    }

    async fn perform_snapshot(&self) {
        println!("performing snapshot in {:?}", self.snapshot_path);
    }
}

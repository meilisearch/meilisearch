use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use log::{error, info};
use tokio::fs;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

use super::update_actor::UpdateActorHandle;
use super::uuid_resolver::UuidResolverHandle;
use crate::helpers::compression;

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
    R: UuidResolverHandle,
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
        if !self.snapshot_path.is_file() {
            bail!("invalid snapshot file path");
        }

        let temp_snapshot_dir = spawn_blocking(move || tempfile::tempdir_in(".")).await??;
        let temp_snapshot_path = temp_snapshot_dir.path().to_owned();

        fs::create_dir_all(&temp_snapshot_path).await?;

        let uuids = self
            .uuid_resolver_handle
            .snapshot(temp_snapshot_path.clone())
            .await?;

        if uuids.is_empty() {
            return Ok(());
        }

        let tasks = uuids
            .iter()
            .map(|&uuid| {
                self.update_handle
                    .snapshot(uuid, temp_snapshot_path.clone())
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(tasks).await?;

        let temp_snapshot_file = temp_snapshot_path.with_extension("temp");

        let temp_snapshot_file_clone = temp_snapshot_file.clone();
        let temp_snapshot_path_clone = temp_snapshot_path.clone();
        spawn_blocking(move || {
            compression::to_tar_gz(temp_snapshot_path_clone, temp_snapshot_file_clone)
        })
        .await??;

        fs::rename(temp_snapshot_file, &self.snapshot_path).await?;

        info!("Created snapshot in {:?}.", self.snapshot_path);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use futures::future::{ok, err};
    use rand::Rng;
    use tokio::time::timeout;
    use uuid::Uuid;

    use super::*;
    use crate::index_controller::update_actor::{MockUpdateActorHandle, UpdateError};
    use crate::index_controller::uuid_resolver::{MockUuidResolverHandle, UuidError};

    #[actix_rt::test]
    async fn test_normal() {
        let mut rng = rand::thread_rng();
        let uuids_num = rng.gen_range(5, 10);
        let uuids = (0..uuids_num).map(|_| Uuid::new_v4()).collect::<Vec<_>>();

        let mut uuid_resolver = MockUuidResolverHandle::new();
        let uuids_clone = uuids.clone();
        uuid_resolver
            .expect_snapshot()
            .times(1)
            .returning(move |_| Box::pin(ok(uuids_clone.clone())));

        let mut update_handle = MockUpdateActorHandle::new();
        let uuids_clone = uuids.clone();
        update_handle
            .expect_snapshot()
            .withf(move |uuid, _path| uuids_clone.contains(uuid))
            .times(uuids_num)
            .returning(move |_, _| Box::pin(ok(())));

        let snapshot_path = tempfile::NamedTempFile::new_in(".").unwrap();
        let snapshot_service = SnapshotService::new(
            uuid_resolver,
            update_handle,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
        );

        snapshot_service.perform_snapshot().await.unwrap();
    }

    #[actix_rt::test]
    async fn bad_file_name() {
        let uuid_resolver = MockUuidResolverHandle::new();
        let update_handle = MockUpdateActorHandle::new();

        let snapshot_service = SnapshotService::new(
            uuid_resolver,
            update_handle,
            Duration::from_millis(100),
            "directory/".into(),
        );

        assert!(snapshot_service.perform_snapshot().await.is_err());
    }

    #[actix_rt::test]
    async fn error_performing_uuid_snapshot() {
        let mut uuid_resolver = MockUuidResolverHandle::new();
        uuid_resolver
            .expect_snapshot()
            .times(1)
            // abitrary error
            .returning(|_| Box::pin(err(UuidError::NameAlreadyExist)));

        let update_handle = MockUpdateActorHandle::new();

        let snapshot_path = tempfile::NamedTempFile::new_in(".").unwrap();
        let snapshot_service = SnapshotService::new(
            uuid_resolver,
            update_handle,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
        );

        assert!(snapshot_service.perform_snapshot().await.is_err());
        // Nothing was written to the file
        assert_eq!(snapshot_path.as_file().metadata().unwrap().len(), 0);
    }

    #[actix_rt::test]
    async fn error_performing_index_snapshot() {
        let uuid = Uuid::new_v4();
        let mut uuid_resolver = MockUuidResolverHandle::new();
        uuid_resolver
            .expect_snapshot()
            .times(1)
            .returning(move |_| Box::pin(ok(vec![uuid])));

        let mut update_handle = MockUpdateActorHandle::new();
        update_handle
            .expect_snapshot()
            // abitrary error
            .returning(|_, _| Box::pin(err(UpdateError::UnexistingUpdate(0))));

        let snapshot_path = tempfile::NamedTempFile::new_in(".").unwrap();
        let snapshot_service = SnapshotService::new(
            uuid_resolver,
            update_handle,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
        );

        assert!(snapshot_service.perform_snapshot().await.is_err());
        // Nothing was written to the file
        assert_eq!(snapshot_path.as_file().metadata().unwrap().len(), 0);
    }

    #[actix_rt::test]
    async fn test_loop() {
        let mut uuid_resolver = MockUuidResolverHandle::new();
        uuid_resolver
            .expect_snapshot()
            // we expect the funtion to be called between 2 and 3 time in the given interval.
            .times(2..4)
            // abitrary error, to short-circuit the function
            .returning(move |_| Box::pin(err(UuidError::NameAlreadyExist)));

        let update_handle = MockUpdateActorHandle::new();

        let snapshot_path = tempfile::NamedTempFile::new_in(".").unwrap();
        let snapshot_service = SnapshotService::new(
            uuid_resolver,
            update_handle,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
        );

        let _ = timeout(Duration::from_millis(300), snapshot_service.run()).await;
    }
}

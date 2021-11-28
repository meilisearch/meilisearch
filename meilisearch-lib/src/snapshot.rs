use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::bail;
use log::{info, trace};
use tokio::time::sleep;
use walkdir::WalkDir;

use crate::compression::from_tar_gz;
use crate::tasks::task::Job;
use crate::tasks::task_store::TaskStore;

pub struct SnapshotService {
    pub(crate) db_path: PathBuf,
    pub(crate) snapshot_period: Duration,
    pub(crate) snapshot_path: PathBuf,
    pub(crate) index_size: usize,
    pub(crate) meta_env_size: usize,
    pub(crate) task_store: TaskStore,
}

impl SnapshotService {
    pub async fn run(self) {
        info!(
            "Snapshot scheduled every {}s.",
            self.snapshot_period.as_secs()
        );
        loop {
            let snapshot_job = SnapshotJob {
                dest_path: self.snapshot_path.clone(),
                src_path: self.db_path.clone(),
                meta_env_size: self.meta_env_size,
                index_size: self.index_size,
            };
            let job = Job::Snapshot(snapshot_job);
            self.task_store.register_ghost_task(job).await;

            sleep(self.snapshot_period).await;
        }
    }
}

#[allow(dead_code)]
pub fn load_snapshot(
    db_path: impl AsRef<Path>,
    snapshot_path: impl AsRef<Path>,
    ignore_snapshot_if_db_exists: bool,
    ignore_missing_snapshot: bool,
) -> anyhow::Result<()> {
    if !db_path.as_ref().exists() && snapshot_path.as_ref().exists() {
        match from_tar_gz(snapshot_path, &db_path) {
            Ok(()) => Ok(()),
            Err(e) => {
                //clean created db folder
                std::fs::remove_dir_all(&db_path)?;
                Err(e)
            }
        }
    } else if db_path.as_ref().exists() && !ignore_snapshot_if_db_exists {
        bail!(
            "database already exists at {:?}, try to delete it or rename it",
            db_path
                .as_ref()
                .canonicalize()
                .unwrap_or_else(|_| db_path.as_ref().to_owned())
        )
    } else if !snapshot_path.as_ref().exists() && !ignore_missing_snapshot {
        bail!(
            "snapshot doesn't exist at {:?}",
            snapshot_path
                .as_ref()
                .canonicalize()
                .unwrap_or_else(|_| snapshot_path.as_ref().to_owned())
        )
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub struct SnapshotJob {
    dest_path: PathBuf,
    src_path: PathBuf,

    meta_env_size: usize,
    index_size: usize,
}

impl SnapshotJob {
    pub async fn run(self) -> anyhow::Result<()> {
        tokio::task::spawn_blocking(|| self.run_sync()).await??;

        Ok(())
    }

    fn run_sync(self) -> anyhow::Result<()> {
        trace!("Performing snapshot.");

        let snapshot_dir = self.dest_path.clone();
        std::fs::create_dir_all(&snapshot_dir)?;
        let temp_snapshot_dir = tempfile::tempdir()?;
        let temp_snapshot_path = temp_snapshot_dir.path();

        dbg!(self.snapshot_meta_env(temp_snapshot_path))?;
        dbg!(self.snapshot_file_store(temp_snapshot_path))?;
        dbg!(self.snapshot_indexes(temp_snapshot_path))?;

        let db_name = self
            .src_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("data.ms")
            .to_string();

        let snapshot_path = self.dest_path.join(format!("{}.snapshot", db_name));
        let temp_snapshot_file = tempfile::NamedTempFile::new_in(&snapshot_dir)?;
        let temp_snapshot_file_path = temp_snapshot_file.path().to_owned();
        crate::compression::to_tar_gz(temp_snapshot_path, temp_snapshot_file_path)?;
        temp_snapshot_file.persist(&snapshot_path)?;

        trace!("Created snapshot in {:?}.", snapshot_path);

        Ok(())
    }

    fn snapshot_meta_env(&self, path: &Path) -> anyhow::Result<()> {
        let mut options = heed::EnvOpenOptions::new();
        options.map_size(self.meta_env_size);
        let env = options.open(&self.src_path)?;

        let dst = path.join("data.mdb");
        env.copy_to_path(dst, heed::CompactionOption::Enabled)?;

        Ok(())
    }

    fn snapshot_file_store(&self, path: &Path) -> anyhow::Result<()> {
        // for now we simply copy the updates/updates_files
        // FIXME(marin): We may copy more files than necessary, if new files are added while we are
        // performing the snapshop. We need a way to filter them out.

        let update_files_path = self.src_path.join("updates/updates_files/");
        let dst = path.join("updates/updates_files/");
        std::fs::create_dir_all(&dst)?;

        for entry in WalkDir::new(update_files_path).into_iter().skip(1) {
            let entry = entry?;
            let name = entry.file_name();
            let dst = dst.join(name);
            std::fs::copy(entry.path(), dst)?;
        }

        Ok(())
    }

    fn snapshot_indexes(&self, path: &Path) -> anyhow::Result<()> {
        let indexes_path = self.src_path.join("indexes/");
        let dst = path.join("indexes/");

        for entry in WalkDir::new(indexes_path).max_depth(1).into_iter().skip(1) {
            let entry = entry?;
            let name = entry.file_name();
            let dst = dst.join(name);

            std::fs::create_dir_all(&dst)?;

            let dst = dst.join("data.mdb");

            let mut options = heed::EnvOpenOptions::new();
            options.map_size(self.index_size);
            let env = options.open(dbg!(entry.path()))?;

            env.copy_to_path(dbg!(dst), heed::CompactionOption::Enabled)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, sync::Arc};

    use futures::future::{err, ok};
    use once_cell::sync::Lazy;
    use rand::Rng;
    use uuid::Uuid;

    use crate::index::error::IndexError;
    use crate::index::test::Mocker;
    use crate::index::{error::Result as IndexResult, Index};
    use crate::index_controller::index_resolver::error::IndexResolverError;
    use crate::index_controller::index_resolver::index_store::MockIndexStore;
    use crate::index_controller::index_resolver::uuid_store::MockUuidStore;
    use crate::index_controller::index_resolver::IndexResolver;
    use crate::index_controller::updates::create_update_handler;

    use super::*;

    fn setup() {
        static SETUP: Lazy<()> = Lazy::new(|| {
            if cfg!(windows) {
                std::env::set_var("TMP", ".");
            } else {
                std::env::set_var("TMPDIR", ".");
            }
        });

        // just deref to make sure the env is setup
        *SETUP
    }

    #[actix_rt::test]
    async fn test_normal() {
        setup();

        let mut rng = rand::thread_rng();
        let uuids_num: usize = rng.gen_range(5..10);
        let uuids = (0..uuids_num)
            .map(|_| Uuid::new_v4())
            .collect::<HashSet<_>>();

        let mut uuid_store = MockUuidStore::new();
        let uuids_clone = uuids.clone();
        uuid_store
            .expect_snapshot()
            .times(1)
            .returning(move |_| Box::pin(ok(uuids_clone.clone())));

        let mut indexes = uuids.clone().into_iter().map(|uuid| {
            let mocker = Mocker::default();
            mocker
                .when("snapshot")
                .times(1)
                .then(|_: &Path| -> IndexResult<()> { Ok(()) });
            mocker.when("uuid").then(move |_: ()| uuid);
            Index::faux(mocker)
        });

        let uuids_clone = uuids.clone();
        let mut index_store = MockIndexStore::new();
        index_store
            .expect_get()
            .withf(move |uuid| uuids_clone.contains(uuid))
            .times(uuids_num)
            .returning(move |_| Box::pin(ok(Some(indexes.next().unwrap()))));

        let index_resolver = Arc::new(IndexResolver::new(uuid_store, index_store));

        let dir = tempfile::tempdir().unwrap();
        let update_sender =
            create_update_handler(index_resolver.clone(), dir.path(), 4096 * 100).unwrap();

        let snapshot_path = tempfile::tempdir().unwrap();
        let snapshot_service = SnapshotService::new(
            index_resolver,
            update_sender,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
            // this should do nothing
            snapshot_path.path().to_owned(),
            "data.ms".to_string(),
        );

        snapshot_service.perform_snapshot().await.unwrap();
    }

    #[actix_rt::test]
    async fn error_performing_uuid_snapshot() {
        setup();

        let mut uuid_store = MockUuidStore::new();
        uuid_store.expect_snapshot().once().returning(move |_| {
            Box::pin(err(IndexResolverError::IndexAlreadyExists(
                "test".to_string(),
            )))
        });

        let mut index_store = MockIndexStore::new();
        index_store.expect_get().never();

        let index_resolver = Arc::new(IndexResolver::new(uuid_store, index_store));

        let dir = tempfile::tempdir().unwrap();
        let update_sender =
            create_update_handler(index_resolver.clone(), dir.path(), 4096 * 100).unwrap();

        let snapshot_path = tempfile::tempdir().unwrap();
        let snapshot_service = SnapshotService::new(
            index_resolver,
            update_sender,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
            // this should do nothing
            snapshot_path.path().to_owned(),
            "data.ms".to_string(),
        );

        assert!(snapshot_service.perform_snapshot().await.is_err());
    }

    #[actix_rt::test]
    async fn error_performing_index_snapshot() {
        setup();

        let uuids: HashSet<Uuid> = vec![Uuid::new_v4()].into_iter().collect();

        let mut uuid_store = MockUuidStore::new();
        let uuids_clone = uuids.clone();
        uuid_store
            .expect_snapshot()
            .once()
            .returning(move |_| Box::pin(ok(uuids_clone.clone())));

        let mut indexes = uuids.clone().into_iter().map(|uuid| {
            let mocker = Mocker::default();
            // index returns random error
            mocker.when("snapshot").then(|_: &Path| -> IndexResult<()> {
                Err(IndexError::DocumentNotFound("1".to_string()))
            });
            mocker.when("uuid").then(move |_: ()| uuid);
            Index::faux(mocker)
        });

        let uuids_clone = uuids.clone();
        let mut index_store = MockIndexStore::new();
        index_store
            .expect_get()
            .withf(move |uuid| uuids_clone.contains(uuid))
            .once()
            .returning(move |_| Box::pin(ok(Some(indexes.next().unwrap()))));

        let index_resolver = Arc::new(IndexResolver::new(uuid_store, index_store));

        let dir = tempfile::tempdir().unwrap();
        let update_sender =
            create_update_handler(index_resolver.clone(), dir.path(), 4096 * 100).unwrap();

        let snapshot_path = tempfile::tempdir().unwrap();
        let snapshot_service = SnapshotService::new(
            index_resolver,
            update_sender,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
            // this should do nothing
            snapshot_path.path().to_owned(),
            "data.ms".to_string(),
        );

        assert!(snapshot_service.perform_snapshot().await.is_err());
    }
}

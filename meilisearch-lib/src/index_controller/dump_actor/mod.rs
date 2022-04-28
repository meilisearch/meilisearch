use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::bail;
use log::{info, trace};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub use actor::DumpActor;
pub use handle_impl::*;
use meilisearch_auth::AuthController;
pub use message::DumpMsg;
use tempfile::TempDir;
use tokio::fs::create_dir_all;
use tokio::sync::{oneshot, RwLock};

use crate::analytics;
use crate::compression::{from_tar_gz, to_tar_gz};
use crate::index_controller::dump_actor::error::DumpActorError;
use crate::index_controller::dump_actor::loaders::{v2, v3, v4};
use crate::options::IndexerOpts;
use crate::tasks::task::Job;
use crate::tasks::Scheduler;
use crate::update_file_store::UpdateFileStore;
use error::Result;

mod actor;
mod compat;
pub mod error;
mod handle_impl;
mod loaders;
mod message;

const META_FILE_NAME: &str = "metadata.json";

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    db_version: String,
    index_db_size: usize,
    update_db_size: usize,
    #[serde(with = "time::serde::rfc3339")]
    dump_date: OffsetDateTime,
}

impl Metadata {
    pub fn new(index_db_size: usize, update_db_size: usize) -> Self {
        Self {
            db_version: env!("CARGO_PKG_VERSION").to_string(),
            index_db_size,
            update_db_size,
            dump_date: OffsetDateTime::now_utc(),
        }
    }
}

#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait DumpActorHandle {
    /// Start the creation of a dump
    /// Implementation: [handle_impl::DumpActorHandleImpl::create_dump]
    async fn create_dump(&self) -> Result<DumpInfo>;

    /// Return the status of an already created dump
    /// Implementation: [handle_impl::DumpActorHandleImpl::dump_info]
    async fn dump_info(&self, uid: String) -> Result<DumpInfo>;
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetadataV1 {
    pub db_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "dumpVersion")]
pub enum MetadataVersion {
    V1(MetadataV1),
    V2(Metadata),
    V3(Metadata),
    V4(Metadata),
}

impl MetadataVersion {
    pub fn load_dump(
        self,
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        index_db_size: usize,
        meta_env_size: usize,
        indexing_options: &IndexerOpts,
    ) -> anyhow::Result<()> {
        match self {
            MetadataVersion::V1(_meta) => {
                anyhow::bail!("The version 1 of the dumps is not supported anymore. You can re-export your dump from a version between 0.21 and 0.24, or start fresh from a version 0.25 onwards.")
            }
            MetadataVersion::V2(meta) => v2::load_dump(
                meta,
                src,
                dst,
                index_db_size,
                meta_env_size,
                indexing_options,
            )?,
            MetadataVersion::V3(meta) => v3::load_dump(
                meta,
                src,
                dst,
                index_db_size,
                meta_env_size,
                indexing_options,
            )?,
            MetadataVersion::V4(meta) => v4::load_dump(
                meta,
                src,
                dst,
                index_db_size,
                meta_env_size,
                indexing_options,
            )?,
        }

        Ok(())
    }

    pub fn new_v4(index_db_size: usize, update_db_size: usize) -> Self {
        let meta = Metadata::new(index_db_size, update_db_size);
        Self::V4(meta)
    }

    pub fn db_version(&self) -> &str {
        match self {
            Self::V1(meta) => &meta.db_version,
            Self::V2(meta) | Self::V3(meta) | Self::V4(meta) => &meta.db_version,
        }
    }

    pub fn version(&self) -> &str {
        match self {
            MetadataVersion::V1(_) => "V1",
            MetadataVersion::V2(_) => "V2",
            MetadataVersion::V3(_) => "V3",
            MetadataVersion::V4(_) => "V4",
        }
    }

    pub fn dump_date(&self) -> Option<&OffsetDateTime> {
        match self {
            MetadataVersion::V1(_) => None,
            MetadataVersion::V2(meta) | MetadataVersion::V3(meta) | MetadataVersion::V4(meta) => {
                Some(&meta.dump_date)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DumpStatus {
    Done,
    InProgress,
    Failed,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DumpInfo {
    pub uid: String,
    pub status: DumpStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    started_at: OffsetDateTime,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    finished_at: Option<OffsetDateTime>,
}

impl DumpInfo {
    pub fn new(uid: String, status: DumpStatus) -> Self {
        Self {
            uid,
            status,
            error: None,
            started_at: OffsetDateTime::now_utc(),
            finished_at: None,
        }
    }

    pub fn with_error(&mut self, error: String) {
        self.status = DumpStatus::Failed;
        self.finished_at = Some(OffsetDateTime::now_utc());
        self.error = Some(error);
    }

    pub fn done(&mut self) {
        self.finished_at = Some(OffsetDateTime::now_utc());
        self.status = DumpStatus::Done;
    }

    pub fn dump_already_in_progress(&self) -> bool {
        self.status == DumpStatus::InProgress
    }
}

pub fn load_dump(
    dst_path: impl AsRef<Path>,
    src_path: impl AsRef<Path>,
    ignore_dump_if_db_exists: bool,
    ignore_missing_dump: bool,
    index_db_size: usize,
    update_db_size: usize,
    indexer_opts: &IndexerOpts,
) -> anyhow::Result<()> {
    let empty_db = crate::is_empty_db(&dst_path);
    let src_path_exists = src_path.as_ref().exists();

    if empty_db && src_path_exists {
        let (tmp_src, tmp_dst, meta) = extract_dump(&dst_path, &src_path)?;
        meta.load_dump(
            tmp_src.path(),
            tmp_dst.path(),
            index_db_size,
            update_db_size,
            indexer_opts,
        )?;
        persist_dump(&dst_path, tmp_dst)?;
        Ok(())
    } else if !empty_db && !ignore_dump_if_db_exists {
        bail!(
            "database already exists at {:?}, try to delete it or rename it",
            dst_path
                .as_ref()
                .canonicalize()
                .unwrap_or_else(|_| dst_path.as_ref().to_owned())
        )
    } else if !src_path_exists && !ignore_missing_dump {
        bail!("dump doesn't exist at {:?}", src_path.as_ref())
    } else {
        // there is nothing to do
        Ok(())
    }
}

fn extract_dump(
    dst_path: impl AsRef<Path>,
    src_path: impl AsRef<Path>,
) -> anyhow::Result<(TempDir, TempDir, MetadataVersion)> {
    // Setup a temp directory path in the same path as the database, to prevent cross devices
    // references.
    let temp_path = dst_path
        .as_ref()
        .parent()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| ".".into());

    let tmp_src = tempfile::tempdir_in(temp_path)?;
    let tmp_src_path = tmp_src.path();

    from_tar_gz(&src_path, tmp_src_path)?;

    let meta_path = tmp_src_path.join(META_FILE_NAME);
    let mut meta_file = File::open(&meta_path)?;
    let meta: MetadataVersion = serde_json::from_reader(&mut meta_file)?;

    if !dst_path.as_ref().exists() {
        std::fs::create_dir_all(dst_path.as_ref())?;
    }

    let tmp_dst = tempfile::tempdir_in(dst_path.as_ref())?;

    info!(
        "Loading dump {}, dump database version: {}, dump version: {}",
        meta.dump_date()
            .map(|t| format!("from {}", t))
            .unwrap_or_else(String::new),
        meta.db_version(),
        meta.version()
    );

    Ok((tmp_src, tmp_dst, meta))
}

fn persist_dump(dst_path: impl AsRef<Path>, tmp_dst: TempDir) -> anyhow::Result<()> {
    let persisted_dump = tmp_dst.into_path();

    // Delete everything in the `data.ms` except the tempdir.
    if dst_path.as_ref().exists() {
        for file in dst_path.as_ref().read_dir().unwrap() {
            let file = file.unwrap().path();
            if file.file_name() == persisted_dump.file_name() {
                continue;
            }

            if file.is_file() {
                std::fs::remove_file(&file)?;
            } else {
                std::fs::remove_dir_all(&file)?;
            }
        }
    }

    // Move the whole content of the tempdir into the `data.ms`.
    for file in persisted_dump.read_dir().unwrap() {
        let file = file.unwrap().path();

        std::fs::rename(&file, &dst_path.as_ref().join(file.file_name().unwrap()))?;
    }

    // Delete the empty tempdir.
    std::fs::remove_dir_all(&persisted_dump)?;

    Ok(())
}

struct DumpJob {
    dump_path: PathBuf,
    db_path: PathBuf,
    update_file_store: UpdateFileStore,
    scheduler: Arc<RwLock<Scheduler>>,
    uid: String,
    update_db_size: usize,
    index_db_size: usize,
}

impl DumpJob {
    async fn run(self) -> Result<()> {
        trace!("Performing dump.");

        create_dir_all(&self.dump_path).await?;

        let temp_dump_dir = tokio::task::spawn_blocking(tempfile::TempDir::new).await??;
        let temp_dump_path = temp_dump_dir.path().to_owned();

        let meta = MetadataVersion::new_v4(self.index_db_size, self.update_db_size);
        let meta_path = temp_dump_path.join(META_FILE_NAME);
        let mut meta_file = File::create(&meta_path)?;
        serde_json::to_writer(&mut meta_file, &meta)?;
        analytics::copy_user_id(&self.db_path, &temp_dump_path);

        create_dir_all(&temp_dump_path.join("indexes")).await?;

        let (sender, receiver) = oneshot::channel();

        self.scheduler
            .write()
            .await
            .schedule_job(Job::Dump {
                ret: sender,
                path: temp_dump_path.clone(),
            })
            .await;

        // wait until the job has started performing before finishing the dump process
        let sender = receiver.await??;

        AuthController::dump(&self.db_path, &temp_dump_path)?;

        //TODO(marin): this is not right, the scheduler should dump itself, not do it here...
        self.scheduler
            .read()
            .await
            .dump(&temp_dump_path, self.update_file_store.clone())
            .await?;

        let dump_path = tokio::task::spawn_blocking(move || -> Result<PathBuf> {
            // for now we simply copy the updates/updates_files
            // FIXME: We may copy more files than necessary, if new files are added while we are
            // performing the dump. We need a way to filter them out.

            let temp_dump_file = tempfile::NamedTempFile::new_in(&self.dump_path)?;
            to_tar_gz(temp_dump_path, temp_dump_file.path())
                .map_err(|e| DumpActorError::Internal(e.into()))?;

            let dump_path = self.dump_path.join(self.uid).with_extension("dump");
            temp_dump_file.persist(&dump_path)?;

            Ok(dump_path)
        })
        .await??;

        // notify the update loop that we are finished performing the dump.
        let _ = sender.send(());

        info!("Created dump in {:?}.", dump_path);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use nelson::Mocker;
    use once_cell::sync::Lazy;

    use super::*;
    use crate::index_resolver::error::IndexResolverError;
    use crate::options::SchedulerConfig;
    use crate::tasks::error::Result as TaskResult;
    use crate::tasks::task::{Task, TaskId};
    use crate::tasks::{MockTaskPerformer, TaskFilter, TaskStore};
    use crate::update_file_store::UpdateFileStore;

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
    async fn test_dump_normal() {
        setup();

        let tmp = tempfile::tempdir().unwrap();

        let mocker = Mocker::default();
        let update_file_store = UpdateFileStore::mock(mocker);

        let mut performer = MockTaskPerformer::new();
        performer
            .expect_process_job()
            .once()
            .returning(|j| match j {
                Job::Dump { ret, .. } => {
                    let (sender, _receiver) = oneshot::channel();
                    ret.send(Ok(sender)).unwrap();
                }
                _ => unreachable!(),
            });
        let performer = Arc::new(performer);
        let mocker = Mocker::default();
        mocker
            .when::<(&Path, UpdateFileStore), TaskResult<()>>("dump")
            .then(|_| Ok(()));
        mocker
            .when::<(Option<TaskId>, Option<TaskFilter>, Option<usize>), TaskResult<Vec<Task>>>(
                "list_tasks",
            )
            .then(|_| Ok(Vec::new()));
        let store = TaskStore::mock(mocker);
        let config = SchedulerConfig::default();

        let scheduler = Scheduler::new(store, performer, config).unwrap();

        let task = DumpJob {
            dump_path: tmp.path().into(),
            // this should do nothing
            update_file_store,
            db_path: tmp.path().into(),
            uid: String::from("test"),
            update_db_size: 4096 * 10,
            index_db_size: 4096 * 10,
            scheduler,
        };

        task.run().await.unwrap();
    }

    #[actix_rt::test]
    async fn error_performing_dump() {
        let tmp = tempfile::tempdir().unwrap();

        let mocker = Mocker::default();
        let file_store = UpdateFileStore::mock(mocker);

        let mocker = Mocker::default();
        mocker
            .when::<(Option<TaskId>, Option<TaskFilter>, Option<usize>), TaskResult<Vec<Task>>>(
                "list_tasks",
            )
            .then(|_| Ok(Vec::new()));
        let task_store = TaskStore::mock(mocker);
        let mut performer = MockTaskPerformer::new();
        performer
            .expect_process_job()
            .once()
            .returning(|job| match job {
                Job::Dump { ret, .. } => drop(ret.send(Err(IndexResolverError::BadlyFormatted(
                    "blabla".to_string(),
                )))),
                _ => unreachable!(),
            });
        let performer = Arc::new(performer);

        let scheduler = Scheduler::new(task_store, performer, SchedulerConfig::default()).unwrap();

        let task = DumpJob {
            dump_path: tmp.path().into(),
            // this should do nothing
            db_path: tmp.path().into(),
            update_file_store: file_store,
            uid: String::from("test"),
            update_db_size: 4096 * 10,
            index_db_size: 4096 * 10,
            scheduler,
        };

        assert!(task.run().await.is_err());
    }
}

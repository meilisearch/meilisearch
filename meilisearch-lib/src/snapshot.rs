use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::bail;
use fs_extra::dir::{self, CopyOptions};
use log::{info, trace};
use tokio::time::sleep;
use walkdir::WalkDir;

use crate::compression::from_tar_gz;
use crate::index_controller::versioning::VERSION_FILE_NAME;
use crate::tasks::task::Job;
use crate::tasks::TaskStore;

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
            self.task_store.register_job(job).await;

            sleep(self.snapshot_period).await;
        }
    }
}

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

        self.snapshot_version_file(temp_snapshot_path)?;
        self.snapshot_meta_env(temp_snapshot_path)?;
        self.snapshot_file_store(temp_snapshot_path)?;
        self.snapshot_indexes(temp_snapshot_path)?;
        self.snapshot_auth(temp_snapshot_path)?;

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
        let _file = temp_snapshot_file.persist(&snapshot_path)?;

        #[cfg(unix)]
        {
            use std::fs::Permissions;
            use std::os::unix::fs::PermissionsExt;

            let perm = Permissions::from_mode(0o644);
            _file.set_permissions(perm)?;
        }

        trace!("Created snapshot in {:?}.", snapshot_path);

        Ok(())
    }

    fn snapshot_version_file(&self, path: &Path) -> anyhow::Result<()> {
        let dst = path.join(VERSION_FILE_NAME);
        let src = self.src_path.join(VERSION_FILE_NAME);

        fs::copy(src, dst)?;

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

        let dst = path.join("updates");
        fs::create_dir_all(&dst)?;
        let options = CopyOptions::default();
        dir::copy(self.src_path.join("updates/updates_files"), dst, &options)?;

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
            let env = options.open(entry.path())?;

            env.copy_to_path(dst, heed::CompactionOption::Enabled)?;
        }

        Ok(())
    }

    fn snapshot_auth(&self, path: &Path) -> anyhow::Result<()> {
        let auth_path = self.src_path.join("auth");
        let dst = path.join("auth");
        std::fs::create_dir_all(&dst)?;
        let dst = dst.join("data.mdb");

        let mut options = heed::EnvOpenOptions::new();
        options.map_size(1_073_741_824);
        let env = options.open(auth_path)?;
        env.copy_to_path(dst, heed::CompactionOption::Enabled)?;

        Ok(())
    }
}

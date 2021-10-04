use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use log::{error, info, trace};
use tokio::fs;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

use crate::compression::from_tar_gz;
use crate::index_controller::updates::UpdateMsg;

use super::index_resolver::HardStateIndexResolver;
use super::updates::UpdateSender;

pub struct SnapshotService {
    index_resolver: Arc<HardStateIndexResolver>,
    update_sender: UpdateSender,
    snapshot_period: Duration,
    snapshot_path: PathBuf,
    db_name: String,
}

impl SnapshotService {
    pub fn new(
        index_resolver: Arc<HardStateIndexResolver>,
        update_sender: UpdateSender,
        snapshot_period: Duration,
        snapshot_path: PathBuf,
        db_name: String,
    ) -> Self {
        Self {
            index_resolver,
            update_sender,
            snapshot_period,
            snapshot_path,
            db_name,
        }
    }

    pub async fn run(self) {
        info!(
            "Snapshot scheduled every {}s.",
            self.snapshot_period.as_secs()
        );
        loop {
            if let Err(e) = self.perform_snapshot().await {
                error!("Error while performing snapshot: {}", e);
            }
            sleep(self.snapshot_period).await;
        }
    }

    async fn perform_snapshot(&self) -> anyhow::Result<()> {
        trace!("Performing snapshot.");

        let snapshot_dir = self.snapshot_path.clone();
        fs::create_dir_all(&snapshot_dir).await?;
        let temp_snapshot_dir = spawn_blocking(tempfile::tempdir).await??;
        let temp_snapshot_path = temp_snapshot_dir.path().to_owned();

        let indexes = self
            .index_resolver
            .snapshot(temp_snapshot_path.clone())
            .await?;

        if indexes.is_empty() {
            return Ok(());
        }

        UpdateMsg::snapshot(&self.update_sender, temp_snapshot_path.clone(), indexes).await?;

        let snapshot_path = self
            .snapshot_path
            .join(format!("{}.snapshot", self.db_name));
        let snapshot_path = spawn_blocking(move || -> anyhow::Result<PathBuf> {
            let temp_snapshot_file = tempfile::NamedTempFile::new()?;
            let temp_snapshot_file_path = temp_snapshot_file.path().to_owned();
            crate::compression::to_tar_gz(temp_snapshot_path, temp_snapshot_file_path)?;
            temp_snapshot_file.persist(&snapshot_path)?;
            Ok(snapshot_path)
        })
        .await??;

        trace!("Created snapshot in {:?}.", snapshot_path);

        Ok(())
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

#[cfg(test)]
mod test {
    //use std::iter::FromIterator;
    //use std::{collections::HashSet, sync::Arc};

    //use futures::future::{err, ok};
    //use rand::Rng;
    //use tokio::time::timeout;
    //use uuid::Uuid;

    //use super::*;

    //#[actix_rt::test]
    //async fn test_normal() {
        //let mut rng = rand::thread_rng();
        //let uuids_num: usize = rng.gen_range(5..10);
        //let uuids = (0..uuids_num)
            //.map(|_| Uuid::new_v4())
            //.collect::<HashSet<_>>();

        //let mut uuid_resolver = MockUuidResolverHandle::new();
        //let uuids_clone = uuids.clone();
        //uuid_resolver
            //.expect_snapshot()
            //.times(1)
            //.returning(move |_| Box::pin(ok(uuids_clone.clone())));

        //let uuids_clone = uuids.clone();
        //let mut index_handle = MockIndexActorHandle::new();
        //index_handle
            //.expect_snapshot()
            //.withf(move |uuid, _path| uuids_clone.contains(uuid))
            //.times(uuids_num)
            //.returning(move |_, _| Box::pin(ok(())));

        //let dir = tempfile::tempdir_in(".").unwrap();
        //let handle = Arc::new(index_handle);
        //let update_handle =
            //UpdateActorHandleImpl::<Vec<u8>>::new(handle.clone(), dir.path(), 4096 * 100).unwrap();

        //let snapshot_path = tempfile::tempdir_in(".").unwrap();
        //let snapshot_service = SnapshotService::new(
            //uuid_resolver,
            //update_handle,
            //Duration::from_millis(100),
            //snapshot_path.path().to_owned(),
            //"data.ms".to_string(),
        //);

        //snapshot_service.perform_snapshot().await.unwrap();
    //}

    //#[actix_rt::test]
    //async fn error_performing_uuid_snapshot() {
    //let mut uuid_resolver = MockUuidResolverHandle::new();
    //uuid_resolver
    //.expect_snapshot()
    //.times(1)
    ////abitrary error
    //.returning(|_| Box::pin(err(UuidResolverError::NameAlreadyExist)));

    //let update_handle = MockUpdateActorHandle::new();

    //let snapshot_path = tempfile::tempdir_in(".").unwrap();
    //let snapshot_service = SnapshotService::new(
    //uuid_resolver,
    //update_handle,
    //Duration::from_millis(100),
    //snapshot_path.path().to_owned(),
    //"data.ms".to_string(),
    //);

    //assert!(snapshot_service.perform_snapshot().await.is_err());
    ////Nothing was written to the file
    //assert!(!snapshot_path.path().join("data.ms.snapshot").exists());
    //}

    //#[actix_rt::test]
    //async fn error_performing_index_snapshot() {
    //let uuid = Uuid::new_v4();
    //let mut uuid_resolver = MockUuidResolverHandle::new();
    //uuid_resolver
    //.expect_snapshot()
    //.times(1)
    //.returning(move |_| Box::pin(ok(HashSet::from_iter(Some(uuid)))));

    //let mut update_handle = MockUpdateActorHandle::new();
    //update_handle
    //.expect_snapshot()
    ////abitrary error
    //.returning(|_, _| Box::pin(err(UpdateActorError::UnexistingUpdate(0))));

    //let snapshot_path = tempfile::tempdir_in(".").unwrap();
    //let snapshot_service = SnapshotService::new(
    //uuid_resolver,
    //update_handle,
    //Duration::from_millis(100),
    //snapshot_path.path().to_owned(),
    //"data.ms".to_string(),
    //);

    //assert!(snapshot_service.perform_snapshot().await.is_err());
    ////Nothing was written to the file
    //assert!(!snapshot_path.path().join("data.ms.snapshot").exists());
    //}

    //#[actix_rt::test]
    //async fn test_loop() {
    //let mut uuid_resolver = MockUuidResolverHandle::new();
    //uuid_resolver
    //.expect_snapshot()
    ////we expect the funtion to be called between 2 and 3 time in the given interval.
    //.times(2..4)
    ////abitrary error, to short-circuit the function
    //.returning(move |_| Box::pin(err(UuidResolverError::NameAlreadyExist)));

    //let update_handle = MockUpdateActorHandle::new();

    //let snapshot_path = tempfile::tempdir_in(".").unwrap();
    //let snapshot_service = SnapshotService::new(
    //uuid_resolver,
    //update_handle,
    //Duration::from_millis(100),
    //snapshot_path.path().to_owned(),
    //"data.ms".to_string(),
    //);

    //let _ = timeout(Duration::from_millis(300), snapshot_service.run()).await;
    //}
}

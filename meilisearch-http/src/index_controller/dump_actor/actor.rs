use super::{DumpError, DumpInfo, DumpMsg, DumpResult, DumpStatus};
use crate::helpers::compression;
use crate::index_controller::{update_actor, uuid_resolver};
use async_stream::stream;
use chrono::Utc;
use futures::stream::StreamExt;
use log::{error, info};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs::create_dir_all, sync::{mpsc, oneshot, RwLock}};

pub const CONCURRENT_DUMP_MSG: usize = 10;

pub struct DumpActor<UuidResolver, Update> {
    inbox: Option<mpsc::Receiver<DumpMsg>>,
    uuid_resolver: UuidResolver,
    update: Update,
    dump_path: PathBuf,
    dump_info: Arc<RwLock<Option<DumpInfo>>>,
    _update_db_size: u64,
    _index_db_size: u64,
}

/// Generate uid from creation date
fn generate_uid() -> String {
    Utc::now().format("%Y%m%d-%H%M%S%3f").to_string()
}

impl<UuidResolver, Update> DumpActor<UuidResolver, Update>
where
    UuidResolver: uuid_resolver::UuidResolverHandle + Send + Sync + Clone + 'static,
    Update: update_actor::UpdateActorHandle + Send + Sync + Clone + 'static,
{
    pub fn new(
        inbox: mpsc::Receiver<DumpMsg>,
        uuid_resolver: UuidResolver,
        update: Update,
        dump_path: impl AsRef<Path>,
        _index_db_size: u64,
        _update_db_size: u64,
    ) -> Self {
        Self {
            inbox: Some(inbox),
            uuid_resolver,
            update,
            dump_path: dump_path.as_ref().into(),
            dump_info: Arc::new(RwLock::new(None)),
            _index_db_size,
            _update_db_size,
        }
    }

    pub async fn run(mut self) {
        info!("Started dump actor.");

        let mut inbox = self
            .inbox
            .take()
            .expect("Dump Actor must have a inbox at this point.");

        let stream = stream! {
            loop {
                match inbox.recv().await {
                    Some(msg) => yield msg,
                    None => break,
                }
            }
        };

        stream
            .for_each_concurrent(Some(CONCURRENT_DUMP_MSG), |msg| self.handle_message(msg))
            .await;

        error!("Dump actor stopped.");
    }

    async fn handle_message(&self, msg: DumpMsg) {
        use DumpMsg::*;

        match msg {
            CreateDump { ret } => {
                let _ = self.handle_create_dump(ret).await;
            }
            DumpInfo { ret, uid } => {
                let _ = ret.send(self.handle_dump_info(uid).await);
            }
        }
    }

    async fn handle_create_dump(&self, ret: oneshot::Sender<DumpResult<DumpInfo>>) {
        if self.is_running().await {
            ret.send(Err(DumpError::DumpAlreadyRunning))
                .expect("Dump actor is dead");
            return;
        }
        let uid = generate_uid();
        let info = DumpInfo::new(uid.clone(), DumpStatus::InProgress);
        *self.dump_info.write().await = Some(info.clone());

        ret.send(Ok(info)).expect("Dump actor is dead");

        let dump_info = self.dump_info.clone();

        let task_result = tokio::task::spawn(perform_dump(
            self.dump_path.clone(),
            self.uuid_resolver.clone(),
            self.update.clone(),
            uid.clone(),
        ))
        .await;

        match task_result {
            Ok(Ok(())) => {
                (*dump_info.write().await).as_mut().expect("Inconsistent dump service state").done();
                info!("Dump succeed");
            }
            Ok(Err(e)) => {
                (*dump_info.write().await).as_mut().expect("Inconsistent dump service state").with_error(e.to_string());
                error!("Dump failed: {}", e);
            }
            Err(_) => {
                error!("Dump panicked. Dump status set to failed");
                *dump_info.write().await = Some(DumpInfo::new(uid, DumpStatus::Failed));
            }
        };
    }

    async fn handle_dump_info(&self, uid: String) -> DumpResult<DumpInfo> {
        match &*self.dump_info.read().await {
            None => self.dump_from_fs(uid).await,
            Some(DumpInfo { uid: ref s, .. }) if &uid != s => self.dump_from_fs(uid).await,
            Some(info) => Ok(info.clone()),
        }
    }

    async fn dump_from_fs(&self, uid: String) -> DumpResult<DumpInfo> {
        self.dump_path
            .join(format!("{}.dump", &uid))
            .exists()
            .then(|| DumpInfo::new(uid.clone(), DumpStatus::Done))
            .ok_or(DumpError::DumpDoesNotExist(uid))
    }

    async fn is_running(&self) -> bool {
        matches!(
            *self.dump_info.read().await,
            Some(DumpInfo {
                status: DumpStatus::InProgress,
                ..
            })
        )
    }
}

async fn perform_dump<UuidResolver, Update>(
    path: PathBuf,
    uuid_resolver: UuidResolver,
    update_handle: Update,
    uid: String,
) -> anyhow::Result<()>
where
    UuidResolver: uuid_resolver::UuidResolverHandle + Send + Sync + Clone + 'static,
    Update: update_actor::UpdateActorHandle + Send + Sync + Clone + 'static,
{
    info!("Performing dump.");

    create_dir_all(&path).await?;

    let path_clone = path.clone();
    let temp_dump_dir = tokio::task::spawn_blocking(|| tempfile::TempDir::new_in(path_clone)).await??;
    let temp_dump_path = temp_dump_dir.path().to_owned();

    let uuids = uuid_resolver.dump(temp_dump_path.clone()).await?;

    update_handle.dump(uuids, temp_dump_path.clone()).await?;

    let dump_path = tokio::task::spawn_blocking(move || -> anyhow::Result<PathBuf> {
        let temp_dump_file = tempfile::NamedTempFile::new_in(&path)?;
        compression::to_tar_gz(temp_dump_path, temp_dump_file.path())?;

        let dump_path = path.join(format!("{}.dump", uid));
        temp_dump_file.persist(&dump_path)?;

        Ok(dump_path)
    })
    .await??;

    info!("Created dump in {:?}.", dump_path);

    Ok(())
}

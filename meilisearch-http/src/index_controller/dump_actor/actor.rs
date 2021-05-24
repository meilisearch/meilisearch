use super::{DumpError, DumpInfo, DumpMsg, DumpResult, DumpStatus};
use crate::helpers::compression;
use crate::index_controller::{index_actor, update_actor, uuid_resolver, IndexMetadata};
use async_stream::stream;
use chrono::Utc;
use futures::stream::StreamExt;
use log::{error, info, warn};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

pub const CONCURRENT_DUMP_MSG: usize = 10;

pub struct DumpActor<UuidResolver, Index, Update> {
    inbox: Option<mpsc::Receiver<DumpMsg>>,
    uuid_resolver: UuidResolver,
    index: Index,
    update: Update,
    dump_path: PathBuf,
    dump_info: Arc<Mutex<Option<DumpInfo>>>,
}

/// Generate uid from creation date
fn generate_uid() -> String {
    Utc::now().format("%Y%m%d-%H%M%S%3f").to_string()
}

impl<UuidResolver, Index, Update> DumpActor<UuidResolver, Index, Update>
where
    UuidResolver: uuid_resolver::UuidResolverHandle + Send + Sync + Clone + 'static,
    Index: index_actor::IndexActorHandle + Send + Sync + Clone + 'static,
    Update: update_actor::UpdateActorHandle + Send + Sync + Clone + 'static,
{
    pub fn new(
        inbox: mpsc::Receiver<DumpMsg>,
        uuid_resolver: UuidResolver,
        index: Index,
        update: Update,
        dump_path: impl AsRef<Path>,
    ) -> Self {
        Self {
            inbox: Some(inbox),
            uuid_resolver,
            index,
            update,
            dump_path: dump_path.as_ref().into(),
            dump_info: Arc::new(Mutex::new(None)),
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
        *self.dump_info.lock().await = Some(info.clone());

        ret.send(Ok(info)).expect("Dump actor is dead");

        let dump_info = self.dump_info.clone();

        let task_result = tokio::task::spawn(perform_dump(
            self.dump_path.clone(),
            self.uuid_resolver.clone(),
            self.index.clone(),
            self.update.clone(),
            uid.clone(),
        ))
        .await;

        match task_result {
            Ok(Ok(())) => {
                if let Some(ref mut info) = *dump_info.lock().await {
                    info.done();
                } else {
                    warn!("dump actor was in an inconsistant state");
                }
                info!("Dump succeed");
            }
            Ok(Err(e)) => {
                if let Some(ref mut info) = *dump_info.lock().await {
                    info.with_error(e.to_string());
                } else {
                    warn!("dump actor was in an inconsistant state");
                }
                error!("Dump failed: {}", e);
            }
            Err(_) => {
                error!("Dump panicked. Dump status set to failed");
                *dump_info.lock().await = Some(DumpInfo::new(uid, DumpStatus::Failed));
            }
        };
    }

    async fn handle_dump_info(&self, uid: String) -> DumpResult<DumpInfo> {
        match &*self.dump_info.lock().await {
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
            *self.dump_info.lock().await,
            Some(DumpInfo {
                status: DumpStatus::InProgress,
                ..
            })
        )
    }
}

async fn perform_dump<UuidResolver, Index, Update>(
    dump_path: PathBuf,
    uuid_resolver: UuidResolver,
    index: Index,
    update: Update,
    uid: String,
) -> anyhow::Result<()>
where
    UuidResolver: uuid_resolver::UuidResolverHandle + Send + Sync + Clone + 'static,
    Index: index_actor::IndexActorHandle + Send + Sync + Clone + 'static,
    Update: update_actor::UpdateActorHandle + Send + Sync + Clone + 'static,
{
    info!("Performing dump.");

    let dump_dir = dump_path.clone();
    tokio::fs::create_dir_all(&dump_dir).await?;
    let temp_dump_dir =
        tokio::task::spawn_blocking(move || tempfile::tempdir_in(dump_dir)).await??;
    let temp_dump_path = temp_dump_dir.path().to_owned();

    let uuids = uuid_resolver.list().await?;
    // maybe we could just keep the vec as-is
    let uuids: HashSet<(String, Uuid)> = uuids.into_iter().collect();

    if uuids.is_empty() {
        return Ok(());
    }

    let indexes = list_indexes(&uuid_resolver, &index).await?;

    // we create one directory by index
    for meta in indexes.iter() {
        tokio::fs::create_dir(temp_dump_path.join(&meta.uid)).await?;
    }

    let metadata = super::Metadata::new(indexes, env!("CARGO_PKG_VERSION").to_string());
    metadata.to_path(&temp_dump_path).await?;

    update.dump(uuids, temp_dump_path.clone()).await?;

    let dump_dir = dump_path.clone();
    let dump_path = dump_path.join(format!("{}.dump", uid));
    let dump_path = tokio::task::spawn_blocking(move || -> anyhow::Result<PathBuf> {
        let temp_dump_file = tempfile::NamedTempFile::new_in(dump_dir)?;
        let temp_dump_file_path = temp_dump_file.path().to_owned();
        compression::to_tar_gz(temp_dump_path, temp_dump_file_path)?;
        temp_dump_file.persist(&dump_path)?;
        Ok(dump_path)
    })
    .await??;

    info!("Created dump in {:?}.", dump_path);

    Ok(())
}

async fn list_indexes<UuidResolver, Index>(
    uuid_resolver: &UuidResolver,
    index: &Index,
) -> anyhow::Result<Vec<IndexMetadata>>
where
    UuidResolver: uuid_resolver::UuidResolverHandle,
    Index: index_actor::IndexActorHandle,
{
    let uuids = uuid_resolver.list().await?;

    let mut ret = Vec::new();

    for (uid, uuid) in uuids {
        let meta = index.get_index_meta(uuid).await?;
        let meta = IndexMetadata {
            uuid,
            name: uid.clone(),
            uid,
            meta,
        };
        ret.push(meta);
    }

    Ok(ret)
}

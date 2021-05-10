use super::{DumpError, DumpInfo, DumpMsg, DumpResult, DumpStatus};
use crate::helpers::compression;
use crate::index_controller::{index_actor, update_actor, uuid_resolver, IndexMetadata};
use chrono::Utc;
use log::{error, info, warn};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

pub struct DumpActor<UuidResolver, Index, Update> {
    inbox: mpsc::Receiver<DumpMsg>,
    inner: InnerDump<UuidResolver, Index, Update>,
}

#[derive(Clone)]
struct InnerDump<UuidResolver, Index, Update> {
    pub uuid_resolver: UuidResolver,
    pub index: Index,
    pub update: Update,
    pub dump_path: PathBuf,
    pub dump_info: Arc<Mutex<Option<DumpInfo>>>,
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
            inbox,
            inner: InnerDump {
                uuid_resolver,
                index,
                update,
                dump_path: dump_path.as_ref().into(),
                dump_info: Arc::new(Mutex::new(None)),
            },
        }
    }

    pub async fn run(mut self) {
        use DumpMsg::*;

        info!("Started dump actor.");

        loop {
            match self.inbox.recv().await {
                Some(CreateDump { ret }) => {
                    let _ = ret.send(self.inner.clone().handle_create_dump().await);
                }
                Some(DumpInfo { ret, uid }) => {
                    let _ = ret.send(self.inner.handle_dump_info(uid).await);
                }
                None => break,
            }
        }

        error!("Dump actor stopped.");
    }
}

impl<UuidResolver, Index, Update> InnerDump<UuidResolver, Index, Update>
where
    UuidResolver: uuid_resolver::UuidResolverHandle + Send + Sync + Clone + 'static,
    Index: index_actor::IndexActorHandle + Send + Sync + Clone + 'static,
    Update: update_actor::UpdateActorHandle + Send + Sync + Clone + 'static,
{
    async fn handle_create_dump(self) -> DumpResult<DumpInfo> {
        if self.is_running().await {
            return Err(DumpError::DumpAlreadyRunning);
        }
        let uid = generate_uid();
        let info = DumpInfo::new(uid.clone(), DumpStatus::InProgress);
        *self.dump_info.lock().await = Some(info.clone());

        let this = self.clone();

        tokio::task::spawn(async move {
            match this.perform_dump(uid).await {
                Ok(()) => {
                    if let Some(ref mut info) = *self.dump_info.lock().await {
                        info.done();
                    } else {
                        warn!("dump actor was in an inconsistant state");
                    }
                    info!("Dump succeed");
                }
                Err(e) => {
                    if let Some(ref mut info) = *self.dump_info.lock().await {
                        info.with_error(e.to_string());
                    } else {
                        warn!("dump actor was in an inconsistant state");
                    }
                    error!("Dump failed: {}", e);
                }
            };
        });

        Ok(info)
    }

    async fn perform_dump(self, uid: String) -> anyhow::Result<()> {
        info!("Performing dump.");

        let dump_dir = self.dump_path.clone();
        tokio::fs::create_dir_all(&dump_dir).await?;
        let temp_dump_dir =
            tokio::task::spawn_blocking(move || tempfile::tempdir_in(dump_dir)).await??;
        let temp_dump_path = temp_dump_dir.path().to_owned();

        let uuids = self.uuid_resolver.list().await?;
        // maybe we could just keep the vec as-is
        let uuids: HashSet<(String, Uuid)> = uuids.into_iter().collect();

        if uuids.is_empty() {
            return Ok(());
        }

        let indexes = self.list_indexes().await?;

        // we create one directory by index
        for meta in indexes.iter() {
            tokio::fs::create_dir(temp_dump_path.join(&meta.uid)).await?;
        }

        let metadata = super::Metadata::new(indexes, env!("CARGO_PKG_VERSION").to_string());
        metadata.to_path(&temp_dump_path).await?;

        self.update.dump(uuids, temp_dump_path.clone()).await?;

        let dump_dir = self.dump_path.clone();
        let dump_path = self.dump_path.join(format!("{}.dump", uid));
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

    async fn list_indexes(&self) -> anyhow::Result<Vec<IndexMetadata>> {
        let uuids = self.uuid_resolver.list().await?;

        let mut ret = Vec::new();

        for (uid, uuid) in uuids {
            let meta = self.index.get_index_meta(uuid).await?;
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

    async fn handle_dump_info(&self, uid: String) -> DumpResult<DumpInfo> {
        match &*self.dump_info.lock().await {
            None => Err(DumpError::DumpDoesNotExist(uid)),
            Some(DumpInfo { uid: ref s, .. }) if &uid != s => Err(DumpError::DumpDoesNotExist(uid)),
            Some(info) => Ok(info.clone()),
        }
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

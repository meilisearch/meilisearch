use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_stream::stream;
use chrono::Utc;
use futures::stream::StreamExt;
use log::{error, info};
use update_actor::UpdateActorHandle;
use uuid_resolver::UuidResolverHandle;
use tokio::sync::{mpsc, oneshot, RwLock};

use super::{DumpError, DumpInfo, DumpMsg, DumpResult, DumpStatus, DumpTask};
use crate::index_controller::{update_actor, uuid_resolver};

pub const CONCURRENT_DUMP_MSG: usize = 10;

pub struct DumpActor<UuidResolver, Update> {
    inbox: Option<mpsc::Receiver<DumpMsg>>,
    uuid_resolver: UuidResolver,
    update: Update,
    dump_path: PathBuf,
    dump_info: Arc<RwLock<Option<DumpInfo>>>,
    update_db_size: u64,
    index_db_size: u64,
}

/// Generate uid from creation date
fn generate_uid() -> String {
    Utc::now().format("%Y%m%d-%H%M%S%3f").to_string()
}

impl<UuidResolver, Update> DumpActor<UuidResolver, Update>
where
    UuidResolver: UuidResolverHandle + Send + Sync + Clone + 'static,
    Update: UpdateActorHandle + Send + Sync + Clone + 'static,
{
    pub fn new(
        inbox: mpsc::Receiver<DumpMsg>,
        uuid_resolver: UuidResolver,
        update: Update,
        dump_path: impl AsRef<Path>,
        index_db_size: u64,
        update_db_size: u64,
    ) -> Self {
        Self {
            inbox: Some(inbox),
            uuid_resolver,
            update,
            dump_path: dump_path.as_ref().into(),
            dump_info: Arc::new(RwLock::new(None)),
            index_db_size,
            update_db_size,
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

        let task = DumpTask {
            path: self.dump_path.clone(),
            uuid_resolver: self.uuid_resolver.clone(),
            update_handle: self.update.clone(),
            uid: uid.clone(),
            update_db_size: self.update_db_size,
            index_db_size: self.index_db_size,
        };

        let task_result = tokio::task::spawn(task.run()).await;

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
                (*dump_info.write().await).as_mut().expect("Inconsistent dump service state").with_error("Unexpected error while performing dump.".to_string());
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

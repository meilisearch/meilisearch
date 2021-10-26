use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_stream::stream;
use chrono::Utc;
use futures::{lock::Mutex, stream::StreamExt};
use log::{error, trace};
use tokio::sync::{mpsc, oneshot, RwLock};

use super::error::{DumpActorError, Result};
use super::{DumpInfo, DumpMsg, DumpStatus, DumpTask};
use crate::index_controller::index_resolver::index_store::IndexStore;
use crate::index_controller::index_resolver::uuid_store::UuidStore;
use crate::index_controller::index_resolver::IndexResolver;
use crate::index_controller::updates::UpdateSender;

pub const CONCURRENT_DUMP_MSG: usize = 10;

pub struct DumpActor<U, I> {
    inbox: Option<mpsc::Receiver<DumpMsg>>,
    index_resolver: Arc<IndexResolver<U, I>>,
    update: UpdateSender,
    dump_path: PathBuf,
    analytics_path: PathBuf,
    lock: Arc<Mutex<()>>,
    dump_infos: Arc<RwLock<HashMap<String, DumpInfo>>>,
    update_db_size: usize,
    index_db_size: usize,
}

/// Generate uid from creation date
fn generate_uid() -> String {
    Utc::now().format("%Y%m%d-%H%M%S%3f").to_string()
}

impl<U, I> DumpActor<U, I>
where
    U: UuidStore + Sync + Send + 'static,
    I: IndexStore + Sync + Send + 'static,
{
    pub fn new(
        inbox: mpsc::Receiver<DumpMsg>,
        index_resolver: Arc<IndexResolver<U, I>>,
        update: UpdateSender,
        dump_path: impl AsRef<Path>,
        analytics_path: impl AsRef<Path>,
        index_db_size: usize,
        update_db_size: usize,
    ) -> Self {
        let dump_infos = Arc::new(RwLock::new(HashMap::new()));
        let lock = Arc::new(Mutex::new(()));
        Self {
            inbox: Some(inbox),
            index_resolver,
            update,
            dump_path: dump_path.as_ref().into(),
            analytics_path: analytics_path.as_ref().into(),
            dump_infos,
            lock,
            index_db_size,
            update_db_size,
        }
    }

    pub async fn run(mut self) {
        trace!("Started dump actor.");

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

    async fn handle_create_dump(&self, ret: oneshot::Sender<Result<DumpInfo>>) {
        let uid = generate_uid();
        let info = DumpInfo::new(uid.clone(), DumpStatus::InProgress);

        let _lock = match self.lock.try_lock() {
            Some(lock) => lock,
            None => {
                ret.send(Err(DumpActorError::DumpAlreadyRunning))
                    .expect("Dump actor is dead");
                return;
            }
        };

        self.dump_infos
            .write()
            .await
            .insert(uid.clone(), info.clone());

        ret.send(Ok(info)).expect("Dump actor is dead");

        let task = DumpTask {
            dump_path: self.dump_path.clone(),
            db_path: self.analytics_path.clone(),
            index_resolver: self.index_resolver.clone(),
            update_sender: self.update.clone(),
            uid: uid.clone(),
            update_db_size: self.update_db_size,
            index_db_size: self.index_db_size,
        };

        let task_result = tokio::task::spawn(task.run()).await;

        let mut dump_infos = self.dump_infos.write().await;
        let dump_infos = dump_infos
            .get_mut(&uid)
            .expect("dump entry deleted while lock was acquired");

        match task_result {
            Ok(Ok(())) => {
                dump_infos.done();
                trace!("Dump succeed");
            }
            Ok(Err(e)) => {
                dump_infos.with_error(e.to_string());
                error!("Dump failed: {}", e);
            }
            Err(_) => {
                dump_infos.with_error("Unexpected error while performing dump.".to_string());
                error!("Dump panicked. Dump status set to failed");
            }
        };
    }

    async fn handle_dump_info(&self, uid: String) -> Result<DumpInfo> {
        match self.dump_infos.read().await.get(&uid) {
            Some(info) => Ok(info.clone()),
            _ => Err(DumpActorError::DumpDoesNotExist(uid)),
        }
    }
}

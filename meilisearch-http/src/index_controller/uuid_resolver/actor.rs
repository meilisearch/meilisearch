use std::{collections::HashSet, path::PathBuf};

use log::{info, warn};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{Result, UuidResolveMsg, UuidResolverError, UuidStore};

pub struct UuidResolverActor<S> {
    inbox: mpsc::Receiver<UuidResolveMsg>,
    store: S,
}

impl<S: UuidStore> UuidResolverActor<S> {
    pub fn new(inbox: mpsc::Receiver<UuidResolveMsg>, store: S) -> Self {
        Self { inbox, store }
    }

    pub async fn run(mut self) {
        use UuidResolveMsg::*;

        info!("uuid resolver started");

        loop {
            match self.inbox.recv().await {
                Some(Get { uid: name, ret }) => {
                    let _ = ret.send(self.handle_get(name).await);
                }
                Some(Delete { uid: name, ret }) => {
                    let _ = ret.send(self.handle_delete(name).await);
                }
                Some(List { ret }) => {
                    let _ = ret.send(self.handle_list().await);
                }
                Some(Insert { ret, uuid, name }) => {
                    let _ = ret.send(self.handle_insert(name, uuid).await);
                }
                Some(SnapshotRequest { path, ret }) => {
                    let _ = ret.send(self.handle_snapshot(path).await);
                }
                Some(GetSize { ret }) => {
                    let _ = ret.send(self.handle_get_size().await);
                }
                Some(DumpRequest { path, ret }) => {
                    let _ = ret.send(self.handle_dump(path).await);
                }
                // all senders have been dropped, need to quit.
                None => break,
            }
        }

        warn!("exiting uuid resolver loop");
    }

    async fn handle_get(&self, uid: String) -> Result<Uuid> {
        self.store
            .get_uuid(uid.clone())
            .await?
            .ok_or(UuidResolverError::UnexistingIndex(uid))
    }

    async fn handle_delete(&self, uid: String) -> Result<Uuid> {
        self.store
            .delete(uid.clone())
            .await?
            .ok_or(UuidResolverError::UnexistingIndex(uid))
    }

    async fn handle_list(&self) -> Result<Vec<(String, Uuid)>> {
        let result = self.store.list().await?;
        Ok(result)
    }

    async fn handle_snapshot(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        self.store.snapshot(path).await
    }

    async fn handle_dump(&self, path: PathBuf) -> Result<HashSet<Uuid>> {
        self.store.dump(path).await
    }

    async fn handle_insert(&self, uid: String, uuid: Uuid) -> Result<()> {
        if !is_index_uid_valid(&uid) {
            return Err(UuidResolverError::BadlyFormatted(uid));
        }
        self.store.insert(uid, uuid).await?;
        Ok(())
    }

    async fn handle_get_size(&self) -> Result<u64> {
        self.store.get_size().await
    }
}

fn is_index_uid_valid(uid: &str) -> bool {
    uid.chars()
        .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
}

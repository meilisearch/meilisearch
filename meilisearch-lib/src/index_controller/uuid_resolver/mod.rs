pub mod error;
mod message;
pub mod store;

use std::path::Path;
use std::{collections::HashSet, path::PathBuf};

use log::{trace, warn};
use tokio::sync::mpsc;
use uuid::Uuid;

pub use self::error::UuidResolverError;
pub use self::message::UuidResolverMsg;
pub use self::store::{HeedUuidStore, UuidStore};
use self::error::Result;

pub type UuidResolverSender = mpsc::Sender<UuidResolverMsg>;

const UUID_STORE_SIZE: usize = 1_073_741_824; //1GiB

pub fn create_uuid_resolver(path: impl AsRef<Path>) -> Result<mpsc::Sender<UuidResolverMsg>> {
    let (sender, reveiver) = mpsc::channel(100);
    let store = HeedUuidStore::new(path)?;
    let actor = UuidResolver::new(reveiver, store);
    tokio::spawn(actor.run());
    Ok(sender)
}

pub struct UuidResolver<S> {
    inbox: mpsc::Receiver<UuidResolverMsg>,
    store: S,
}

impl<S: UuidStore> UuidResolver<S> {
    pub fn new(inbox: mpsc::Receiver<UuidResolverMsg>, store: S) -> Self {
        Self { inbox, store }
    }

    pub async fn run(mut self) {
        use UuidResolverMsg::*;

        trace!("uuid resolver started");

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

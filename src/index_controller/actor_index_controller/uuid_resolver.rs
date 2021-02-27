use thiserror::Error;
use tokio::sync::{RwLock, mpsc, oneshot};
use uuid::Uuid;
use std::collections::HashMap;
use std::sync::Arc;
use std::collections::hash_map::Entry;
use log::{info, warn};

pub type Result<T> = std::result::Result<T, UuidError>;

#[derive(Debug)]
enum UuidResolveMsg {
    Resolve {
        name: String,
        ret: oneshot::Sender<Result<Option<Uuid>>>,
    },
    GetOrCreate {
        name: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
    Create {
        name: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
}

struct UuidResolverActor<S> {
    inbox: mpsc::Receiver<UuidResolveMsg>,
    store: S,
}

impl<S: UuidStore> UuidResolverActor<S> {
    fn new(inbox: mpsc::Receiver<UuidResolveMsg>, store: S) -> Self {
        Self { inbox, store }
    }

    async fn run(mut self) {
        use UuidResolveMsg::*;

        info!("uuid resolver started");

        // TODO: benchmark and use buffered streams to improve throughput.
        loop {
            match self.inbox.recv().await {
                Some(Create { name, ret }) => self.handle_create(name, ret).await,
                Some(GetOrCreate { name, ret }) => self.handle_get_or_create(name, ret).await,
                Some(Resolve { name, ret }) => self.handle_resolve(name, ret).await,
                // all senders have been dropped, need to quit.
                None => break,
            }
        }

        warn!("exiting uuid resolver loop");
    }

    async fn handle_create(&self, name: String, ret: oneshot::Sender<Result<Uuid>>) {
        let result = self.store.create_uuid(name, true).await;
        let _ = ret.send(result);
    }

    async fn handle_get_or_create(&self, name: String, ret: oneshot::Sender<Result<Uuid>>) {
        let result = self.store.create_uuid(name, false).await;
        let _ = ret.send(result);
    }

    async fn handle_resolve(&self, name: String, ret: oneshot::Sender<Result<Option<Uuid>>>) {
        let result = self.store.get_uuid(name).await;
        let _ = ret.send(result);
    }
}

#[derive(Clone)]
pub struct UuidResolverHandle {
    sender: mpsc::Sender<UuidResolveMsg>,
}

impl UuidResolverHandle {
    pub fn new() -> Self {
        let (sender, reveiver) = mpsc::channel(100);
        let store = MapUuidStore(Arc::new(RwLock::new(HashMap::new())));
        let actor = UuidResolverActor::new(reveiver, store);
        tokio::spawn(actor.run());
        Self { sender }
    }

    pub async fn resolve(&self, name: String) -> anyhow::Result<Option<Uuid>> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::Resolve { name, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("Uuid resolver actor has been killed")?)
    }

    pub async fn get_or_create(&self, name: String) -> Result<Uuid> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::GetOrCreate { name, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("Uuid resolver actor has been killed")?)
    }

    pub async fn create(&self, name: String) -> anyhow::Result<Uuid> {
        let (ret, receiver) = oneshot::channel();
        let msg = UuidResolveMsg::Create { name, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("Uuid resolver actor has been killed")?)
    }
}

#[derive(Clone, Debug, Error)]
pub enum UuidError {
    #[error("Name already exist.")]
    NameAlreadyExist,
}

#[async_trait::async_trait]
trait UuidStore {
    // Create a new entry for `name`. Return an error if `err` and the entry already exists, return
    // the uuid otherwise.
    async fn create_uuid(&self, name: String, err: bool) -> Result<Uuid>;
    async fn get_uuid(&self, name: String) -> Result<Option<Uuid>>;
}

struct MapUuidStore(Arc<RwLock<HashMap<String, Uuid>>>);

#[async_trait::async_trait]
impl UuidStore for MapUuidStore {
    async fn create_uuid(&self, name: String, err: bool) -> Result<Uuid> {
        match self.0.write().await.entry(name) {
            Entry::Occupied(entry) => {
                if err {
                    Err(UuidError::NameAlreadyExist)
                } else {
                    Ok(entry.get().clone())
                }
            },
            Entry::Vacant(entry) => {
                let uuid = Uuid::new_v4();
                let uuid = entry.insert(uuid);
                Ok(uuid.clone())
            }
        }
    }

    async fn get_uuid(&self, name: String) -> Result<Option<Uuid>> {
        Ok(self.0.read().await.get(&name).cloned())
    }
}

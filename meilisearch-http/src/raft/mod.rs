mod router;
mod server;
mod snapshot;
mod store;

pub mod raft_service {
    tonic::include_proto!("raftservice");
}

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::Data;
use anyhow::Result;
use async_raft::config::Config;
use async_raft::error::InitializeError;
use async_raft::raft::ClientWriteRequest;
use async_raft::{AppData, AppDataResponse, NodeId};
use meilisearch_core::settings::SettingsUpdate;
use raft_service::raft_service_server::RaftServiceServer;
use router::RaftRouter;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use server::RaftServerService;
use store::RaftStore;
use tokio::time;
use tonic::transport::Server;

use crate::data::{IndexCreateRequest, IndexResponse, UpdateDocumentsQuery};

type InnerRaft = async_raft::Raft<ClientRequest, ClientResponse, RaftRouter, RaftStore>;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RaftConfig {
    id: NodeId,
    addr: SocketAddr,
    peers: Vec<(NodeId, SocketAddr)>,
    shared_folder: PathBuf,
    snapshot_dir: PathBuf,
    log_db_path: PathBuf,
    cluster_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Message {
    CreateIndex(IndexCreateRequest),
    UpdateIndex {
        index_uid: String,
        update: IndexCreateRequest,
    },
    DeleteIndex(String),
    SettingsUpdate {
        index_uid: String,
        update: SettingsUpdate,
    },
    DocumentsDeletion {
        index_uid: String,
        ids: Vec<Value>,
    },
    ClearAllDocuments {
        index_uid: String,
    },
    DocumentAddition {
        update_query: UpdateDocumentsQuery,
        index_uid: String,
        filename: String,
        partial: bool,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The serial number of this request.
    pub serial: u64,
    /// A string describing the status of the client. For a real application, this should probably
    /// be an enum representing all of the various types of requests / operations which a client
    /// can perform.
    pub message: Message,
}
///
/// The application data response type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientResponse {
    IndexCreation(std::result::Result<IndexResponse, String>),
}

impl AppDataResponse for ClientResponse {}

impl AppData for ClientRequest {}

#[allow(dead_code)]
pub struct Raft {
    pub inner: Arc<InnerRaft>,
    pub store: Data,
    id: NodeId,
    server_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    next_id: AtomicU64,
    pub shared_folder: PathBuf,
}

impl Raft {
    pub fn id(&self) -> NodeId {
        self.id
    }

    pub async fn propose(&self, message: Message) -> Result<ClientResponse> {
        let serial = self.next_id.fetch_add(1, Ordering::Relaxed);
        let client_request = ClientRequest { serial, message };
        let request = ClientWriteRequest::new(client_request);
        let response = self.inner.client_write(request).await?;
        Ok(response.data)
    }
}

pub async fn init_raft(raft_config: RaftConfig, store: Data) -> Result<Raft> {
    let config = Arc::new(Config::build(raft_config.cluster_name).validate()?);
    let router = Arc::new(RaftRouter::new());
    let storage = Arc::new(RaftStore::new(
        raft_config.id,
        raft_config.log_db_path,
        store.clone(),
        raft_config.snapshot_dir,
    )?);
    let inner = Arc::new(InnerRaft::new(
        raft_config.id,
        config,
        router.clone(),
        storage.clone(),
    ));
    let svc = RaftServerService::new(inner.clone(), router.clone(), storage);
    let server_handle = tokio::spawn(
        Server::builder()
            .add_service(RaftServiceServer::new(svc))
            .serve(raft_config.addr),
    );
    let next_id = AtomicU64::new(0);
    for (id, addr) in &raft_config.peers {
        router.add_client(*id, addr.to_string()).await;
    }

    // TODO: we want to wait until all the peers have been discovered and connected to before
    // running the raft, we could for example timeout for connection on return only the peers that
    // we could connect to.
    time::delay_for(Duration::from_millis(10_000)).await;
    let members = raft_config.peers.iter().map(|(id, _)| *id).collect();
    match inner.initialize(members).await {
        Ok(()) | Err(InitializeError::NotAllowed) => (),
        Err(e) => return Err(anyhow::Error::new(e)),
    }

    Ok(Raft {
        inner,
        id: raft_config.id,
        server_handle,
        next_id,
        shared_folder: raft_config.shared_folder,
        store,
    })
}

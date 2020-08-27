mod router;
mod server;
mod snapshot;
mod store;

pub mod raft_service {
    tonic::include_proto!("raftservice");
}

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::Data;
use anyhow::Result;
use async_raft::config::Config;
use async_raft::NodeId;
use async_raft::{AppData, AppDataResponse};
use meilisearch_core::settings::Settings;
use raft_service::raft_service_server::RaftServiceServer;
use router::RaftRouter;
use serde::{Deserialize, Serialize};
use server::RaftServerService;
use store::RaftStore;
use tonic::transport::Server;

use crate::data::{IndexCreateRequest, IndexResponse};

type Raft = async_raft::Raft<ClientRequest, ClientResponse, RaftRouter, RaftStore>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Message {
    CreateIndex(IndexCreateRequest),
    SettingChange(Settings),
    DocumentAddition {
        index_uid: String,
        addition: PathBuf,
        partial: bool,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The ID of the client which has sent the request.
    pub client: String,
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

pub fn run_raft(
    id: NodeId,
    config: Arc<Config>,
    db_path: PathBuf,
    store: Arc<Data>,
    snapshot_dir: PathBuf,
    raft_addr: SocketAddr,
) -> Result<(
    Arc<Raft>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let network = Arc::new(RaftRouter::new());
    let storage = Arc::new(RaftStore::new(id, db_path, store, snapshot_dir)?);
    let raft = Raft::new(id, config, network, storage);
    let raft = Arc::new(raft);
    let svc = RaftServerService::new(raft.clone());
    let handle = tokio::spawn(
        Server::builder()
            .add_service(RaftServiceServer::new(svc))
            .serve(raft_addr),
    );
    Ok((raft, handle))
}

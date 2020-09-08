mod mdns;
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
//use async_raft::error::ClientWriteError;
use async_raft::metrics::RaftMetrics;
use async_raft::raft::ClientWriteRequest;
use async_raft::{AppData, AppDataResponse, InitializeError, NodeId};
use log::{info, warn};
use meilisearch_core::settings::SettingsUpdate;
use raft_service::raft_service_server::RaftServiceServer;
use router::RaftRouter;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use server::RaftServerService;
use store::RaftStore;
use tonic::transport::Server;

use self::mdns::MDNSServer;
use crate::data::{IndexCreateRequest, IndexResponse, UpdateDocumentsQuery};
use crate::routes::IndexUpdateResponse;

type InnerRaft = async_raft::Raft<ClientRequest, ClientResponse, RaftRouter, RaftStore>;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RaftConfig {
    addr: SocketAddr,
    shared_folder: PathBuf,
    snapshot_dir: PathBuf,
    log_db_path: PathBuf,
    cluster_name: String,
    cluster_formation_timeout: u64,
    discovery_interval: u64,
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
    IndexUpdate(std::result::Result<IndexResponse, String>),
    UpdateResponse(std::result::Result<IndexUpdateResponse, String>),
    DeleteIndex(std::result::Result<(), String>),
}

impl AppDataResponse for ClientResponse {}

impl AppData for ClientRequest {}

#[allow(dead_code)]
pub struct Raft {
    pub inner: Arc<InnerRaft>,
    router: Arc<RaftRouter>,
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
        match self.inner.client_write(request).await {
            Ok(response) => Ok(response.data),
            //Err(ClientWriteError::ForwardToLeader(req, id)) => {
            //info!("Forwarding request to leader: {:?}", id);
            //let response = self
            //.router
            //.clients
            //.get_mut(&id.unwrap())
            //.ok_or_else(|| anyhow::Error::msg("Can't find leader node"))?
            //.forward(req)
            //.await?;
            //Ok(response)
            //}
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Metrics {
    pub id: u64,
    /// The state of the Raft node.
    pub state: String,
    /// The current term of the Raft node.
    pub current_term: u64,
    /// The last log index to be appended to this Raft node's log.
    pub last_log_index: u64,
    /// The last log index to be applied to this Raft node's state machine.
    pub last_applied: u64,
    /// The current cluster leader.
    pub current_leader: Option<u64>,
    /// The current membership config of the cluster.
    pub membership_config: std::collections::HashSet<u64>,
}

impl From<RaftMetrics> for Metrics {
    fn from(other: RaftMetrics) -> Self {
        Metrics {
            id: other.id,
            /// The state of the Raft node.
            state: format!("{:?}", other.state),
            /// The current term of the Raft node.
            current_term: other.current_term,
            /// The last log index to be appended to this Raft node's log.
            last_log_index: other.last_log_index,
            /// The last log index to be applied to this Raft node's state machine.
            last_applied: other.last_applied,
            /// The current cluster leader.
            current_leader: other.current_leader,
            /// The current membership config of the cluster.
            membership_config: other.membership_config.members,
        }
    }
}

/// initial cluster formation, from seed host
async fn init_raft_cluster(raft_config: RaftConfig, store: Data) -> Result<Raft> {
    // generate random id
    let id = rand::random();

    println!("started raft with id: {}", id);

    let config = Arc::new(
        Config::build(raft_config.cluster_name)
            .election_timeout_min(3000)
            .election_timeout_max(5000)
            .heartbeat_interval(500)
            .validate()?,
    );
    let router = Arc::new(RaftRouter::new());
    let storage = Arc::new(RaftStore::new(
        id,
        raft_config.log_db_path,
        store.clone(),
        raft_config.snapshot_dir,
        raft_config.shared_folder.clone(),
    )?);
    let inner = Arc::new(InnerRaft::new(id, config, router.clone(), storage.clone()));
    let svc = RaftServerService::new(inner.clone(), router.clone(), storage.clone());
    let server_handle = tokio::spawn(
        Server::builder()
            .add_service(RaftServiceServer::new(svc))
            .serve(raft_config.addr),
    );
    let next_id = AtomicU64::new(0);

    let mut mdns = MDNSServer::new(Duration::from_secs(raft_config.discovery_interval))?;
    mdns.advertise(id, raft_config.addr.port()).await?;
    let mut peers_receiver = mdns.discover();

    let mut timeout =
        tokio::time::delay_for(Duration::from_secs(raft_config.cluster_formation_timeout));
    loop {
        tokio::select! {
            node = peers_receiver.recv() => {
                match node {
                    Ok(self::mdns::Node { id, addr }) => {
                        let _ = router.add_client(id, addr).await;
                    }
                    error => warn!("error: {:?}", error),
                }
            },
            _ = &mut timeout => break,
        }
    }

    let nodes = router.clients.iter().map(|e| *e.key()).collect();

    println!("starting with nodes: {:?}", nodes);

    tokio::time::delay_for(Duration::from_secs(5)).await;

    match inner.initialize(nodes).await {
        Ok(()) | Err(InitializeError::NotAllowed) => (),
        Err(e) => return Err(anyhow::Error::new(e)),
    }

    let mut metrics = inner.metrics();
    tokio::spawn(async move {
        while let Some(info) = metrics.recv().await {
            let metrics: Metrics = info.into();
            let client = reqwest::Client::new();
            let _ = client
                .post(&format!("http://metrics:8080/put/{}", id))
                .json(&metrics)
                .send()
                .await;
        }
    });

    info!("Raft started at {}", raft_config.addr.to_string());

    Ok(Raft {
        inner,
        id,
        server_handle,
        next_id,
        router,
        shared_folder: raft_config.shared_folder,
        store,
    })
}

pub async fn init_raft(raft_config: RaftConfig, store: Data) -> Result<Raft> {
    init_raft_cluster(raft_config, store).await
}

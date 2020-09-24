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
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::Data;
use anyhow::Result;
use async_raft::config::Config;
use async_raft::error::ClientWriteError;
use async_raft::metrics::RaftMetrics;
use async_raft::raft::ClientWriteRequest;
use async_raft::{AppData, AppDataResponse, InitializeError, NodeId};
use futures_util::pin_mut;
use futures_util::StreamExt;
use log::info;
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
use crate::routes::IndexUpdateResponse;

type InnerRaft = async_raft::Raft<ClientRequest, ClientResponse, RaftRouter, RaftStore>;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RaftConfig {
    addr: SocketAddr,
    snapshot_dir: PathBuf,
    log_db_path: PathBuf,
    cluster_name: String,
    cluster_formation_timeout: u64,
    discovery_interval: u64,
    metrics_server: Option<String>,
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
        documents: String,
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
    running: bool,
    cluster_name: String,
    port: u16,
    pub inner: Arc<InnerRaft>,
    router: Arc<RaftRouter>,
    pub store: Data,
    id: NodeId,
    server_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    next_id: AtomicU64,
}

impl Raft {
    pub fn id(&self) -> NodeId {
        self.id
    }

    pub async fn propose(&self, message: Message) -> Result<ClientResponse> {
        let serial = rand::random();
        let client_request = ClientRequest { serial, message };
        let request = ClientWriteRequest::new(client_request);
        match self.inner.client_write(request).await {
            Ok(response) => Ok(response.data),
            Err(ClientWriteError::ForwardToLeader(req, Some(id))) => {
                info!("Forwarding request to leader: {:?}", id);
                let response = self
                    .router
                    .clients
                    .get(&id)
                    .ok_or_else(|| anyhow::Error::msg("Can't find leader node"))?
                    .write()
                    .await
                    .forward(req)
                    .await?;
                Ok(response.data)
            }
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }

    /// starts the raft cluster. Initally tries to form a cluster during the `cluster_formation_timeout` period.
    pub async fn start(&self, cluster_formation_timeout: Duration) -> Result<()> {
    let service_name = format!("_raft_{}._tcp.local", self.cluster_name);
    let stream = mdns::discover_peers(&service_name, self.id, self.port);
    pin_mut!(stream);
        let mut timeout = time::delay_for(cluster_formation_timeout);
        loop {
            tokio::select! {
                _ = &mut timeout, if !timeout.is_elapsed() => break,
                peer = stream.next() => {
                    if let Some((addr, id)) = peer {
                        self.router.add_client(id, addr).await?;
                    }
                }
            }
        }

        let members = self.router.members();

        match self.inner.initialize(members).await {
            Ok(()) | Err(InitializeError::NotAllowed) => (),
            Err(e) => return Err(anyhow::Error::new(e)),
        }
        Ok(())
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

    info!("started raft with id: {}", id);

    // the raft configuration used to initialize the node
    let config = Arc::new(
        Config::build(raft_config.cluster_name.clone())
            .heartbeat_interval(150)
            .election_timeout_min(1000)
            .election_timeout_max(1500)
            .validate()?,
    );

    let router = Arc::new(RaftRouter::new());
    let storage = Arc::new(RaftStore::new(
        id,
        raft_config.log_db_path,
        store.clone(),
        raft_config.snapshot_dir,
    )?);
    let inner = Arc::new(InnerRaft::new(id, config, router.clone(), storage.clone()));
    let svc = RaftServerService::new(inner.clone(), router.clone(), storage.clone());
    let server_handle = tokio::spawn(
        Server::builder()
            .add_service(RaftServiceServer::new(svc))
            .serve(raft_config.addr),
    );

    // handle dynamic membership changes
    //let inner_cloned = inner.clone();
    //let router_cloned = router.clone();
    //tokio::spawn(async move {
    //let stream = mdns::discover_peers(&service_name, id, raft_port);
    //pin_mut!(stream);
    //loop {
    //let inner = inner_cloned.clone();
    //let router = router_cloned.clone();
    //if let Some((addr, id)) = stream.next().await {
    //tokio::spawn(async move {
    //let _ = router.add_client(id, addr).await;
    //let _ = inner.add_non_voter(id).await;
    //let members = router.members();
    //let _ = inner.change_membership(members).await;
    //});
    //}
    //}
    //});

    if let Some(metrics_server) = raft_config.metrics_server {
        let mut metrics = inner.metrics();
        tokio::spawn(async move {
            while let Some(info) = metrics.recv().await {
                let metrics: Metrics = info.into();
                let client = reqwest::Client::new();
                let _ = client
                    .post(&format!("{}/put/{}", metrics_server, id))
                    .json(&metrics)
                    .send()
                    .await;
            }
        });
    }

    Ok(Raft {
        cluster_name: raft_config.cluster_name,
        port: raft_config.addr.port(),
        running: false,
        inner,
        id,
        server_handle,
        next_id: AtomicU64::new(0),
        router,
        store,
    })
}

pub async fn init_raft(raft_config: RaftConfig, store: Data) -> Result<Raft> {
    init_raft_cluster(raft_config, store).await
}

mod mdns;
mod router;
mod server;
mod snapshot;
mod store;
mod client;

pub mod raft_service {
    tonic::include_proto!("raftservice");
}

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::Data;
use anyhow::Result;
use async_raft::config::Config;
use async_raft::error::ClientWriteError;
use async_raft::metrics::RaftMetrics;
use async_raft::raft::ClientWriteRequest;
use async_raft::{AppData, AppDataResponse, InitializeError, NodeId};
use futures_util::pin_mut;
use futures_util::StreamExt;
use log::{info, warn};
use meilisearch_core::settings::SettingsUpdate;
use raft_service::raft_service_server::RaftServiceServer;
use raft_service::NodeState;
use router::RaftRouter;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use server::RaftServerService;
use store::RaftStore;
use tokio::sync::mpsc::{self, error::SendError};
use tokio::sync::RwLock;
use tokio::time;
use tonic::transport::Server;
use client::Client;

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
    cluster_name: String,
    port: u16,
    pub inner: Arc<InnerRaft>,
    router: Arc<RaftRouter>,
    log_store: Arc<RaftStore>,
    pub store: Data,
    id: NodeId,
    server_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
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
                    .client(id)
                    .await
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
        let name = self.cluster_name.clone();
        let id = self.id;
        let port = self.port;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let router = self.router.clone();

        // Peer discovery will try to send a handle to the client through the tx channel will it is
        // open. This allows the initial cluster formation to get information about the current
        // state of the cluster, and react accordingly. As soon as the tx is dropped, the discovery
        // service will continue to quietly initiate connection with new discover peers and add
        // them to the router.
        tokio::spawn(async move {
            let service_name = format!("_raft_{}._tcp.local", name);
            let stream = mdns::discover_peers(&service_name, id, port);
            pin_mut!(stream);
            loop {
                if let Some((id, addr)) = stream.next().await {
                    if let Ok(client) = router.add_client(id, addr).await {
                        match tx.send((id, client)) {
                            Ok(()) => (),
                            Err(SendError((_id, _client))) => {}
                        }
                    }
                }
            }
        });

        let mut peers = HashMap::new();

        while peers.len() < 2 {
            let mut timeout = time::delay_for(cluster_formation_timeout);
            loop {
                tokio::select! {
                    _ = &mut timeout, if !timeout.is_elapsed() => {
                        // purge the pending newly discovered clients if any.
                        while let Ok((id, client)) = rx.try_recv() {
                            if let Ok(state) = client.clone().write().await.handshake(self.id, self.log_store.state().await?).await {
                                peers.insert(id, (client, state));
                            }
                        }
                        break
                    },
                    Some((id, client)) = rx.recv() => {
                        if let Ok(state) = client.clone().write().await.handshake(self.id, self.log_store.state().await?).await {
                            peers.insert(id, (client, state));
                        }
                    }
                }
            }
            if peers.len() < 2 {
                info!("Could not for cluster: insuficient number of peers: {}/2", peers.len());
            }
        }

        // drop rx to initiate passive peer discovery
        drop(rx);

        if peers
            .iter()
            .all(|(_, (_, state))| *state == NodeState::Uninitialized)
        {
            let peer_ids = peers
                .iter()
                .map(|(id, _)| id)
                .chain([self.id].iter())
                .cloned()
                .collect();
            self.join_uninitialized(peer_ids).await?;
        } else {
            let cluster_members = peers
                .iter()
                .filter_map(|(_, (client, state))| if *state == NodeState::Initialized { Some(client.clone()) } else { None });
            self.join_cluster(cluster_members).await?;
        }

        Ok(())
    }

    async fn join_cluster(&self, members: impl Iterator<Item = Arc<RwLock<Client>>>) -> Result<()>{
        use client::JoinResult;
        info!("Joining cluster...");

        for member in members {
            match member.write().await.join(self.id).await {
                Ok(JoinResult::Ok) => {
                    info!("successfully joined cluster");
                    return Ok(())
                },
                // ignore the errors for now.
                Ok(JoinResult::WrongLeader(_)) => {
                    warn!("not leader, continuing...");
                    continue
                }
                other =>  {
                    info!("error joining: {:?}", other);
                    continue;
                }
            }
        }

        Err(anyhow::anyhow!("Could not join cluster"))
    }

    async fn join_uninitialized(&self, ids: HashSet<NodeId>) -> Result<()> {
        info!("initializing cluster with peers: {:?}", ids);
        match self.inner.initialize(ids).await {
            Ok(()) | Err(InitializeError::NotAllowed) => Ok(()),
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
async fn init_raft_cluster(raft_config: RaftConfig, store: Data, id: NodeId) -> Result<Raft> {
    // the raft configuration used to initialize the node
    let config = Arc::new(
        Config::build(raft_config.cluster_name.clone())
        .heartbeat_interval(150)
        .election_timeout_min(1000)
        .election_timeout_max(1500)
        .validate()?,
    );

    let router = Arc::new(RaftRouter::new());
    let log_store = Arc::new(RaftStore::new(
            id,
            raft_config.log_db_path,
            store.clone(),
            raft_config.snapshot_dir,
    )?);
    let inner = Arc::new(InnerRaft::new(
            id,
            config,
            router.clone(),
            log_store.clone(),
    ));
    let svc = RaftServerService::new(inner.clone(), router.clone(), log_store.clone());
    let server_handle = tokio::spawn(
        Server::builder()
        .add_service(RaftServiceServer::new(svc))
        .serve(raft_config.addr),
    );

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
        inner,
        id,
        server_handle,
        router,
        store,
        log_store,
    })
}

pub async fn init_raft(raft_config: RaftConfig, store: Data, id: NodeId) -> Result<Raft> {
    init_raft_cluster(raft_config, store, id).await
}

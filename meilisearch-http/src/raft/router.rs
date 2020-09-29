use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::network::RaftNetwork;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, ClientWriteResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse
};
use async_raft::NodeId;
use async_raft::{AppData, AppDataResponse};
use bincode::{deserialize, serialize};
use log::error;
use tokio::sync::RwLock;
use tonic::transport::channel::Channel;

use super::raft_service::{self, NodeState};
use super::raft_service::raft_service_client::RaftServiceClient;
use super::ClientRequest;

#[derive(Debug)]
pub struct Client {
    rpc_client: RaftServiceClient<Channel>,
    addr: SocketAddr,
}

impl Client {
    pub async fn forward<D: AppData, R: AppDataResponse>(
        &mut self,
        req: ClientWriteRequest<D>,
    ) -> Result<ClientWriteResponse<R>> {
        let message = raft_service::ClientWriteRequest {
            data: serialize(&req)?,
        };
        let response = self.rpc_client.forward(message).await?;
        let response = deserialize(&response.into_inner().data)?;
        Ok(response)
    }

    pub async fn handshake(&mut self, id: NodeId, state: NodeState) -> Result<NodeState> {
        let message = raft_service::HandshakeRequest {
            id,
            state: state as i32,
        };
        let response = self.rpc_client.handshake(message).await?;
        Ok(response.get_ref().state())
    }
}

pub struct RaftRouter {
    pub clients: RwLock<HashMap<NodeId, Arc<RwLock<Client>>>>,
}

impl RaftRouter {
    pub fn new() -> Self {
        let clients = RwLock::new(HashMap::new());
        Self { clients }
    }

    pub async fn client(&self, id: NodeId) -> Option<Arc<RwLock<Client>>> {
        let clients = self.clients.read().await;
        clients.get(&id).cloned()
    }

    pub async fn add_client(&self, id: NodeId, addr: SocketAddr) -> Result<Arc<RwLock<Client>>> {
        let mut clients = self.clients.write().await;
        match clients.entry(id) {
            Entry::Vacant(entry) => {
                let client = Client {
                    rpc_client: RaftServiceClient::connect(format!("http://{}", addr)).await?,
                    addr,
                };
                let client = entry.insert(Arc::new(RwLock::new(client)));
                Ok(client.clone())
            }
            Entry::Occupied(client) => Ok(client.get().clone()),
        }
    }

    pub async fn clients(&self) -> HashSet<NodeId> {
        unimplemented!()
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for RaftRouter {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        let client = self.client(target)
            .await
            .ok_or_else(|| anyhow::Error::msg(format!("Client {} not found.", target)))?;

        let payload = raft_service::AppendEntriesRequest {
            data: serialize(&rpc)?,
        };
        let mut client = client.write().await;

        match client.rpc_client.append_entries(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => Err(anyhow::Error::msg(status.to_string())),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let client = self.client(target)
            .await
            .ok_or_else(|| anyhow::Error::msg(format!("Client {} not found.", target)))?;

        let payload = raft_service::InstallSnapshotRequest {
            data: serialize(&rpc)?,
        };
        let mut client = client.write().await;

        match client.rpc_client.install_snapshot(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => Err(anyhow::Error::msg(status.to_string())),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let client = self.client(target)
            .await
            .ok_or_else(|| anyhow::Error::msg(format!("Client {} not found.", target)))?;

        let payload = raft_service::VoteRequest {
            data: serialize(&rpc)?,
        };
        let mut client = client.write().await;

        match client.rpc_client.vote(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => {
                error!("error connecting to peer: {}", status.to_string());
                Err(anyhow::Error::msg(status.to_string()))
            }
        }
    }
}

use std::fmt;
use std::collections::HashSet;
use std::net::SocketAddr;

use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::network::RaftNetwork;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, ClientWriteResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use async_raft::NodeId;
use async_raft::{AppData, AppDataResponse};
use bincode::{deserialize, serialize};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use log::error;
use tokio::sync::RwLock;
use tonic::transport::channel::Channel;

use super::raft_service;
use super::raft_service::raft_service_client::RaftServiceClient;
use super::ClientRequest;

#[allow(dead_code)]
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
}

pub struct RaftRouter {
    pub clients: DashMap<NodeId, RwLock<Client>>,
}

impl fmt::Debug for RaftRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftRouter")
            .field(
                "clients",
                &format!(
                    "{:?}",
                    &self
                        .clients
                        .iter()
                        .map(|e| e.key().clone())
                        .collect::<Vec<_>>()
                ),
            )
            .finish()
    }
}

impl RaftRouter {
    pub fn new() -> Self {
        let clients = DashMap::new();
        Self { clients }
    }

    pub async fn add_client(&self, id: NodeId, addr: SocketAddr) -> Result<()> {
        match self.clients.entry(id) {
            Entry::Vacant(entry) => {
                let client = Client {
                    rpc_client: RaftServiceClient::connect(format!("http://{}", addr)).await?,
                    addr,
                };
                entry.insert(RwLock::new(client));
            }
            Entry::Occupied(_) => (),
        }
        Ok(())
    }

    pub fn members(&self) -> HashSet<NodeId> {
        self.clients.iter().map(|e| *e.key()).collect()
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
        let client = self
            .clients
            .get(&target)
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
        let client = self
            .clients
            .get(&target)
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
        let client = self
            .clients
            .get(&target)
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

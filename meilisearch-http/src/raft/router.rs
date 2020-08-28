use std::collections::HashMap;

use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::network::RaftNetwork;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use async_raft::NodeId;
use bincode::{deserialize, serialize};
use tokio::sync::RwLock;
use tonic::transport::channel::Channel;

use super::raft_service;
use super::raft_service::raft_service_client::RaftServiceClient;
use super::ClientRequest;

pub struct RaftRouter {
    clients: RwLock<HashMap<NodeId, RaftServiceClient<Channel>>>,
}

impl RaftRouter {
    pub fn new() -> Self {
        let clients = RwLock::new(HashMap::new());
        Self { clients }
    }

    pub async fn add_client(&self, id: NodeId, addr: String) -> Result<()>  {
        let client = RaftServiceClient::connect(addr).await?;
        self
            .clients
            .write()
            .await
            .insert(id, client);
        Ok(())
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for RaftRouter {
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        let mut clients = self
            .clients
            .write()
            .await;
        let client = clients
            .get_mut(&target)
            .ok_or_else(|| anyhow::Error::msg("Client not found"))?;
        let payload = raft_service::AppendEntriesRequest {
            data: serialize(&rpc)?,
        };
        match client.append_entries(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => Err(anyhow::Error::msg(status.to_string())),
        }
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let mut clients = self
            .clients
            .write()
            .await;
        let client = clients
            .get_mut(&target)
            .ok_or_else(|| anyhow::Error::msg("Client not found"))?;
        let payload = raft_service::InstallSnapshotRequest {
            data: serialize(&rpc)?,
        };
        match client.install_snapshot(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => Err(anyhow::Error::msg(status.to_string())),
        }
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let mut clients = self
            .clients
            .write()
            .await;
        let client = clients
            .get_mut(&target)
            .ok_or_else(|| anyhow::Error::msg("Client not found"))?;
        let payload = raft_service::VoteRequest {
            data: serialize(&rpc)?,
        };
        match client.vote(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => Err(anyhow::Error::msg(status.to_string())),
        }
    }
}

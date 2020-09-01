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
use log::{error, info};
use tokio::sync::RwLock;
use tonic::transport::channel::Channel;

use super::raft_service;
use super::raft_service::raft_service_client::RaftServiceClient;
use super::ClientRequest;

#[allow(dead_code)]
struct Client {
    rpc_client: Option<RaftServiceClient<Channel>>,
    addr: String,
}

impl Client {
    /// method that tries to return the inner rpc client. It tries to init a connection if the
    /// connection was not already made.
    pub async fn client(&mut self) -> Result<&mut RaftServiceClient<Channel>> {
        match self.rpc_client {
            Some(ref mut client) => Ok(client),
            None => {
                self.rpc_client
                    .replace(RaftServiceClient::connect(self.addr.clone()).await?);
                info!("connected to {}", self.addr);
                Ok(self.rpc_client.as_mut().unwrap())
            }
        }
    }
}

pub struct RaftRouter {
    clients: RwLock<HashMap<NodeId, RwLock<Client>>>,
}

impl RaftRouter {
    pub fn new() -> Self {
        let clients = RwLock::new(HashMap::new());
        Self { clients }
    }

    pub async fn add_client(&self, id: NodeId, addr: String) {
        let client = Client {
            rpc_client: None,
            addr,
        };
        self.clients.write().await.insert(id, RwLock::new(client));
    }

    #[allow(dead_code)]
    pub async fn clients(&self) -> Vec<(NodeId, String)> {
        todo!()
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for RaftRouter {
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        let clients = self.clients.read().await;
        let client = clients
            .get(&target)
            .ok_or_else(|| anyhow::Error::msg("Client not found"))?;
        let mut rpc_client = client.write().await;

        let payload = raft_service::AppendEntriesRequest {
            data: serialize(&rpc)?,
        };

        match rpc_client.client().await?.append_entries(payload).await {
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
        let clients = self.clients.read().await;
        let client = clients
            .get(&target)
            .ok_or_else(|| anyhow::Error::msg("Client not found"))?;
        let mut rpc_client = client.write().await;

        let payload = raft_service::InstallSnapshotRequest {
            data: serialize(&rpc)?,
        };

        match rpc_client.client().await?.install_snapshot(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => Err(anyhow::Error::msg(status.to_string())),
        }
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let clients = self.clients.read().await;
        let client = clients
            .get(&target)
            .ok_or_else(|| anyhow::Error::msg("Client not found"))?;
        let mut rpc_client = client.write().await;

        let payload = raft_service::VoteRequest {
            data: serialize(&rpc)?,
        };

        match rpc_client.client().await?.vote(payload).await {
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

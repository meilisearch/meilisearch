use std::collections::{hash_map::Entry, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::network::RaftNetwork;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, 
    InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse 
};
use async_raft::NodeId;
use tokio::sync::{RwLock, oneshot};
use log::error;

use super::ClientRequest;
use super::client::Client;


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
                let client = Client::new(addr).await?;
                let client = entry.insert(Arc::new(RwLock::new(client)));
                Ok(client.clone())
            }
            Entry::Occupied(client) => Ok(client.get().clone()),
        }
    }
}

macro_rules! call_rpc {
    ($self:ident, $target:ident, $rpc: ident, $fn:ident) => {
        $self.client($target)
            .await
            .ok_or_else(|| anyhow::Error::msg(format!("Client {} not found.", $target)))?
            .write()
            .await
            .$fn($rpc)
            .await
    };
}

#[async_trait]
impl RaftNetwork<ClientRequest> for RaftRouter {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
         //append entries rpc can be big, so it is off loaded to another thread
        let (tx, rx) = oneshot::channel();
        let client = self.client(target)
            .await
            .ok_or_else(|| anyhow::Error::msg(format!("Client {} not found.", target)))?;
        tokio::spawn(async move {
            let resp = client
                .write()
                .await
                .append_entries(rpc)
                .await;
            if let Err(_) = tx.send(resp) {
                error!("error appending entries");
            }
        });
        rx.await?
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        call_rpc!(self, target, rpc, install_snapshot)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        call_rpc!(self, target, rpc, vote)
    }
}

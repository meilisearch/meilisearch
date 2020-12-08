use std::net::SocketAddr;

use anyhow::Result;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use async_raft::{AppData, AppDataResponse, NodeId};
use bincode::{deserialize, serialize};
use tonic::transport::channel::Channel;

use super::raft_service::raft_service_client::RaftServiceClient;
use super::raft_service::{self, NodeState};
use super::ClientRequest;

#[derive(Debug)]
pub struct Client {
    rpc_client: RaftServiceClient<Channel>,
    addr: SocketAddr,
}

impl Client {
    pub async fn new(addr: SocketAddr) -> Result<Client> {
        Ok(Client {
            rpc_client: RaftServiceClient::connect(format!("http://{}", addr)).await?,
            addr,
        })
    }

    pub async fn forward<D: AppData, R: AppDataResponse>(
        &mut self,
        req: D,
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

    /// Request to join another cluster. If the node on which this method is called is not the
    /// leader, this will fail with the `WrongLeader` status code.
    pub async fn join(&mut self, id: NodeId) -> Result<JoinResult> {
        use raft_service::Status::*;

        let message = raft_service::JoinRequest { id };
        let response = self.rpc_client.join(message).await?.into_inner();
        match response.status() {
            Success => Ok(JoinResult::Ok),
            WrongLeader => Ok(JoinResult::WrongLeader(deserialize(&response.data)?)),
            Error => Err(anyhow::anyhow!(deserialize::<String>(&response.data)?)),
        }
    }

    pub async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        let payload = raft_service::AppendEntriesRequest {
            data: serialize(&request)?,
        };

        match self.rpc_client.append_entries(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => Err(anyhow::Error::msg(status.to_string())),
        }
    }

    /// sends install_snapshot rpc to the client
    pub async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let payload = raft_service::InstallSnapshotRequest {
            data: serialize(&rpc)?,
        };

        match self.rpc_client.install_snapshot(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => Err(anyhow::Error::msg(status.to_string())),
        }
    }

    /// sends a vote request rpc to the client
    pub async fn vote(&mut self, rpc: VoteRequest) -> Result<VoteResponse> {
        let payload = raft_service::VoteRequest {
            data: serialize(&rpc)?,
        };

        match self.rpc_client.vote(payload).await {
            Ok(response) => {
                let response = deserialize(&response.into_inner().data)?;
                Ok(response)
            }
            Err(status) => {
                Err(anyhow::Error::msg(status.to_string()))
            }
        }
    }
}

#[derive(Debug)]
pub enum JoinResult {
    Ok,
    WrongLeader(NodeId),
}

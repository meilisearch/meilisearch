use std::sync::Arc;

use bincode::{deserialize, serialize};
use tonic::{Code, Request, Response, Status};

use super::raft_service::raft_service_server::RaftService;
use super::raft_service::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse, JoinRequest, JoinResponse, JoinStatus,
};
use super::Raft;
use super::router::RaftRouter;

pub struct RaftServerService {
    raft: Arc<Raft>,
    router: Arc<RaftRouter>,
}

impl RaftServerService {
    pub fn new(raft: Arc<Raft>, router: Arc<RaftRouter>) -> Self {
        Self { raft, router }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServerService {
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let request = deserialize(&request.into_inner().data)
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        let data = match self.raft.vote(request).await {
            Ok(ref response) => serialize(response).unwrap(),
            Err(e) => return Err(Status::new(Code::Internal, e.to_string())),
        };
        Ok(Response::new(VoteResponse { data }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let request = deserialize(&request.into_inner().data)
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        let data = match self.raft.append_entries(request).await {
            Ok(ref response) => serialize(response).unwrap(),
            Err(e) => return Err(Status::new(Code::Internal, e.to_string())),
        };
        Ok(Response::new(AppendEntriesResponse { data }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let request = deserialize(&request.into_inner().data)
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        let data = match self.raft.install_snapshot(request).await {
            Ok(ref response) => serialize(response).unwrap(),
            Err(e) => return Err(Status::new(Code::Internal, e.to_string())),
        };
        Ok(Response::new(InstallSnapshotResponse { data }))
    }

    // we'll need to bring the new node up to date with the cluster. In order to do that, we
    // first need to establish a connection with the new node, add it to the cluster as a
    // non-voting node, wait for it to synchronize, and finally request a membership change
    // with this new node in. If all goes well we can return SUCCESS to the new node.
    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let JoinRequest { addr, id } = request.into_inner();
        self.router.add_client(id, addr)
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        self.raft.add_non_voter(id)
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        let mut all_nodes = self.raft
            .metrics()
            .recv()
            .await
            .ok_or_else(|| Status::new(Code::Internal, "unable to get membership information"))?
            .membership_config.all_nodes();
        all_nodes.insert(id);
        self.raft.change_membership(all_nodes).await.unwrap();
        let mut result = JoinResponse::default();
        result.set_status(JoinStatus::Success);
        Ok(Response::new(result))
    }
}

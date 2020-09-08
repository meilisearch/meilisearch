use std::sync::Arc;

use bincode::{deserialize, serialize};
use tonic::{Code, Request, Response, Status};

use super::raft_service::raft_service_server::RaftService;
use super::raft_service::{
    self, AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, ClientWriteResponse,
    ConnectionRequest, ConnectionResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use super::router::RaftRouter;
use super::store::RaftStore;
use super::InnerRaft;

pub struct RaftServerService {
    raft: Arc<InnerRaft>,
    store: Arc<RaftStore>,
    router: Arc<RaftRouter>,
}

impl RaftServerService {
    pub fn new(raft: Arc<InnerRaft>, router: Arc<RaftRouter>, store: Arc<RaftStore>) -> Self {
        Self {
            raft,
            router,
            store,
        }
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

    async fn forward(
        &self,
        request: Request<ClientWriteRequest>,
    ) -> Result<Response<ClientWriteResponse>, Status> {
        let request: async_raft::raft::ClientWriteRequest<super::ClientRequest> =
            deserialize(&request.into_inner().data)
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

        let data = match self.raft.client_write(request).await {
            Ok(ref response) => serialize(response).unwrap(),
            Err(e) => return Err(Status::new(Code::Internal, e.to_string())),
        };
        Ok(Response::new(ClientWriteResponse { data }))
    }

    async fn request_connection(
        &self,
        request: Request<ConnectionRequest>,
    ) -> Result<Response<ConnectionResponse>, Status> {
        let mut response = ConnectionResponse::default();
        match request.remote_addr() {
            Some(addr) => {
                let ConnectionRequest { id } = request.get_ref();
                let _ = self.router.add_client(*id, addr).await;
                response.data = serialize(&self.store.id).unwrap();
                response.set_status(raft_service::Status::Success);
            }
            None => {
                response.set_status(raft_service::Status::Success);
                response.data = serialize(&"can't get peer addr".to_string()).unwrap();
            }
        }
        Ok(Response::new(response))
    }
}

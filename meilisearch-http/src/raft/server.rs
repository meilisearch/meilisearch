use std::sync::Arc;

use bincode::{deserialize, serialize};
use tonic::{Code, Request, Response, Status};

use super::raft_service::raft_service_server::RaftService;
use super::raft_service::{
    self, AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, ClientWriteResponse,
    ConnectionRequest, ConnectionResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse, HandshakeRequest, HandshakeResponse 
};
use super::router::RaftRouter;
use super::store::RaftStore;
use super::InnerRaft;

pub struct RaftServerService {
    raft: Arc<InnerRaft>,
    log_store: Arc<RaftStore>,
    router: Arc<RaftRouter>,
}

impl RaftServerService {
    pub fn new(raft: Arc<InnerRaft>, router: Arc<RaftRouter>, log_store: Arc<RaftStore>) -> Self {
        Self {
            raft,
            router,
            log_store,
        }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServerService {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let request = deserialize(&request.into_inner().data)
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        let data = match self.raft.vote(request).await {
            Ok(ref response) => serialize(response).unwrap(),
            Err(e) => return Err(Status::new(Code::Internal, e.to_string())),
        };
        Ok(Response::new(VoteResponse { data }))
    }

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
    async fn request_connection(
        &self,
        request: Request<ConnectionRequest>,
    ) -> Result<Response<ConnectionResponse>, Status> {
        let mut response = ConnectionResponse::default();
        match request.remote_addr() {
            Some(addr) => {
                let ConnectionRequest { id } = request.get_ref();
                let _ = self.router.add_client(*id, addr).await;
                response.data = serialize(&self.log_store.id).unwrap();
                response.set_status(raft_service::Status::Success);
            }
            None => {
                response.set_status(raft_service::Status::Success);
                response.data = serialize(&"can't get peer addr".to_string()).unwrap();
            }
        }
        Ok(Response::new(response))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn handshake(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeResponse>, Status> {
        let id = request.get_ref().id;
        let addr = request.remote_addr().ok_or_else(|| Status::aborted("No remote address"))?;
        self.router.add_client(id, addr).await.map_err(|_| Status::internal("Error adding peer"))?;
        let state = self.log_store.state().await.map_err(|_| Status::internal("Impossible to retrieve internal state"))? as i32;
        let response = HandshakeResponse { state };
        Ok(Response::new(response))
    }
}

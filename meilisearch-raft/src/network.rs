use crate::data::Data;

use actix::{Actor, Context, ResponseActFuture, Handler};
use actix_raft::{RaftNetwork, NodeId, messages};

/// Your application's network interface actor.
#[derive(Default)]
pub struct AppNetwork {
    id: NodeId,
    address: Option<String>,
    peers: Vec<String>,
}

impl Actor for AppNetwork {
    type Context = Context<Self>;

    // ... snip ... other actix methods can be implemented here as needed.
}

// Ensure you impl this over your application's data type. Here, it is `Data`.
impl RaftNetwork<Data> for AppNetwork {}

// Then you just implement the various message handlers.
// See the network chapter for details.
impl Handler<messages::AppendEntriesRequest<Data>> for AppNetwork {
    type Result = ResponseActFuture<Self, messages::AppendEntriesResponse, ()>;

    fn handle(&mut self, _msg: messages::AppendEntriesRequest<Data>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl Handler<messages::InstallSnapshotRequest> for AppNetwork {
    type Result = ResponseActFuture<Self, messages::InstallSnapshotResponse, ()>;

    fn handle(&mut self, _msg: messages::InstallSnapshotRequest, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl Handler<messages::VoteRequest> for AppNetwork {
    type Result = ResponseActFuture<Self, messages::VoteResponse, ()>;

    fn handle(&mut self, _msg: messages::VoteRequest, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

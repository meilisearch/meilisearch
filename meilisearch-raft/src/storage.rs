

use crate::error::Error;
use crate::data::{Data, DataResponse};
use actix::{Actor, Context, Handler, ResponseActFuture};
use actix_raft::{AppData, AppDataResponse, NodeId, RaftStorage, storage, messages};
use actix_raft::storage::CurrentSnapshotData;

/// Your application's storage interface actor.
#[derive(Default)]
pub struct AppStorage {
    /* ... snip ... */
}

// Ensure you impl this over your application's data, data response & error types.
impl RaftStorage<Data, DataResponse, Error> for AppStorage {
    type Actor = Self;
    type Context = Context<Self>;
}

impl Actor for AppStorage {
    type Context = Context<Self>;

    // ... snip ... other actix methods can be implemented here as needed.
}

// Then you just implement the various message handlers.
// See the storage chapter for details.
impl Handler<storage::GetInitialState<Error>> for AppStorage {
    type Result = ResponseActFuture<Self, storage::InitialState, Error>;

    fn handle(&mut self, _msg: storage::GetInitialState<Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl Handler<storage::SaveHardState<Error>> for AppStorage {
    type Result = ResponseActFuture<Self, (), Error>;

    fn handle(&mut self, _msg: storage::SaveHardState<Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl <D: AppData> Handler<storage::GetLogEntries<D, Error>> for AppStorage {
    type Result = ResponseActFuture<Self, Vec<messages::Entry<D>>, Error>;

    fn handle(&mut self, _msg: storage::GetLogEntries<D, Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl <D: AppData> Handler<storage::AppendEntryToLog<D, Error>> for AppStorage {
    type Result = ResponseActFuture<Self, (), Error>;

    fn handle(&mut self, _msg: storage::AppendEntryToLog<D, Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl <D: AppData> Handler<storage::ReplicateToLog<D, Error>> for AppStorage {
    type Result = ResponseActFuture<Self, (), Error>;

    fn handle(&mut self, _msg: storage::ReplicateToLog<D, Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl <D: AppData, R: AppDataResponse> Handler<storage::ApplyEntryToStateMachine<D, R, Error>> for AppStorage {
    type Result = ResponseActFuture<Self, R, Error>;

    fn handle(&mut self, _msg: storage::ApplyEntryToStateMachine<D, R, Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl <D: AppData> Handler<storage::ReplicateToStateMachine<D, Error>> for AppStorage {
    type Result = ResponseActFuture<Self, (), Error>;

    fn handle(&mut self, _msg: storage::ReplicateToStateMachine<D, Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl Handler<storage::CreateSnapshot<Error>> for AppStorage {
    type Result = ResponseActFuture<Self, CurrentSnapshotData, Error>;

    fn handle(&mut self, _msg: storage::CreateSnapshot<Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl Handler<storage::InstallSnapshot<Error>> for AppStorage {
    type Result = ResponseActFuture<Self, (), Error>;

    fn handle(&mut self, _msg: storage::InstallSnapshot<Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

impl Handler<storage::GetCurrentSnapshot<Error>> for AppStorage {
    type Result = ResponseActFuture<Self, Option<CurrentSnapshotData>, Error>;

    fn handle(&mut self, _msg: storage::GetCurrentSnapshot<Error>, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}

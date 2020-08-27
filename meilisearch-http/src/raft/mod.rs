mod cluster;
mod router;
mod snapshot;
mod store;

pub mod raft_service {
    tonic::include_proto!("raftservice");
}

use crate::data::{IndexCreateRequest, IndexResponse};
use async_raft::{AppData, AppDataResponse};
use meilisearch_core::settings::Settings;
use router::RaftRouter;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use store::RaftStore;

type Raft = async_raft::Raft<ClientRequest, ClientResponse, RaftRouter, RaftStore>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Message {
    CreateIndex(IndexCreateRequest),
    SettingChange(Settings),
    DocumentAddition {
        index_uid: String,
        addition: PathBuf,
        partial: bool,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The ID of the client which has sent the request.
    pub client: String,
    /// The serial number of this request.
    pub serial: u64,
    /// A string describing the status of the client. For a real application, this should probably
    /// be an enum representing all of the various types of requests / operations which a client
    /// can perform.
    pub message: Message,
}
///
/// The application data response type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientResponse {
    IndexCreation(std::result::Result<IndexResponse, String>),
}

impl AppDataResponse for ClientResponse {}

/// Error data response.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientError {
    /// This request has already been applied to the state machine, and the original response
    /// no longer exists.
    OldRequestReplayed,
}

impl AppData for ClientRequest {}

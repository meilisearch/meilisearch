use async_raft::raft::MembershipConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
pub struct RaftSnapshot {
    pub path: PathBuf,
    pub id: String,
    pub index: u64,
    pub term: u64,
    pub membership: MembershipConfig,
}

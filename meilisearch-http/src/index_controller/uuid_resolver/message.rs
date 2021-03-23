use std::path::PathBuf;

use tokio::sync::oneshot;
use uuid::Uuid;

use super::Result;

#[derive(Debug)]
pub enum UuidResolveMsg {
    Resolve {
        uid: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
    GetOrCreate {
        uid: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
    Create {
        uid: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
    Delete {
        uid: String,
        ret: oneshot::Sender<Result<Uuid>>,
    },
    List {
        ret: oneshot::Sender<Result<Vec<(String, Uuid)>>>,
    },
    SnapshotRequest {
        path: PathBuf,
        ret: oneshot::Sender<Result<Vec<Uuid>>>,
    },
}

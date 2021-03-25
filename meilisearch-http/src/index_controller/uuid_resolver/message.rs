use std::path::PathBuf;

use tokio::sync::oneshot;
use uuid::Uuid;

use super::Result;
pub enum UuidResolveMsg {
    Get {
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
    Insert {
        uuid: Uuid,
        name: String,
        ret: oneshot::Sender<Result<()>>,
    },
    SnapshotRequest {
        path: PathBuf,
        ret: oneshot::Sender<Result<Vec<Uuid>>>,
    },
}

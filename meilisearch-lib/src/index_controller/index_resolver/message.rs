use std::{collections::HashSet, path::PathBuf};

use tokio::sync::oneshot;
use uuid::Uuid;

use crate::index::Index;
use super::error::Result;

pub enum IndexResolverMsg {
    Get {
        uid: String,
        ret: oneshot::Sender<Result<Index>>,
    },
    Delete {
        uid: String,
        ret: oneshot::Sender<Result<Index>>,
    },
    List {
        ret: oneshot::Sender<Result<Vec<(String, Index)>>>,
    },
    Insert {
        uuid: Uuid,
        name: String,
        ret: oneshot::Sender<Result<()>>,
    },
    SnapshotRequest {
        path: PathBuf,
        ret: oneshot::Sender<Result<HashSet<Index>>>,
    },
    GetSize {
        ret: oneshot::Sender<Result<u64>>,
    },
    DumpRequest {
        path: PathBuf,
        ret: oneshot::Sender<Result<HashSet<Index>>>,
    },
}

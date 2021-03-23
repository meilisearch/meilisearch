use std::path::PathBuf;

use uuid::Uuid;
use tokio::sync::{oneshot, mpsc};

use super::{Result, PayloadData, UpdateStatus, UpdateMeta};

pub enum UpdateMsg<D> {
    Update {
        uuid: Uuid,
        meta: UpdateMeta,
        data: mpsc::Receiver<PayloadData<D>>,
        ret: oneshot::Sender<Result<UpdateStatus>>,
    },
    ListUpdates {
        uuid: Uuid,
        ret: oneshot::Sender<Result<Vec<UpdateStatus>>>,
    },
    GetUpdate {
        uuid: Uuid,
        ret: oneshot::Sender<Result<UpdateStatus>>,
        id: u64,
    },
    Delete {
        uuid: Uuid,
        ret: oneshot::Sender<Result<()>>,
    },
    Create {
        uuid: Uuid,
        ret: oneshot::Sender<Result<()>>,
    },
    Snapshot {
        uuid: Uuid,
        path: PathBuf,
        ret: oneshot::Sender<Result<()>>,
    },
}

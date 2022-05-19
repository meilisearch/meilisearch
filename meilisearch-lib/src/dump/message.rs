use tokio::sync::oneshot;

use super::error::Result;

pub enum DumpMsg {
    CreateDump {
        ret: oneshot::Sender<Result<DumpInfo>>,
    },
    DumpInfo {
        uid: String,
        ret: oneshot::Sender<Result<DumpInfo>>,
    },
}

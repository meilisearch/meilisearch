use tokio::sync::oneshot;

use super::error::Result;
use super::DumpInfo;

pub enum DumpMsg {
    CreateDump {
        ret: oneshot::Sender<Result<DumpInfo>>,
    },
    DumpInfo {
        uid: String,
        ret: oneshot::Sender<Result<DumpInfo>>,
    },
}

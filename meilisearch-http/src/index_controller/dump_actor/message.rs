use tokio::sync::oneshot;

use super::{DumpResult, DumpInfo};


pub enum DumpMsg {
    CreateDump {
        ret: oneshot::Sender<DumpResult<DumpInfo>>,
    },
    DumpInfo {
        uid: String,
        ret: oneshot::Sender<DumpResult<DumpInfo>>,
    },
}


use super::index_actor::IndexActorHandle;
use uuid::Uuid;
use tokio::sync::{mpsc, oneshot};

enum UpdateMsg {
    CreateIndex{
        uuid: Uuid,
        ret: oneshot::Sender<anyhow::Result<()>>,
    }
}

struct UpdateActor<S> {
    update_store: S,
    inbox: mpsc::Receiver<UpdateMsg>,
    index_actor: IndexActorHandle,
}

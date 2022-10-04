use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexUuid {
    pub uid: String,
    pub index_meta: IndexMeta,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexMeta {
    pub uuid: Uuid,
    pub creation_task_id: usize,
}

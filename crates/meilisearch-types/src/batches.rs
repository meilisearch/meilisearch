use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub type BatchId = u32;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Batch {
    pub uid: BatchId,

    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,
    // pub details: Option<Details>,

    // pub status: Status,
}

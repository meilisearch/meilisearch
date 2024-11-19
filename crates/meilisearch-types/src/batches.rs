use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::task_view::DetailsView;

pub type BatchId = u32;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Batch {
    pub uid: BatchId,

    pub details: DetailsView,

    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,
    // pub details: Option<Details>,

    // pub status: Status,
}

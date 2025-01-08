use std::collections::BTreeMap;

use milli::progress::ProgressView;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use utoipa::ToSchema;

use crate::task_view::DetailsView;
use crate::tasks::{Kind, Status};

pub type BatchId = u32;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Batch {
    pub uid: BatchId,

    #[serde(skip)]
    pub progress: Option<ProgressView>,
    pub details: DetailsView,
    pub stats: BatchStats,

    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct BatchStats {
    pub total_nb_tasks: BatchId,
    pub status: BTreeMap<Status, u32>,
    pub types: BTreeMap<Kind, u32>,
    pub index_uids: BTreeMap<String, u32>,
}

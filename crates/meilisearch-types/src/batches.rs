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

    // Enqueued at is never displayed and is only required when removing a batch.
    // It's always some except when upgrading from a database pre v1.12
    pub enqueued_at: Option<BatchEnqueuedAt>,
}

impl PartialEq for Batch {
    fn eq(&self, other: &Self) -> bool {
        let Self { uid, progress, details, stats, started_at, finished_at, enqueued_at } = self;

        *uid == other.uid
            && progress.is_none() == other.progress.is_none()
            && details == &other.details
            && stats == &other.stats
            && started_at == &other.started_at
            && finished_at == &other.finished_at
            && enqueued_at == &other.enqueued_at
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchEnqueuedAt {
    #[serde(with = "time::serde::rfc3339")]
    pub earliest: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub oldest: OffsetDateTime,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct BatchStats {
    pub total_nb_tasks: BatchId,
    pub status: BTreeMap<Status, u32>,
    pub types: BTreeMap<Kind, u32>,
    pub index_uids: BTreeMap<String, u32>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub call_trace: serde_json::Map<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_channel_congestion: Option<serde_json::Map<String, serde_json::Value>>,
}

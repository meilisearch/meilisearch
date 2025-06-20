use std::collections::BTreeMap;

use milli::progress::ProgressView;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use utoipa::ToSchema;

use crate::task_view::DetailsView;
use crate::tasks::{BatchStopReason, Kind, Status};

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
    #[serde(default = "default_stop_reason")]
    pub stop_reason: String,
}

pub fn default_stop_reason() -> String {
    BatchStopReason::default().to_string()
}

impl PartialEq for Batch {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            uid,
            progress,
            details,
            stats,
            started_at,
            finished_at,
            enqueued_at,
            stop_reason,
        } = self;

        *uid == other.uid
            && progress.is_none() == other.progress.is_none()
            && details == &other.details
            && stats == &other.stats
            && started_at == &other.started_at
            && finished_at == &other.finished_at
            && enqueued_at == &other.enqueued_at
            && stop_reason == &other.stop_reason
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
    pub progress_trace: serde_json::Map<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_channel_congestion: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub internal_database_sizes: serde_json::Map<String, serde_json::Value>,
    pub embeddings: BatchEmbeddingStats
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct BatchEmbeddingStats {
    pub total_count: usize,
    pub error_count: usize,
    pub last_error: Option<String>,
}

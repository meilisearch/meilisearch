use std::collections::BTreeMap;

use milli::progress::{EmbedderStats, ProgressView};
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
    #[serde(skip_serializing_if = "EmbedderStatsView::skip_serializing", default)]
    pub embedder_stats: EmbedderStatsView,

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
            embedder_stats,
            started_at,
            finished_at,
            enqueued_at,
            stop_reason,
        } = self;

        *uid == other.uid
            && progress.is_none() == other.progress.is_none()
            && details == &other.details
            && stats == &other.stats
            && embedder_stats == &other.embedder_stats
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

/// Statistics for a batch of tasks
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct BatchStats {
    /// Total number of tasks in the batch
    pub total_nb_tasks: BatchId,
    /// Count of tasks by status
    pub status: BTreeMap<Status, u32>,
    /// Count of tasks by type
    pub types: BTreeMap<Kind, u32>,
    /// Count of tasks by index UID
    pub index_uids: BTreeMap<String, u32>,
    /// Detailed progress trace information
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub progress_trace: serde_json::Map<String, serde_json::Value>,
    /// Write channel congestion metrics
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_channel_congestion: Option<serde_json::Map<String, serde_json::Value>>,
    /// Internal database size information
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub internal_database_sizes: serde_json::Map<String, serde_json::Value>,
}

/// Statistics for embedder requests
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct EmbedderStatsView {
    /// Total number of embedder requests
    pub total: usize,
    /// Number of failed embedder requests
    pub failed: usize,
    /// Last error message from the embedder
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_error: Option<String>,
}

impl From<&EmbedderStats> for EmbedderStatsView {
    fn from(stats: &EmbedderStats) -> Self {
        let errors = stats.errors.read().unwrap_or_else(|p| p.into_inner());
        Self {
            total: stats.total_count.load(std::sync::atomic::Ordering::Relaxed),
            failed: errors.1 as usize,
            last_error: errors.0.clone(),
        }
    }
}

impl EmbedderStatsView {
    pub fn skip_serializing(&self) -> bool {
        self.total == 0 && self.failed == 0 && self.last_error.is_none()
    }
}

use milli::progress::ProgressView;
use serde::Serialize;
use time::{Duration, OffsetDateTime};
use utoipa::ToSchema;

use crate::batches::{Batch, BatchId, BatchStats, EmbedderStatsView};
use crate::task_view::DetailsView;
use crate::tasks::serialize_duration;

/// Represents a batch of tasks that were processed together.
///
/// Meilisearch groups compatible tasks into batches for efficient processing.
/// For example, multiple document additions to the same index may be batched
/// together. Use this view to monitor batch progress and performance.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct BatchView {
    /// The unique sequential identifier assigned to this batch. Batch UIDs
    /// are assigned in order of creation and can be used to retrieve specific
    /// batch information or correlate tasks that were processed together.
    pub uid: BatchId,
    /// Real-time progress information for the batch if it's currently being
    /// processed. Contains details about which step is executing and the
    /// percentage of completion. This is `null` for completed batches.
    #[schema(value_type = Option<ProgressView>)]
    pub progress: Option<ProgressView>,
    /// Aggregated details from all tasks in this batch. For example, if the
    /// batch contains multiple document addition tasks, this will show the
    /// total number of documents received and indexed across all tasks.
    pub details: DetailsView,
    /// Statistical information about the batch, including the number of tasks
    /// by status, the types of tasks included, and the indexes affected.
    /// Useful for understanding the composition and outcome of the batch.
    pub stats: BatchStatsView,
    /// The total time spent processing this batch, formatted as an ISO-8601
    /// duration (e.g., `PT2.5S` for 2.5 seconds). This is `null` for batches
    /// that haven't finished processing yet.
    #[serde(serialize_with = "serialize_duration", default)]
    pub duration: Option<Duration>,
    /// The timestamp when Meilisearch began processing this batch, formatted
    /// as an RFC 3339 date-time string. All batches have a start time as it's
    /// set when processing begins.
    #[serde(with = "time::serde::rfc3339", default)]
    pub started_at: OffsetDateTime,
    /// The timestamp when this batch finished processing, formatted as an
    /// RFC 3339 date-time string. This is `null` for batches that are still
    /// being processed.
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub finished_at: Option<OffsetDateTime>,
    /// Explains why the batch was finalized and stopped accepting more tasks.
    /// Common reasons include reaching the maximum batch size, encountering
    /// incompatible tasks, or processing being explicitly triggered.
    #[serde(default = "meilisearch_types::batches::default_stop_reason")]
    pub batch_strategy: String,
}

/// Provides comprehensive statistics about a batch's execution.
///
/// Includes task counts, status breakdowns, and AI embedder usage. This
/// information is useful for monitoring system performance and understanding
/// batch composition.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct BatchStatsView {
    /// Core batch statistics including the total number of tasks, counts by
    /// status (succeeded, failed, canceled), task types included, and which
    /// indexes were affected by this batch.
    #[serde(flatten)]
    pub stats: BatchStats,
    /// Statistics about AI embedder API requests made during batch processing.
    /// Includes total requests, successful/failed counts, and response times.
    /// Only present when the batch involved vector embedding operations.
    #[serde(skip_serializing_if = "EmbedderStatsView::skip_serializing", default)]
    pub embedder_requests: EmbedderStatsView,
}

impl BatchView {
    pub fn from_batch(batch: &Batch) -> Self {
        Self {
            uid: batch.uid,
            progress: batch.progress.clone(),
            details: batch.details.clone(),
            stats: BatchStatsView {
                stats: batch.stats.clone(),
                embedder_requests: batch.embedder_stats.clone(),
            },
            duration: batch.finished_at.map(|finished_at| finished_at - batch.started_at),
            started_at: batch.started_at,
            finished_at: batch.finished_at,
            batch_strategy: batch.stop_reason.clone(),
        }
    }
}

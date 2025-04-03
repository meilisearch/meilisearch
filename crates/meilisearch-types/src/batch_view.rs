use milli::progress::ProgressView;
use serde::Serialize;
use time::{Duration, OffsetDateTime};
use utoipa::ToSchema;

use crate::batches::{Batch, BatchId, BatchStats};
use crate::task_view::DetailsView;
use crate::tasks::serialize_duration;

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct BatchView {
    pub uid: BatchId,
    pub progress: Option<ProgressView>,
    pub details: DetailsView,
    pub stats: BatchStats,
    #[serde(serialize_with = "serialize_duration", default)]
    pub duration: Option<Duration>,
    #[serde(with = "time::serde::rfc3339", default)]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub finished_at: Option<OffsetDateTime>,
}

impl BatchView {
    pub fn from_batch(batch: &Batch) -> Self {
        Self {
            uid: batch.uid,
            progress: batch.progress.clone(),
            details: batch.details.clone(),
            stats: batch.stats.clone(),
            duration: batch.finished_at.map(|finished_at| finished_at - batch.started_at),
            started_at: batch.started_at,
            finished_at: batch.finished_at,
        }
    }
}

use serde::Serialize;
use time::{Duration, OffsetDateTime};

use crate::{
    batches::{Batch, BatchId, BatchStats},
    task_view::DetailsView,
    tasks::serialize_duration,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchView {
    pub uid: BatchId,
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
            details: batch.details.clone(),
            stats: batch.stats.clone(),
            duration: batch.finished_at.map(|finished_at| finished_at - batch.started_at),
            started_at: batch.started_at,
            finished_at: batch.finished_at,
        }
    }
}

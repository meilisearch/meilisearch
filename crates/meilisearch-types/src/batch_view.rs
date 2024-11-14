use serde::Serialize;
use time::{Duration, OffsetDateTime};

use crate::{
    batches::{Batch, BatchId},
    tasks::serialize_duration,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchView {
    pub uid: BatchId,
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
            duration: batch.finished_at.map(|finished_at| finished_at - batch.started_at),
            started_at: batch.started_at,
            finished_at: batch.finished_at,
        }
    }
}

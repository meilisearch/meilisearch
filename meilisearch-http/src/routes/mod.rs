use std::time::Duration;

use actix_web::{get, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::index::{Settings, Unchecked};
use crate::index_controller::{UpdateMeta, UpdateResult, UpdateStatus};

pub mod document;
pub mod dump;
pub mod health;
pub mod index;
pub mod key;
pub mod search;
pub mod settings;
pub mod stats;
pub mod synonym;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name")]
pub enum UpdateType {
    ClearAll,
    Customs,
    DocumentsAddition {
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<usize>
    },
    DocumentsPartial {
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<usize>
    },
    DocumentsDeletion {
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<usize>
    },
    Settings { settings: Settings<Unchecked> },
}

impl From<&UpdateStatus> for UpdateType {
    fn from(other: &UpdateStatus) -> Self {
        use milli::update::IndexDocumentsMethod::*;

        match other.meta() {
            UpdateMeta::DocumentsAddition { method, .. } => {
                let number = match other {
                    UpdateStatus::Processed(processed) => match processed.success {
                        UpdateResult::DocumentsAddition(ref addition) => {
                            Some(addition.nb_documents)
                        }
                        _ => None,
                    },
                    _ => None,
                };

                match method {
                    ReplaceDocuments => UpdateType::DocumentsAddition { number },
                    UpdateDocuments => UpdateType::DocumentsPartial { number },
                    _ => unreachable!(),
                }
            }
            UpdateMeta::ClearDocuments => UpdateType::ClearAll,
            UpdateMeta::DeleteDocuments { ids } => {
                UpdateType::DocumentsDeletion { number: Some(ids.len()) }
            }
            UpdateMeta::Settings(settings) => UpdateType::Settings {
                settings: settings.clone(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    pub duration: f64, // in seconds
    pub enqueued_at: DateTime<Utc>,
    pub processed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    pub error: String,
    pub error_type: String,
    pub error_code: String,
    pub error_link: String,
    pub duration: f64, // in seconds
    pub enqueued_at: DateTime<Utc>,
    pub processed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnqueuedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    pub enqueued_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_processing_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum UpdateStatusResponse {
    Enqueued {
        #[serde(flatten)]
        content: EnqueuedUpdateResult,
    },
    Processing {
        #[serde(flatten)]
        content: EnqueuedUpdateResult,
    },
    Failed {
        #[serde(flatten)]
        content: FailedUpdateResult,
    },
    Processed {
        #[serde(flatten)]
        content: ProcessedUpdateResult,
    },
}

impl From<UpdateStatus> for UpdateStatusResponse {
    fn from(other: UpdateStatus) -> Self {
        let update_type = UpdateType::from(&other);

        match other {
            UpdateStatus::Processing(processing) => {
                let content = EnqueuedUpdateResult {
                    update_id: processing.id(),
                    update_type,
                    enqueued_at: processing.from.enqueued_at,
                    started_processing_at: Some(processing.started_processing_at),
                };
                UpdateStatusResponse::Processing { content }
            }
            UpdateStatus::Enqueued(enqueued) => {
                let content = EnqueuedUpdateResult {
                    update_id: enqueued.id(),
                    update_type,
                    enqueued_at: enqueued.enqueued_at,
                    started_processing_at: None,
                };
                UpdateStatusResponse::Enqueued { content }
            }
            UpdateStatus::Processed(processed) => {
                let duration = processed
                    .processed_at
                    .signed_duration_since(processed.from.started_processing_at)
                    .num_milliseconds();

                // necessary since chrono::duration don't expose a f64 secs method.
                let duration = Duration::from_millis(duration as u64).as_secs_f64();

                let content = ProcessedUpdateResult {
                    update_id: processed.id(),
                    update_type,
                    duration,
                    enqueued_at: processed.from.from.enqueued_at,
                    processed_at: processed.processed_at,
                };
                UpdateStatusResponse::Processed { content }
            }
            UpdateStatus::Aborted(_) => unreachable!(),
            UpdateStatus::Failed(failed) => {
                let duration = failed
                    .failed_at
                    .signed_duration_since(failed.from.started_processing_at)
                    .num_milliseconds();

                // necessary since chrono::duration don't expose a f64 secs method.
                let duration = Duration::from_millis(duration as u64).as_secs_f64();

                let content = FailedUpdateResult {
                    update_id: failed.id(),
                    update_type,
                    error: failed.error,
                    error_type: String::from("todo"),
                    error_code: String::from("todo"),
                    error_link: String::from("todo"),
                    duration,
                    enqueued_at: failed.from.from.enqueued_at,
                    processed_at: failed.failed_at,
                };
                UpdateStatusResponse::Failed { content }
            }
        }
    }
}

#[derive(Deserialize)]
pub struct IndexParam {
    index_uid: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

impl IndexUpdateResponse {
    pub fn with_id(update_id: u64) -> Self {
        Self { update_id }
    }
}

/// Always return a 200 with:
/// ```json
/// {
///     "status": "Meilisearch is running"
/// }
/// ```
#[get("/")]
pub async fn running() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({ "status": "MeiliSearch is running" }))
}

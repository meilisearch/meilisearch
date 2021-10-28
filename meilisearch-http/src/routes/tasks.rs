use std::time::Duration;

use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use meilisearch_lib::MeiliSearch;
use serde::Serialize;
use meilisearch_tasks::task::{DocumentAdditionMergeStrategy, DocumentDeletion, Task, TaskContent, TaskEvent};

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum TaskType {
    IndexCreation,
    IndexUpdate,
    IndexDeletion,
    DocumentsAddition,
    DocumentsPartial,
    DocumentsDeletion,
    SettingsUpdate,
    ClearAll,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum TaskStatus {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum TaskDetails {
    DocumentsUpdate {
        number_of_documents: usize,
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskResponse {
    uid: u32,
    index_uid: String,
    status: TaskStatus,
    #[serde(rename = "type")]
    task_type: TaskType,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<TaskDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ResponseError>,
    duration: Option<Duration>,
    enqueued_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl From<Task> for TaskResponse {
    fn from(task: Task) -> Self {
        let Task {
            id,
            index_uid,
            content,
            events,
        } = task;

        // An event always has at least one event: "Created"
        let (status, error, finished_at) = match events.last().unwrap() {
            TaskEvent::Created(_) => (TaskStatus::Enqueued, None, None),
            TaskEvent::Batched { .. } => (TaskStatus::Enqueued, None, None),
            TaskEvent::Processing(_) => (TaskStatus::Processing, None, None),
            TaskEvent::Succeded { timestamp, .. } => (TaskStatus::Succeeded, None, Some(*timestamp)),
            TaskEvent::Failed { timestamp, .. } => (TaskStatus::Failed, None, Some(*timestamp)),
        };

        let (task_type, details) = match content {
            TaskContent::DocumentAddition { merge_strategy, documents_count, .. } => {
                let details = TaskDetails::DocumentsUpdate {
                    number_of_documents: documents_count,
                };

                let task_type = match merge_strategy {
                    DocumentAdditionMergeStrategy::UpdateDocument => TaskType::DocumentsPartial,
                    DocumentAdditionMergeStrategy::ReplaceDocument => TaskType::DocumentsAddition,
                };

                (task_type, Some(details))
            },
            TaskContent::DocumentDeletion(DocumentDeletion::Ids(ids)) => {
                (TaskType::DocumentsDeletion, Some(TaskDetails::DocumentsUpdate { number_of_documents: ids.len() }))
            },
            TaskContent::DocumentDeletion(DocumentDeletion::Clear) => (TaskType::ClearAll, None),
            TaskContent::IndexDeletion => (TaskType::IndexDeletion, None),
            TaskContent::SettingsUpdate => (TaskType::SettingsUpdate, None),
        };

        let enqueued_at = match events.first().unwrap() {
            TaskEvent::Created(ts) => *ts,
            _ => unreachable!("A task first element should always be a cretion event."),
        };

        let duration = finished_at.map(|ts| (ts - enqueued_at).to_std().unwrap());

        let started_at = events.iter().find_map(|e| match e {
            TaskEvent::Processing(ts) => Some(*ts),
            _ => None,
        });

        Self {
            uid: id,
            index_uid,
            status,
            task_type,
            details,
            error,
            duration,
            enqueued_at,
            started_at,
            finished_at,
        }
    }
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg
        .service(web::resource("").route(web::get().to(get_tasks)))
        .service(web::resource("/{task_uid}").route(web::get().to(get_task)));
}

async fn get_tasks(
    _meilisearch: GuardedData<Private, MeiliSearch>,
) -> Result<HttpResponse, ResponseError> {
    let response = TaskResponse {
        uid: 123,
        index_uid: "hello".to_string(),
        status: TaskStatus::Enqueued,
        task_type: TaskType::ClearAll,
        details: None,
        error: None,
        duration: None,
        enqueued_at: Utc::now(),
        started_at: None,
        finished_at: Some(Utc::now()),
    };
    Ok(HttpResponse::Ok().json(response))
}

async fn get_task(
    _meilisearch: GuardedData<Private, MeiliSearch>,
    task_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    Ok(HttpResponse::Ok().body(format!("Goodbye world: {}", task_uid)))
}

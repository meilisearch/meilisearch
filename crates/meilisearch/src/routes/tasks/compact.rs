use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::time::Duration;

use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use byte_unit::Byte;
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::heed::CompactionOption;
use meilisearch_types::keys::actions;
use meilisearch_types::tasks::Status;
use serde::{Deserialize, Serialize};
use tempfile::TempPath;
use tracing::{error, info};
use utoipa::ToSchema;

use super::ActionPolicy;
use crate::extractors::authentication::GuardedData;

#[routes::routes(
    routes("/tasks/compact" => [post(compact_task_queue)]),
    tag = "Async task management",
 )]
pub struct CompactApi;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct TaskCompactionSummary {
    /// Size of the task queue database before compaction.
    #[schema(value_type = String)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_size: Option<Byte>,
    /// Size of the task queue database after compaction.
    #[schema(value_type = String)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_size: Option<Byte>,
    /// Outcome of the compaction operation.
    pub status: Status,
    /// Follow-up action required after a successful compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_required: Option<&'static str>,
    /// Error message if compaction failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Compact task queue.
///
/// Trigger a compaction process on the task queue database and return its size before and
/// after compaction. A successful compaction requires restarting the instance before it can
/// safely resume normal writes.
#[routes::path(
    security(("Bearer" = ["tasks.compact", "tasks.*", "*"])),
    responses(
        (status = 200, description = "Task queue compaction successfully completed.", body = TaskCompactionSummary, content_type = "application/json", example = json!(
            {
                "preSize": "456 kiB",
                "postSize": "123 kiB",
                "status": "succeeded",
                "actionRequired": "you must restart your instance",
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 500, description = "Task queue compaction failed", body = TaskCompactionSummary, content_type = "application/json", example = json!(
            {
                "status": "failed",
                "errorMessage": "unexpected internal error"
            }
        )),
    )
)]
pub async fn compact_task_queue(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_COMPACT }>, Data<IndexScheduler>>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_task_queue_compaction_route()?;

    let (sender, receiver) = tokio::sync::oneshot::channel();
    tokio::task::spawn_blocking(move || -> index_scheduler::Result<()> {
        // we keep an open write transaction to prevent tasks insertion in the queue. We keep
        // the transaction until the server get restarted.
        let wtxn = index_scheduler
            .env
            .write_txn()
            .inspect_err(|e| error!(error = %e, "Error when running task queue compaction"))?;

        let outcome =
            execute_task_queue_compaction(&index_scheduler).map(|(pre_size, post_size)| {
                TaskCompactionSummary {
                    pre_size: Some(Byte::from_u64(pre_size)),
                    post_size: Some(Byte::from_u64(post_size)),
                    status: Status::Succeeded,
                    action_required: Some("you must restart your instance"),
                    error_message: None,
                }
            });

        let has_failed = outcome.is_err();

        _ = sender.send(outcome);

        if has_failed {
            wtxn.abort();
            return Ok(());
        }

        loop {
            std::thread::sleep(Duration::from_millis(500));
        }
    });

    let resp = if let Ok(outcome) = receiver.await {
        match outcome {
            Err(e) => {
                HttpResponse::InternalServerError().json(report_task_queue_compaction_failure(e))
            }

            Ok(summary) => HttpResponse::Ok().json(summary),
        }
    } else {
        HttpResponse::InternalServerError()
            .json(report_task_queue_compaction_failure("unexpected internal error"))
    };

    Ok(resp)
}

fn report_task_queue_compaction_failure(msg: impl std::fmt::Display) -> TaskCompactionSummary {
    TaskCompactionSummary {
        pre_size: None,
        post_size: None,
        status: Status::Failed,
        action_required: None,
        error_message: Some(msg.to_string()),
    }
}

fn execute_task_queue_compaction(
    index_scheduler: &IndexScheduler,
) -> index_scheduler::Result<(u64, u64)> {
    let tasks_path = index_scheduler.env.path();
    let src_path = tasks_path.join("data.mdb");
    let pre_size = std::fs::metadata(&src_path)?.len();

    let dest_path = TempPath::from_path(tasks_path.join("data.mdb.cpy"));
    let dest_file = File::create(&dest_path)?;
    let mut dest_file = tempfile::NamedTempFile::from_parts(dest_file, dest_path);

    index_scheduler.env.copy_to_file(dest_file.as_file_mut(), CompactionOption::Enabled)?;
    // reset the file position as specified in the heed documentation
    dest_file.seek(SeekFrom::Start(0))?;

    let file = dest_file.persist(&src_path)?;

    // If `persist` succeeds, the compaction is considered successful. Any fallible calls that follow are not critical.
    // If one fails, the server will need to be restarted anyway. Conversely, we do not force a server restart
    // if compaction itself failed.
    _ = file.sync_all();
    let post_size = file.metadata().map_or(pre_size, |meta| meta.len());
    info!("Task queue compacted: used size {pre_size} -> {post_size} bytes");

    Ok((pre_size, post_size))
}

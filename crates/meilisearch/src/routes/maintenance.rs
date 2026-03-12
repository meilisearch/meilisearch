use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::time::Duration;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
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
use crate::Opt;

#[routes::routes(
    routes(
        "/tasks/compact" => [post(compact_task_queue)]
    ),
    tag = "Maintenance",
    tags((
        name = "Maintenance",
        description = "The `/maintenance` namespace provides maintenance related endpoints."
    )),
 )]
pub struct MaintenanceApi;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct TaskCompactionSummary {
    #[schema(value_type = String)]
    pub pre_size: Byte,
    #[schema(value_type = String)]
    pub post_size: Byte,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_required: Option<&'static str>,
}

/// Compact task queue. It triggers a compaction process on the task queue database.
#[routes::path(
    security(("Bearer" = ["tasks.compact", "*"])),
    responses(
        (status = 202, description = "Task successfully enqueued.", body = TaskCompactionSummary, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "status": "enqueued",
                "type": "taskQueueCompaction",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
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
    )
)]
pub async fn compact_task_queue(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_COMPACT }>, Data<IndexScheduler>>,
    _req: HttpRequest,
    _opt: web::Data<Opt>,
) -> Result<HttpResponse, ResponseError> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    tokio::task::spawn_blocking(move || -> index_scheduler::Result<()> {
        // we keep an open write transaction to prevent tasks insertion in the queue. We keep
        // the transaction until the server get restarted.
        let _wtxn = index_scheduler
            .lmdb_env()
            .write_txn()
            .inspect_err(|e| error!(error = %e, "Error when running task queue compaction"))?;

        let outcome =
            execute_task_queue_compaction(&index_scheduler).map(|(pre_size, post_size)| {
                TaskCompactionSummary {
                    pre_size: Byte::from_u64(pre_size),
                    post_size: Byte::from_u64(post_size),
                    status: Status::Failed,
                    action_required: Some("you must restart your instance"),
                }
            });

        _ = sender.send(outcome);

        loop {
            std::thread::sleep(Duration::from_millis(500));
        }
    });

    let summary = receiver
        .await
        .unwrap_or_else(|_| Ok(report_task_queue_compaction_failure()))
        .unwrap_or_else(|e| {
            error!(error = %e, "Error when running task queue compaction");
            report_task_queue_compaction_failure()
        });

    Ok(HttpResponse::Ok().json(summary))
}

fn report_task_queue_compaction_failure() -> TaskCompactionSummary {
    TaskCompactionSummary {
        pre_size: Byte::default(),
        post_size: Byte::default(),
        status: Status::Failed,
        action_required: None,
    }
}

fn execute_task_queue_compaction(
    index_scheduler: &IndexScheduler,
) -> index_scheduler::Result<(u64, u64)> {
    let tasks_path = index_scheduler.lmdb_env().path();
    let src_path = tasks_path.join("data.mdb");
    let pre_size = std::fs::metadata(&src_path)?.len();

    let dest_path = TempPath::from_path(tasks_path.join("data.mdb.cpy"));
    let dest_file = File::create(&dest_path)?;
    let mut dest_file = tempfile::NamedTempFile::from_parts(dest_file, dest_path);

    index_scheduler.lmdb_env().copy_to_file(dest_file.as_file_mut(), CompactionOption::Enabled)?;
    // reset the file position as specified in the heed documentation
    dest_file.seek(SeekFrom::Start(0))?;

    let file = dest_file.persist(&src_path)?;
    file.sync_all()?;
    let post_size = file.metadata()?.len();
    info!("Task queue compacted: used size {pre_size} -> {post_size} bytes");

    Ok((pre_size, post_size))
}

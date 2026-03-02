use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use meilisearch_types::tasks::KindWithContent;
use tracing::debug;

use super::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
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

/// Compact task queue. It triggers a compaction process on the task queue database.
#[routes::path(
    security(("Bearer" = ["tasks.*", "*"])),
    responses(
        (status = 202, description = "Task successfully enqueued.", body = SummarizedTaskView, content_type = "application/json", example = json!(
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
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_ALL }>, Data<IndexScheduler>>,
    req: HttpRequest,
    opt: web::Data<Opt>,
) -> Result<HttpResponse, ResponseError> {
    let task = KindWithContent::TaskQueueCompaction;
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Compact the task queue");
    Ok(HttpResponse::Accepted().json(task))
}

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::error::ResponseError;
use meilisearch_types::tasks::KindWithContent;
use tracing::debug;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::Opt;

#[routes::routes(
    routes(
        "" => post(create_dump),
    ),
    tag = "Backups",
)]
pub struct DumpApi;

crate::empty_analytics!(DumpAnalytics, "Dump Created");

/// Create dump
///
/// Trigger a dump creation process. When complete, a dump file is written to the [dump directory](https://www.meilisearch.com/docs/learn/self_hosted/configure_meilisearch_at_launch#dump-directory). The directory is created if it does not exist.
#[routes::path(
    security(("Bearer" = ["dumps.create", "dumps.*", "*"])),
    responses(
        (status = 202, description = "Dump is being created.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 0,
                "indexUid": null,
                "status": "enqueued",
                "type": "dumpCreation",
                "enqueuedAt": "2021-01-01T09:39:00.000000Z"
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
pub async fn create_dump(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DUMPS_CREATE }>, Data<IndexScheduler>>,
    auth_controller: GuardedData<ActionPolicy<{ actions::DUMPS_CREATE }>, Data<AuthController>>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(DumpAnalytics::default(), &req);

    let task = KindWithContent::DumpCreation {
        keys: auth_controller.list_keys()?,
        instance_uid: analytics.instance_uid().cloned(),
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Create dump");
    Ok(HttpResponse::Accepted().json(task))
}

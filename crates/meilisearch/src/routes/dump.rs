use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::error::ResponseError;
use meilisearch_types::tasks::KindWithContent;
use tracing::debug;
use utoipa::OpenApi;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::Opt;

#[derive(OpenApi)]
#[openapi(
    paths(create_dump),
    tags((
        name = "Dumps",
        description = "The `dumps` route allows the creation of database dumps.
Dumps are `.dump` files that can be used to launch Meilisearch. Dumps are compatible between Meilisearch versions.
Creating a dump is also referred to as exporting it, whereas launching Meilisearch with a dump is referred to as importing it.
During a [dump export](https://www.meilisearch.com/docs/reference/api/dump#create-a-dump), all indexes of the current instance are
exported—together with their documents and settings—and saved as a single `.dump` file. During a dump import,
all indexes contained in the indicated `.dump` file are imported along with their associated documents and settings.
Any existing index with the same uid as an index in the dump file will be overwritten.
Dump imports are [performed at launch](https://www.meilisearch.com/docs/learn/advanced/dumps#importing-a-dump) using an option.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/dump"),
    )),
)]
pub struct DumpApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(create_dump))));
}

crate::empty_analytics!(DumpAnalytics, "Dump Created");

/// Create a dump
///
/// Triggers a dump creation process. Once the process is complete, a dump is created in the
/// [dump directory](https://www.meilisearch.com/docs/learn/self_hosted/configure_meilisearch_at_launch#dump-directory).
/// If the dump directory does not exist yet, it will be created.
#[utoipa::path(
    post,
    path = "",
    tag = "Dumps",
    security(("Bearer" = ["dumps.create", "dumps.*", "*"])),
    responses(
        (status = 202, description = "Dump is being created", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 0,
                "indexUid": null,
                "status": "enqueued",
                "type": "dumpCreation",
                "enqueuedAt": "2021-01-01T09:39:00.000000Z"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
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

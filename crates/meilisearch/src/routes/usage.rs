use std::sync::atomic::AtomicU64;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::error::ResponseError;
use meilisearch_types::tasks::KindWithContent;
use serde::Serialize;
use tokio::sync::RwLock;
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
    paths(usage),
    tags((
        name = "Usage",
        description = "The `usage` route provides information about Meilisearch's usage of the chat tokens and internal searches.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/usage"),
    )),
)]
pub struct UsageApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(SeqHandler(usage))));
}

crate::empty_analytics!(UsageAnalytics, "Usage Fetched");

#[derive(Debug, Serialize)]
pub struct Usage {
    #[serde(serialize_with = "rwlock_serde::serialize")]
    pub tokens: RwLock<Vec<ModelUsage>>,
    pub searches: InternalSearchUsage,
}

#[derive(Debug, Serialize)]
pub struct ModelUsage {
    pub workspace: String,
    pub model: String,
    pub base_url: String,
    pub api_key: String,
    pub prompt_tokens: AtomicU64,
    pub completion_tokens: AtomicU64,
    pub total_tokens: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct InternalSearchUsage {
    pub internal_searches: AtomicU64,
    pub external_searches: AtomicU64,
    pub total_searches: AtomicU64,
}

/// Returns the usage information
#[utoipa::path(
    get,
    path = "",
    tag = "Usage",
    security(("Bearer" = ["dumps.create", "dumps.*", "*"])),
    responses(
        (status = 202, description = "Dump is being created", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 0,
                "indexUid": null,
                "status": "enqueued",
                "type": "DumpCreation",
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
pub async fn usage(
    // TODO change the actions rights
    index_scheduler: GuardedData<ActionPolicy<{ actions::DUMPS_CREATE }>, Data<IndexScheduler>>,
    auth_controller: GuardedData<ActionPolicy<{ actions::DUMPS_CREATE }>, Data<AuthController>>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(UsageAnalytics::default(), &req);

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

    debug!(returns = ?task, "Fetch usage");
    Ok(HttpResponse::Accepted().json(task))
}

mod rwlock_serde {
    use serde::ser::Serializer;
    use serde::Serialize;
    use tokio::sync::RwLock;

    pub fn serialize<S, T>(val: &RwLock<T>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        T::serialize(&*val.blocking_read(), s)
    }
}

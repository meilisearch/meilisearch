use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::actions;
use meilisearch_types::tasks::KindWithContent;
use tracing::debug;
use utoipa::OpenApi;

use super::ActionPolicy;
use crate::analytics::Analytics;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::SummarizedTaskView;

#[derive(OpenApi)]
#[openapi(
    paths(compact),
    tags(
        (
            name = "Compact an index",
            description = "The /compact route uses compacts the database to reorganize and make it smaller and more efficient.",
            external_docs(url = "https://www.meilisearch.com/docs/reference/api/compact"),
        ),
    ),
)]
pub struct CompactApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(compact))));
}

/// Compact an index
///
/// Triggers a compaction process on the specified index. Compaction reorganizes the index database to make it smaller and more efficient.
#[utoipa::path(
    post,
    path = "{indexUid}/compact",
    tag = "Compact an index",
    security(("Bearer" = ["search", "*"])),
    params(("indexUid" = String, Path, example = "movies", description = "Index Unique Identifier", nullable = false)),
    responses(
        (status = ACCEPTED, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentDeletion",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
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
pub async fn compact(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_COMPACT }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    analytics.publish(IndexCompacted::default(), &req);

    let task = KindWithContent::IndexCompaction { index_uid: index_uid.to_string() };
    let task =
        match tokio::task::spawn_blocking(move || index_scheduler.register(task, None, false))
            .await?
        {
            Ok(task) => task,
            Err(e) => return Err(e.into()),
        };

    debug!(returns = ?task, "Compact the {index_uid} index");
    Ok(HttpResponse::Accepted().json(SummarizedTaskView::from(task)))
}

crate::empty_analytics!(IndexCompacted, "Index Compacted");

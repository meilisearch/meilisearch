use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use deserr::actix_web::AwebQueryParameter;
use index_scheduler::{IndexScheduler, Query};
use meilisearch_types::batch_view::BatchView;
use meilisearch_types::batches::BatchId;
use meilisearch_types::deserr::DeserrQueryParamError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use serde::Serialize;
use utoipa::{OpenApi, ToSchema};

use super::tasks::TasksFilterQuery;
use super::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

#[derive(OpenApi)]
#[openapi(
    paths(get_batch, get_batches),
    tags((
        name = "Batches",
        description = "The /batches route gives information about the progress of batches of asynchronous operations.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/batches"),
    )),
)]
pub struct BatchesApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(SeqHandler(get_batches))))
        .service(web::resource("/{batch_id}").route(web::get().to(SeqHandler(get_batch))));
}

/// Get one batch
///
/// Get a single batch.
#[utoipa::path(
    get,
    path = "/{batchUid}",
    tag = "Batches",
    security(("Bearer" = ["tasks.get", "tasks.*", "*"])),
    params(
        ("batchUid" = String, Path, example = "8685", description = "The unique batch id", nullable = false),
    ),
    responses(
        (status = OK, description = "Return the batch", body = BatchView, content_type = "application/json", example = json!(
            {
                "uid": 1,
                "details": {},
                "progress": null,
                "stats": {
                    "totalNbTasks": 1,
                    "status": {
                        "succeeded": 1
                    },
                    "types": {
                        "documentAdditionOrUpdate": 1
                    },
                    "indexUids": {
                        "INDEX_NAME": 1
                    }
                },
                "duration": "PT0.364788S",
                "startedAt": "2024-12-10T15:48:49.672141Z",
                "finishedAt": "2024-12-10T15:48:50.036929Z"
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
async fn get_batch(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    batch_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let batch_uid_string = batch_uid.into_inner();

    let batch_uid: BatchId = match batch_uid_string.parse() {
        Ok(id) => id,
        Err(_e) => {
            return Err(
                index_scheduler::Error::InvalidBatchUid { batch_uid: batch_uid_string }.into()
            )
        }
    };

    let query = index_scheduler::Query { batch_uids: Some(vec![batch_uid]), ..Query::default() };
    let filters = index_scheduler.filters();
    let (batches, _) = index_scheduler.get_batches_from_authorized_indexes(&query, filters)?;

    if let Some(batch) = batches.first() {
        let batch_view = BatchView::from_batch(batch);
        Ok(HttpResponse::Ok().json(batch_view))
    } else {
        Err(index_scheduler::Error::BatchNotFound(batch_uid).into())
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AllBatches {
    results: Vec<BatchView>,
    total: u64,
    limit: u32,
    from: Option<u32>,
    next: Option<u32>,
}

/// Get batches
///
/// List all batches, regardless of index. The batch objects are contained in the results array.
/// Batches are always returned in descending order of uid. This means that by default, the most recently created batch objects appear first.
/// Batch results are paginated and can be filtered with query parameters.
#[utoipa::path(
    get,
    path = "",
    tag = "Batches",
    security(("Bearer" = ["tasks.get", "tasks.*", "*"])),
    params(TasksFilterQuery),
    responses(
        (status = OK, description = "Return the batches", body = AllBatches, content_type = "application/json", example = json!(
            {
                "results": [
                    {
                        "uid": 2,
                        "details": {
                            "stopWords": [
                                "of",
                                "the"
                            ]
                        },
                        "progress": null,
                        "stats": {
                            "totalNbTasks": 1,
                            "status": {
                                "succeeded": 1
                            },
                            "types": {
                                "settingsUpdate": 1
                            },
                            "indexUids": {
                                "INDEX_NAME": 1
                            }
                        },
                        "duration": "PT0.110083S",
                        "startedAt": "2024-12-10T15:49:04.995321Z",
                        "finishedAt": "2024-12-10T15:49:05.105404Z"
                    }
                ],
                "total": 3,
                "limit": 1,
                "from": 2,
                "next": 1
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
async fn get_batches(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    params: AwebQueryParameter<TasksFilterQuery, DeserrQueryParamError>,
) -> Result<HttpResponse, ResponseError> {
    let mut params = params.into_inner();
    // We +1 just to know if there is more after this "page" or not.
    params.limit.0 = params.limit.0.saturating_add(1);
    let limit = params.limit.0;
    let query = params.into_query();

    let filters = index_scheduler.filters();
    let (tasks, total) = index_scheduler.get_batches_from_authorized_indexes(&query, filters)?;
    let mut results: Vec<_> = tasks.iter().map(BatchView::from_batch).collect();

    // If we were able to fetch the number +1 tasks we asked
    // it means that there is more to come.
    let next = if results.len() == limit as usize { results.pop().map(|t| t.uid) } else { None };

    let from = results.first().map(|t| t.uid);
    let tasks = AllBatches { results, limit: limit.saturating_sub(1), total, from, next };

    Ok(HttpResponse::Ok().json(tasks))
}

use actix_web::web::{self, Data};
use actix_web::{HttpResponse, Responder};
use actix_web_lab::sse::{self, Event, Sse};
use deserr::actix_web::AwebQueryParameter;
use index_scheduler::{IndexScheduler, ModifiedTasks, Query};
use meilisearch_types::batch_view::BatchView;
use meilisearch_types::batches::BatchId;
use meilisearch_types::deserr::DeserrQueryParamError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use serde::Serialize;
use tokio::runtime::Handle;
use tokio::sync::broadcast::error::RecvError;
use utoipa::ToSchema;

use super::tasks::TasksFilterQuery;
use super::ActionPolicy;
use crate::extractors::authentication::GuardedData;

#[routes::routes(
    tag = "Async task management",
    routes(
        "" => get(get_batches),
        "/stream" => get(get_batches_stream),
        "/{batch_id}" => get(get_batch)
    ),
    tags((
        name = "Batches",
        description = "Meilisearch groups compatible tasks ([asynchronous operations](https://www.meilisearch.com/docs/learn/async/asynchronous_operations)) into batches for efficient processing. For example, multiple document additions to the same index may be batched together. The /batches routes give information about the progress of these batches and let you monitor batch progress and performance.",
    )),
)]
pub struct BatchesApi;

/// Get batch
///
/// Meilisearch groups compatible tasks ([asynchronous operations](https://www.meilisearch.com/docs/learn/async/asynchronous_operations)) into batches for efficient processing.
///
/// For example, multiple document additions to the same index may be batched together. Retrieve a single batch by its unique identifier to monitor its progress and performance.
#[routes::path(
    security(("Bearer" = ["tasks.get", "tasks.*", "*"])),
    params(
        ("batch_id" = String, Path, example = "8685", description = "The unique batch identifier.", nullable = false),
    ),
    responses(
        (status = OK, description = "Returns the batch.", body = BatchView, content_type = "application/json", example = json!(
            {
                "uid": 0,
                "details": {
                    "receivedDocuments": 1,
                    "indexedDocuments": 1
                },
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
                "finishedAt": "2024-12-10T15:48:50.036929Z",
                "batchStrategy": "batched all enqueued tasks"
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
        (status = 404, description = "Batch not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Batch not found.",
                "code": "batch_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#batch_not_found"
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

/// Response containing a paginated list of batches
#[derive(Debug, Serialize, ToSchema)]
pub struct AllBatches {
    /// Array of batch objects
    results: Vec<BatchView>,
    /// Total number of batches
    total: u64,
    /// Maximum number of batches returned
    limit: u32,
    /// The first batch uid returned
    from: Option<u32>,
    /// Value to send in from to fetch the next slice of results
    next: Option<u32>,
}

/// List batches
///
/// Meilisearch groups compatible tasks ([asynchronous operations](https://www.meilisearch.com/docs/learn/async/asynchronous_operations)) into batches for efficient processing.
///
/// For example, multiple document additions to the same index may be batched together. List batches to monitor their progress and performance.
///
/// Batches are always returned in descending order of uid. This means that by default, the most recently created batch objects appear first. Batch results are paginated and can be filtered with query parameters.
#[routes::path(
    security(("Bearer" = ["tasks.get", "tasks.*", "*"])),
    params(TasksFilterQuery),
    responses(
        (status = OK, description = "Returns the batches.", body = AllBatches, content_type = "application/json", example = json!(
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
                "total": 1,
                "limit": 20,
                "from": 1,
                "next": null
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

/// Stream batches changes
///
/// The `/batches/stream` route returns information about [asynchronous operations](https://docs.meilisearch.com/learn/advanced/asynchronous_operations.html) (indexing, document updates, settings changes, and so on).
///
/// Batches are sent throught an SSE stream any time their progress or status changes, i.e., enqueued, processing, succeeded, failed.
#[routes::path(
    security(("Bearer" = ["tasks.get", "tasks.*", "*"])),
    responses(
        (status = 200, description = "Stream of batches changes.", body = String, content_type = "application/json", example = json!(
            {
                "uid": 0,
                "details": {
                    "receivedDocuments": 1,
                    "indexedDocuments": 1
                },
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
                "finishedAt": "2024-12-10T15:48:50.036929Z",
                "batchStrategy": "batched all enqueued tasks"
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
async fn get_batches_stream(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
) -> Result<impl Responder, ResponseError> {
    index_scheduler.features().check_tasks_streaming_route("calling the /batches/stream route")?;

    let query = Query { limit: Some(u32::MAX), ..Default::default() };
    let filters = index_scheduler.filters().clone();

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let _join_handle = Handle::current().spawn(async move {
        let mut wake_up = index_scheduler.as_ref().scheduler.wake_up.resubscribe();

        'listener: loop {
            // wait for new tasks to be available. Every time tasks statuses
            // change this loop is unblocked and fetches new tasks info.
            let query = match wake_up.recv().await {
                // We list all the tasks that were imported by a dump
                Ok(ModifiedTasks::DumpImported) => query.clone(),
                Ok(ModifiedTasks::Some { ids }) => {
                    Query { uids: Some(ids.into_iter().collect()), ..query.clone() }
                }
                Err(RecvError::Closed) => break 'listener,
                Err(RecvError::Lagged(_)) => {
                    wake_up = wake_up.resubscribe();
                    continue;
                }
            };

            // TODO should I unwrap here? nooo
            let (batches, _total) =
                index_scheduler.get_batches_from_authorized_indexes(&query, &filters).unwrap();

            for task in batches.iter().map(BatchView::from_batch) {
                let data = sse::Data::new_json(task).unwrap();
                if tx.send(Event::Data(data)).await.is_err() {
                    break 'listener;
                }
            }
        }
    });

    Ok(Sse::from_infallible_receiver(rx)
        .with_retry_duration(std::time::Duration::from_secs(10))
        .customize()
        .insert_header(("X-Accel-Buffering", "no")))
}

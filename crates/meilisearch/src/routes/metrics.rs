use actix_web::http::header;
use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use index_scheduler::{IndexScheduler, Query};
use meilisearch_auth::AuthController;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use meilisearch_types::tasks::Status;
use prometheus::{Encoder, TextEncoder};
use time::OffsetDateTime;
use utoipa::OpenApi;

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::routes::create_all_stats;
use crate::search_queue::SearchQueue;

#[derive(OpenApi)]
#[openapi(paths(get_metrics))]
pub struct MetricApi;

pub fn configure(config: &mut web::ServiceConfig) {
    config.service(web::resource("").route(web::get().to(get_metrics)));
}

/// Get prometheus metrics
///
/// Retrieve metrics on the engine. See https://www.meilisearch.com/docs/learn/experimental/metrics
/// Currently, [the feature is experimental](https://www.meilisearch.com/docs/learn/experimental/overview)
/// which means it must be enabled.
#[utoipa::path(
    get,
    path = "",
    tag = "Stats",
    security(("Bearer" = ["metrics.get", "metrics.*", "*"])),
    responses(
        (status = 200, description = "The metrics of the instance", body = String, content_type = "text/plain", example = json!(
            r#"
# HELP meilisearch_db_size_bytes Meilisearch DB Size In Bytes
# TYPE meilisearch_db_size_bytes gauge
meilisearch_db_size_bytes 1130496
# HELP meilisearch_http_requests_total Meilisearch HTTP requests total
# TYPE meilisearch_http_requests_total counter
meilisearch_http_requests_total{method="GET",path="/metrics",status="400"} 1
meilisearch_http_requests_total{method="PATCH",path="/experimental-features",status="200"} 1
# HELP meilisearch_http_response_time_seconds Meilisearch HTTP response times
# TYPE meilisearch_http_response_time_seconds histogram
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="0.005"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="0.01"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="0.025"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="0.05"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="0.075"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="0.1"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="0.25"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="0.5"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="0.75"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="1"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="2.5"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="5"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="7.5"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="10"} 0
meilisearch_http_response_time_seconds_bucket{method="GET",path="/metrics",le="+Inf"} 0
meilisearch_http_response_time_seconds_sum{method="GET",path="/metrics"} 0
meilisearch_http_response_time_seconds_count{method="GET",path="/metrics"} 0
# HELP meilisearch_index_count Meilisearch Index Count
# TYPE meilisearch_index_count gauge
meilisearch_index_count 1
# HELP meilisearch_index_docs_count Meilisearch Index Docs Count
# TYPE meilisearch_index_docs_count gauge
meilisearch_index_docs_count{index="mieli"} 2
# HELP meilisearch_is_indexing Meilisearch Is Indexing
# TYPE meilisearch_is_indexing gauge
meilisearch_is_indexing 0
# HELP meilisearch_last_update Meilisearch Last Update
# TYPE meilisearch_last_update gauge
meilisearch_last_update 1726675964
# HELP meilisearch_nb_tasks Meilisearch Number of tasks
# TYPE meilisearch_nb_tasks gauge
meilisearch_nb_tasks{kind="indexes",value="mieli"} 39
meilisearch_nb_tasks{kind="statuses",value="canceled"} 0
meilisearch_nb_tasks{kind="statuses",value="enqueued"} 0
meilisearch_nb_tasks{kind="statuses",value="failed"} 4
meilisearch_nb_tasks{kind="statuses",value="processing"} 0
meilisearch_nb_tasks{kind="statuses",value="succeeded"} 35
meilisearch_nb_tasks{kind="types",value="documentAdditionOrUpdate"} 9
meilisearch_nb_tasks{kind="types",value="documentDeletion"} 0
meilisearch_nb_tasks{kind="types",value="documentEdition"} 0
meilisearch_nb_tasks{kind="types",value="dumpCreation"} 0
meilisearch_nb_tasks{kind="types",value="indexCreation"} 0
meilisearch_nb_tasks{kind="types",value="indexDeletion"} 8
meilisearch_nb_tasks{kind="types",value="indexSwap"} 0
meilisearch_nb_tasks{kind="types",value="indexUpdate"} 0
meilisearch_nb_tasks{kind="types",value="settingsUpdate"} 22
meilisearch_nb_tasks{kind="types",value="snapshotCreation"} 0
meilisearch_nb_tasks{kind="types",value="taskCancelation"} 0
meilisearch_nb_tasks{kind="types",value="taskDeletion"} 0
# HELP meilisearch_used_db_size_bytes Meilisearch Used DB Size In Bytes
# TYPE meilisearch_used_db_size_bytes gauge
meilisearch_used_db_size_bytes 409600
"#
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
pub async fn get_metrics(
    index_scheduler: GuardedData<ActionPolicy<{ actions::METRICS_GET }>, Data<IndexScheduler>>,
    auth_controller: Data<AuthController>,
    search_queue: web::Data<SearchQueue>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_metrics()?;
    let auth_filters = index_scheduler.filters();
    if !auth_filters.all_indexes_authorized() {
        let mut error = ResponseError::from(AuthenticationError::InvalidToken);
        error
            .message
            .push_str(" The API key for the `/metrics` route must allow access to all indexes.");
        return Err(error);
    }

    let response = create_all_stats((*index_scheduler).clone(), auth_controller, auth_filters)?;

    crate::metrics::MEILISEARCH_DB_SIZE_BYTES.set(response.database_size as i64);
    crate::metrics::MEILISEARCH_USED_DB_SIZE_BYTES.set(response.used_database_size as i64);
    crate::metrics::MEILISEARCH_INDEX_COUNT.set(response.indexes.len() as i64);

    crate::metrics::MEILISEARCH_SEARCH_QUEUE_SIZE.set(search_queue.capacity() as i64);
    crate::metrics::MEILISEARCH_SEARCHES_RUNNING.set(search_queue.searches_running() as i64);
    crate::metrics::MEILISEARCH_SEARCHES_WAITING_TO_BE_PROCESSED
        .set(search_queue.searches_waiting() as i64);

    for (index, value) in response.indexes.iter() {
        crate::metrics::MEILISEARCH_INDEX_DOCS_COUNT
            .with_label_values(&[index])
            .set(value.number_of_documents as i64);
    }

    for (kind, value) in index_scheduler.get_stats()? {
        for (value, count) in value {
            crate::metrics::MEILISEARCH_NB_TASKS
                .with_label_values(&[&kind, &value])
                .set(count as i64);
        }
    }

    if let Some(last_update) = response.last_update {
        crate::metrics::MEILISEARCH_LAST_UPDATE.set(last_update.unix_timestamp());
    }
    crate::metrics::MEILISEARCH_IS_INDEXING.set(index_scheduler.is_task_processing()? as i64);

    let task_queue_latency_seconds = index_scheduler
        .get_tasks_from_authorized_indexes(
            &Query {
                limit: Some(1),
                reverse: Some(true),
                statuses: Some(vec![Status::Enqueued, Status::Processing]),
                ..Query::default()
            },
            auth_filters,
        )?
        .0
        .first()
        .map(|task| (OffsetDateTime::now_utc() - task.enqueued_at).as_seconds_f64())
        .unwrap_or(0.0);
    crate::metrics::MEILISEARCH_TASK_QUEUE_LATENCY_SECONDS.set(task_queue_latency_seconds);

    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    encoder.encode(&prometheus::gather(), &mut buffer).expect("Failed to encode metrics");

    let response = String::from_utf8(buffer).expect("Failed to convert bytes to string");

    Ok(HttpResponse::Ok().insert_header(header::ContentType(mime::TEXT_PLAIN)).body(response))
}

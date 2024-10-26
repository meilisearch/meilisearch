use actix_web::http::header;
use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use build_info;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use prometheus::{Encoder, TextEncoder};

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::routes::create_all_stats;

pub fn configure(config: &mut web::ServiceConfig) {
    config.service(web::resource("").route(web::get().to(get_metrics)));
}

pub async fn get_metrics(
    index_scheduler: GuardedData<ActionPolicy<{ actions::METRICS_GET }>, Data<IndexScheduler>>,
    auth_controller: Data<AuthController>,
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

    let build_info = build_info::BuildInfo::from_build();
    let commit_sha = build_info.commit_sha1.unwrap_or("unknown").to_string();
    let pkg_version = env!("CARGO_PKG_VERSION").to_string();
    crate::metrics::MEILISEARCH_BUILD_INFO.with_label_values(&[&commit_sha, &pkg_version]).set(1);

    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    encoder.encode(&prometheus::gather(), &mut buffer).expect("Failed to encode metrics");

    let response = String::from_utf8(buffer).expect("Failed to convert bytes to string");

    Ok(HttpResponse::Ok().insert_header(header::ContentType(mime::TEXT_PLAIN)).body(response))
}

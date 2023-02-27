use actix_web::http::header;
use actix_web::web::{self, Data};
use actix_web::HttpResponse;
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
    auth_controller: GuardedData<ActionPolicy<{ actions::METRICS_GET }>, AuthController>,
) -> Result<HttpResponse, ResponseError> {
    let auth_filters = index_scheduler.filters();
    if !auth_filters.all_indexes_authorized() {
        let mut error = ResponseError::from(AuthenticationError::InvalidToken);
        error
            .message
            .push_str(" The API key for the `/metrics` route must allow access to all indexes.");
        return Err(error);
    }

    let response =
        create_all_stats((*index_scheduler).clone(), (*auth_controller).clone(), auth_filters)?;

    crate::metrics::MEILISEARCH_DB_SIZE_BYTES.set(response.database_size as i64);
    crate::metrics::MEILISEARCH_INDEX_COUNT.set(response.indexes.len() as i64);

    for (index, value) in response.indexes.iter() {
        crate::metrics::MEILISEARCH_INDEX_DOCS_COUNT
            .with_label_values(&[index])
            .set(value.number_of_documents as i64);
    }

    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    encoder.encode(&prometheus::gather(), &mut buffer).expect("Failed to encode metrics");

    let response = String::from_utf8(buffer).expect("Failed to convert bytes to string");

    Ok(HttpResponse::Ok().insert_header(header::ContentType(mime::TEXT_PLAIN)).body(response))
}

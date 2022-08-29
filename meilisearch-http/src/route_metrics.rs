use std::future::{ready, Ready};

use actix_web::http::header;
use actix_web::HttpResponse;
use actix_web::{
    dev::{self, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use futures_util::future::LocalBoxFuture;
use meilisearch_auth::actions;
use meilisearch_lib::MeiliSearch;
use meilisearch_types::error::ResponseError;
use prometheus::HistogramTimer;
use prometheus::{Encoder, TextEncoder};

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;

pub async fn get_metrics(
    meilisearch: GuardedData<ActionPolicy<{ actions::METRICS_GET }>, MeiliSearch>,
) -> Result<HttpResponse, ResponseError> {
    let search_rules = &meilisearch.filters().search_rules;
    let response = meilisearch.get_all_stats(search_rules).await?;

    crate::metrics::MEILISEARCH_DB_SIZE_BYTES.set(response.database_size as i64);
    crate::metrics::MEILISEARCH_INDEX_COUNT.set(response.indexes.len() as i64);

    for (index, value) in response.indexes.iter() {
        crate::metrics::MEILISEARCH_INDEX_DOCS_COUNT
            .with_label_values(&[index])
            .set(value.number_of_documents as i64);
    }

    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    encoder
        .encode(&prometheus::gather(), &mut buffer)
        .expect("Failed to encode metrics");

    let response = String::from_utf8(buffer).expect("Failed to convert bytes to string");

    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType(mime::TEXT_PLAIN))
        .body(response))
}

pub struct RouteMetrics;

// Middleware factory is `Transform` trait from actix-service crate
// `S` - type of the next service
// `B` - type of response's body
impl<S, B> Transform<S, ServiceRequest> for RouteMetrics
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = RouteMetricsMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RouteMetricsMiddleware { service }))
    }
}

pub struct RouteMetricsMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for RouteMetricsMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    dev::forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let mut histogram_timer: Option<HistogramTimer> = None;
        let request_path = req.path();
        let is_registered_resource = req.resource_map().has_resource(request_path);
        if is_registered_resource {
            let request_method = req.method().to_string();
            histogram_timer = Some(
                crate::metrics::HTTP_RESPONSE_TIME_SECONDS
                    .with_label_values(&[&request_method, request_path])
                    .start_timer(),
            );
            crate::metrics::HTTP_REQUESTS_TOTAL
                .with_label_values(&[&request_method, request_path])
                .inc();
        }

        let fut = self.service.call(req);

        Box::pin(async move {
            let res = fut.await?;

            if let Some(histogram_timer) = histogram_timer {
                histogram_timer.observe_duration();
            };
            Ok(res)
        })
    }
}

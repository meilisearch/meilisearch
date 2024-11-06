//! Contains all the custom middleware used in meilisearch

use std::future::{ready, Ready};

use actix_web::dev::{self, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::web::Data;
use actix_web::Error;
use futures_util::future::LocalBoxFuture;
use index_scheduler::IndexScheduler;
use prometheus::HistogramTimer;

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

        // calling unwrap here is safe because index scheduler is added to app data while creating actix app.
        // also, the tests will fail if this is not present.
        let index_scheduler = req.app_data::<Data<IndexScheduler>>().unwrap();
        let features = index_scheduler.features();

        let request_path = req.path();
        let request_pattern = req.match_pattern();
        let metric_path = request_pattern.as_ref().map_or(request_path, String::as_str).to_string();
        let request_method = req.method().to_string();

        if features.check_metrics().is_ok() {
            let is_registered_resource = req.resource_map().has_resource(request_path);
            if is_registered_resource {
                histogram_timer = Some(
                    crate::metrics::MEILISEARCH_HTTP_RESPONSE_TIME_SECONDS
                        .with_label_values(&[&request_method, &metric_path])
                        .start_timer(),
                );
            }
        };

        let fut = self.service.call(req);

        Box::pin(async move {
            let res = fut.await?;

            crate::metrics::MEILISEARCH_HTTP_REQUESTS_TOTAL
                .with_label_values(&[&request_method, &metric_path, res.status().as_str()])
                .inc();

            if let Some(histogram_timer) = histogram_timer {
                histogram_timer.observe_duration();
            };
            Ok(res)
        })
    }
}

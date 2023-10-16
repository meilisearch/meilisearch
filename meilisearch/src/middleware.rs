//! Contains all the custom middleware used in meilisearch

use std::future::{ready, Ready};

use actix_web::dev::{self, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
use actix_web::web::Data;
use futures_util::future::LocalBoxFuture;
use index_scheduler::IndexScheduler;
use prometheus::HistogramTimer;

pub struct RouteMetrics;

pub struct RouteMetricsMiddlewareFactory {
    index_scheduler: Data<IndexScheduler>,
}

impl RouteMetricsMiddlewareFactory {
    pub fn new(index_scheduler: Data<IndexScheduler>) -> Self {
        RouteMetricsMiddlewareFactory { index_scheduler }
    }
}


// Middleware factory is `Transform` trait from actix-service crate
// `S` - type of the next service
// `B` - type of response's body
impl<S, B> Transform<S, ServiceRequest> for RouteMetricsMiddlewareFactory
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
        ready(Ok(RouteMetricsMiddleware { service, index_scheduler: self.index_scheduler.clone() }))
    }
}

pub struct RouteMetricsMiddleware<S> {
    service: S,
    index_scheduler: Data<IndexScheduler>,
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
        if let Ok(()) = self.index_scheduler.features().and_then(|features| features.check_metrics()) {
            let request_path = req.path();
            let is_registered_resource = req.resource_map().has_resource(request_path);
            if is_registered_resource {
                let request_method = req.method().to_string();
                histogram_timer = Some(
                    crate::metrics::MEILISEARCH_HTTP_RESPONSE_TIME_SECONDS
                        .with_label_values(&[&request_method, request_path])
                        .start_timer(),
                );
                crate::metrics::MEILISEARCH_HTTP_REQUESTS_TOTAL
                    .with_label_values(&[&request_method, request_path])
                    .inc();
            }
        };

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

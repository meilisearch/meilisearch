//! Contains all the custom middleware used in meilisearch

use std::future::{ready, Ready};

use actix_web::body::EitherBody;
use actix_web::dev::{self, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::web::Data;
use actix_web::{Error, HttpResponse};
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

        // Check cluster degradation state (no leader known = degraded)
        #[cfg(feature = "cluster")]
        let cluster_degraded = {
            req.app_data::<Data<crate::cluster::ClusterState>>()
                .is_some_and(|cs| cs.raft_node.is_some() && cs.current_leader_url().is_none())
        };
        #[cfg(not(feature = "cluster"))]
        let cluster_degraded = false;

        let fut = self.service.call(req);

        Box::pin(async move {
            let mut res = fut.await?;

            if cluster_degraded {
                res.headers_mut().insert(
                    actix_web::http::header::HeaderName::from_static("x-meili-cluster-state"),
                    actix_web::http::header::HeaderValue::from_static("degraded"),
                );
            }

            crate::metrics::MEILISEARCH_HTTP_REQUESTS_TOTAL
                .with_label_values(
                    &[request_method, metric_path, res.status().as_str().to_string()][..],
                )
                .inc();

            if let Some(histogram_timer) = histogram_timer {
                histogram_timer.observe_duration();
            };
            Ok(res)
        })
    }
}

/// Middleware that validates cluster authentication on forwarded requests.
///
/// When a follower forwards a request to the leader, it includes `X-Meili-Forwarded-For`
/// and `X-Meili-Cluster-Auth` headers. This middleware validates that the cluster auth
/// header matches the configured cluster secret, rejecting forged forwarded requests.
pub struct ClusterAuthGuard;

impl<S, B> Transform<S, ServiceRequest> for ClusterAuthGuard
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type InitError = ();
    type Transform = ClusterAuthGuardMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ClusterAuthGuardMiddleware { service }))
    }
}

pub struct ClusterAuthGuardMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for ClusterAuthGuardMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    dev::forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Only validate requests with X-Meili-Forwarded-For (forwarded from a follower)
        let is_forwarded = req.headers().get("x-meili-forwarded-for").is_some();

        if is_forwarded {
            if let Some(cs) = req.app_data::<Data<crate::cluster::ClusterState>>() {
                if let Some(ref expected_secret) = cs.cluster_secret {
                    let auth_header = req.headers().get("x-meili-cluster-auth");
                    let valid = match auth_header {
                        Some(val) => {
                            val.to_str().map_or(false, |v| v == expected_secret.as_str())
                        }
                        None => false,
                    };
                    if !valid {
                        tracing::warn!(
                            remote_addr = ?req.connection_info().peer_addr(),
                            "Rejected forwarded request with invalid cluster auth"
                        );
                        let response = HttpResponse::Forbidden().json(serde_json::json!({
                            "message": "Invalid or missing cluster authentication",
                            "code": "cluster_auth_failed",
                            "type": "auth"
                        }));
                        return Box::pin(ready(Ok(
                            req.into_response(response).map_into_right_body()
                        )));
                    }
                }
            }
        }

        let fut = self.service.call(req);
        Box::pin(async move { fut.await.map(|res| res.map_into_left_body()) })
    }
}

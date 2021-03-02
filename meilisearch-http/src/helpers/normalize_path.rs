/// From https://docs.rs/actix-web/3.0.0-alpha.2/src/actix_web/middleware/normalize.rs.html#34
use actix_http::Error;
use actix_service::{Service, Transform};
use actix_web::{
    dev::ServiceRequest,
    dev::ServiceResponse,
    http::uri::{PathAndQuery, Uri},
};
use futures::future::{ok, Ready};
use regex::Regex;
use std::task::{Context, Poll};
pub struct NormalizePath;

impl<S, B> Transform<S> for NormalizePath
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = NormalizePathNormalization<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(NormalizePathNormalization {
            service,
            merge_slash: Regex::new("//+").unwrap(),
        })
    }
}

pub struct NormalizePathNormalization<S> {
    service: S,
    merge_slash: Regex,
}

impl<S, B> Service for NormalizePathNormalization<S>
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut req: ServiceRequest) -> Self::Future {
        let head = req.head_mut();

        // always add trailing slash, might be an extra one
        let path = head.uri.path().to_string() + "/";

        if self.merge_slash.find(&path).is_some() {
            // normalize multiple /'s to one /
            let path = self.merge_slash.replace_all(&path, "/");

            let path = if path.len() > 1 {
                path.trim_end_matches('/')
            } else {
                &path
            };

            let mut parts = head.uri.clone().into_parts();
            let pq = parts.path_and_query.as_ref().unwrap();

            let path = if let Some(q) = pq.query() {
                bytes::Bytes::from(format!("{}?{}", path, q))
            } else {
                bytes::Bytes::copy_from_slice(path.as_bytes())
            };
            parts.path_and_query = Some(PathAndQuery::from_maybe_shared(path).unwrap());

            let uri = Uri::from_parts(parts).unwrap();
            req.match_info_mut().get_mut().update(&uri);
            req.head_mut().uri = uri;
        }

        self.service.call(req)
    }
}

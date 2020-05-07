///
/// This middleware normalizes slashes in paths
/// * consecutive instances of `/` get collapsed into one `/`
/// * any ending `/` is removed.
/// Original source from: https://gitlab.com/snippets/1884466
///
/// Ex:
///   /this///url/
///   becomes : /this/url
///
use actix_service::{Service, Transform};
use actix_web::{
    dev::ServiceRequest,
    dev::ServiceResponse,
    http::uri::{PathAndQuery, Uri},
    Error as ActixError,
};
use futures::future::{ok, Ready};
use regex::Regex;
use std::task::{Context, Poll};

pub struct NormalizeSlashes;

impl<S, B> Transform<S> for NormalizeSlashes
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = ActixError>,
    S::Future: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = ActixError;
    type InitError = ();
    type Transform = SlashNormalization<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(SlashNormalization { service })
    }
}

pub struct SlashNormalization<S> {
    service: S,
}

impl<S, B> Service for SlashNormalization<S>
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = ActixError>,
    S::Future: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = ActixError;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut req: ServiceRequest) -> Self::Future {
        let head = req.head();

        let path = head.uri.path();
        let original_len = path.len();
        let slash_regex = Regex::new("//+").unwrap();
        let new_path = slash_regex.replace_all(path, "/");
        let new_path = new_path.trim_end_matches("/");

        if original_len != new_path.len() {
            let mut parts = head.uri.clone().into_parts();

            let path = match parts.path_and_query.as_ref().map(|pq| pq.query()).flatten() {
                Some(q) => bytes::Bytes::from(format!("{}?{}", new_path, q)),
                None => bytes::Bytes::from(new_path.to_string()),
            };

            if let Ok(pq) = PathAndQuery::from_maybe_shared(path) {
                parts.path_and_query = Some(pq);

                if let Ok(uri) = Uri::from_parts(parts) {
                    req.match_info_mut().get_mut().update(&uri);
                    req.head_mut().uri = uri;
                }
            }
        }

        self.service.call(req)
    }
}

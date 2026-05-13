#[cfg(not(feature = "enterprise"))]
pub mod community_edition;
#[cfg(feature = "enterprise")]
pub mod enterprise_edition;
use actix_http::header::CONTENT_TYPE;
use actix_http::uri::PathAndQuery;
use actix_web::HttpRequest;
#[cfg(not(feature = "enterprise"))]
pub use community_edition::{proxy, task_network_and_check_leader_and_version};
#[cfg(feature = "enterprise")]
pub use enterprise_edition::{
    import_data_from_req, import_metadata_from_req, origin_from_req, proxy, send_request,
    task_network_and_check_leader_and_version,
};

mod body;
mod error;

pub use body::Body;
pub use error::{ProxyError, ReqwestErrorWithoutUrl};

pub trait Endpoint {
    fn content_type(&self) -> Option<&[u8]>;
    fn method(&self) -> http_client::reqwest::Method;
    fn path_and_query(&self) -> PathAndQuery;
}

impl Endpoint for HttpRequest {
    fn content_type(&self) -> Option<&[u8]> {
        self.headers().get(CONTENT_TYPE).map(|h| h.as_bytes())
    }

    fn method(&self) -> http_client::reqwest::Method {
        from_old_http_method(self.method())
    }

    fn path_and_query(&self) -> PathAndQuery {
        self.uri().path_and_query().cloned().unwrap_or_else(|| PathAndQuery::from_static("/"))
    }
}

fn from_old_http_method(method: &actix_http::Method) -> http_client::reqwest::Method {
    match method {
        &actix_http::Method::CONNECT => http_client::reqwest::Method::CONNECT,
        &actix_http::Method::DELETE => http_client::reqwest::Method::DELETE,
        &actix_http::Method::GET => http_client::reqwest::Method::GET,
        &actix_http::Method::HEAD => http_client::reqwest::Method::HEAD,
        &actix_http::Method::OPTIONS => http_client::reqwest::Method::OPTIONS,
        &actix_http::Method::PATCH => http_client::reqwest::Method::PATCH,
        &actix_http::Method::POST => http_client::reqwest::Method::POST,
        &actix_http::Method::PUT => http_client::reqwest::Method::PUT,
        &actix_http::Method::TRACE => http_client::reqwest::Method::TRACE,
        method => http_client::reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap(),
    }
}

pub struct OverrideEndpoint<'a, E> {
    endpoint: &'a E,
    path_and_query: Option<PathAndQuery>,
    method: Option<http_client::reqwest::Method>,
    content_type: Option<Vec<u8>>,
}

impl<'a, E> OverrideEndpoint<'a, E> {
    pub fn new(
        endpoint: &'a E,
        path_and_query: Option<PathAndQuery>,
        method: Option<http_client::reqwest::Method>,
        content_type: Option<Vec<u8>>,
    ) -> Self {
        Self { endpoint, path_and_query, method, content_type }
    }
}

impl<'a, E: Endpoint> Endpoint for OverrideEndpoint<'a, E> {
    fn content_type(&self) -> Option<&[u8]> {
        self.content_type.as_deref().or_else(|| self.endpoint.content_type())
    }

    fn method(&self) -> http_client::reqwest::Method {
        self.method.clone().unwrap_or_else(|| self.endpoint.method())
    }

    fn path_and_query(&self) -> PathAndQuery {
        self.path_and_query.clone().unwrap_or_else(|| self.endpoint.path_and_query())
    }
}

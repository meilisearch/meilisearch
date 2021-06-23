pub mod data;
#[macro_use]
pub mod error;
pub mod helpers;
mod index;
mod index_controller;
pub mod option;
pub mod routes;

#[cfg(all(not(debug_assertions), feature = "analytics"))]
pub mod analytics;

use std::{pin::Pin, task::{Context, Poll}};

pub use self::data::Data;
use futures::{Stream, future::{Ready, ready}};
pub use option::Opt;

use actix_web::{FromRequest, HttpRequest, dev, error::PayloadError, web};

pub fn configure_data(config: &mut web::ServiceConfig, data: Data) {
    let http_payload_size_limit = data.http_payload_size_limit();
    config
        .data(data)
        .app_data(
            web::JsonConfig::default()
                .limit(dbg!(http_payload_size_limit))
                .content_type(|_mime| true) // Accept all mime types
                .error_handler(|err, _req| error::payload_error_handler(err).into()),
        )
        .app_data(PayloadConfig::new(http_payload_size_limit))
        .app_data(
            web::QueryConfig::default()
                .error_handler(|err, _req| error::payload_error_handler(err).into()),
        );
}

#[cfg(feature = "mini-dashboard")]
pub fn dashboard(config: &mut web::ServiceConfig, enable_frontend: bool) {
    use actix_web_static_files::Resource;
    use actix_web::HttpResponse;

    mod dashboard {
        include!(concat!(env!("OUT_DIR"), "/generated.rs"));
    }

    if enable_frontend {
        let generated = dashboard::generate();
            let mut scope = web::scope("/");
            // Generate routes for mini-dashboard assets
            for (path, resource) in generated.into_iter() {
                let Resource {mime_type, data, ..} = resource;
                // Redirect index.html to /
                if path == "index.html" {
                    config.service(web::resource("/").route(web::get().to(move || {
                        HttpResponse::Ok().content_type(mime_type).body(data)
                    })));
                } else {
                    scope = scope.service(web::resource(path).route(web::get().to(move || {
                        HttpResponse::Ok().content_type(mime_type).body(data)
                    })));
                }
            }
            config.service(scope);
    } else {
        config.service(routes::running);
    }
}

#[cfg(not(feature = "mini-dashboard"))]
pub fn dashboard(config: &mut web::ServiceConfig, _enable_frontend: bool) {
    config.service(routes::running);
}

#[macro_export]
macro_rules! create_app {
    ($data:expr, $enable_frontend:expr) => {{
        use actix_cors::Cors;
        use actix_web::middleware::TrailingSlash;
        use actix_web::App;
        use actix_web::{middleware, web};
        use meilisearch_http::routes::*;
        use meilisearch_http::{dashboard, configure_data};

        App::new()
            .configure(|s| configure_data(s, $data.clone()))
            .configure(document::services)
            .configure(index::services)
            .configure(search::services)
            .configure(settings::services)
            .configure(health::services)
            .configure(stats::services)
            .configure(key::services)
            .configure(dump::services)
            .configure(|s| dashboard(s, $enable_frontend))
            .wrap(
                Cors::default()
                .send_wildcard()
                .allowed_headers(vec!["content-type", "x-meili-api-key"])
                .allow_any_origin()
                .allow_any_method()
                .max_age(86_400), // 24h
            )
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(middleware::NormalizePath::new(middleware::TrailingSlash::Trim))
    }};
}

pub struct Payload {
    payload: dev::Payload,
    limit: usize,
}

pub struct PayloadConfig {
    limit: usize,
}

impl PayloadConfig {
    pub fn new(limit: usize) -> Self { Self { limit } }
}

impl Default for PayloadConfig {
    fn default() -> Self {
        Self { limit: 256 * 1024  }
    }
}

impl FromRequest for Payload {
    type Config = PayloadConfig;

    type Error = PayloadError;

    type Future = Ready<Result<Payload, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, payload: &mut dev::Payload) -> Self::Future {
        let limit = req.app_data::<PayloadConfig>().map(|c| c.limit).unwrap_or(Self::Config::default().limit);
        ready(Ok(Payload { payload: payload.take(), limit }))
    }
}

impl Stream for Payload {
    type Item = Result<web::Bytes, PayloadError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.payload).poll_next(cx) {
            Poll::Ready(Some(result)) => {
                match result {
                    Ok(bytes) => {
                        match self.limit.checked_sub(bytes.len()) {
                            Some(new_limit) => {
                                self.limit = new_limit;
                                Poll::Ready(Some(Ok(bytes)))
                            }
                            None => Poll::Ready(Some(Err(PayloadError::Overflow))),
                        }
                    }
                    x => Poll::Ready(Some(x)),
                }
            },
            otherwise => otherwise,
        }
    }
}

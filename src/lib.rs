#![allow(clippy::or_fun_call)]

pub mod data;
pub mod error;
pub mod helpers;
pub mod option;
pub mod routes;
mod index_controller;

use actix_http::Error;
use actix_service::ServiceFactory;
use actix_web::{dev, web, App};

pub use option::Opt;
pub use self::data::Data;
use self::error::payload_error_handler;

pub fn create_app(
    data: &Data,
    enable_frontend: bool,
) -> App<
    impl ServiceFactory<
        Config = (),
        Request = dev::ServiceRequest,
        Response = dev::ServiceResponse<actix_http::body::Body>,
        Error = Error,
        InitError = (),
    >,
    actix_http::body::Body,
> {
    let app = App::new()
        .data(data.clone())
        .app_data(
            web::JsonConfig::default()
                .limit(data.http_payload_size_limit())
                .content_type(|_mime| true) // Accept all mime types
                .error_handler(|err, _req| payload_error_handler(err).into()),
        )
        .app_data(
            web::QueryConfig::default()
            .error_handler(|err, _req| payload_error_handler(err).into())
        )
        .configure(routes::document::services)
        .configure(routes::index::services)
        .configure(routes::search::services)
        .configure(routes::settings::services)
        .configure(routes::stop_words::services)
        .configure(routes::synonym::services)
        .configure(routes::health::services)
        .configure(routes::stats::services)
        .configure(routes::key::services);
        //.configure(routes::dump::services);
    if enable_frontend {
        app
            .service(routes::load_html)
            .service(routes::load_css)
    } else {
        app
    }
}

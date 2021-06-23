pub mod data;
#[macro_use]
pub mod error;
pub mod extractors;
pub mod helpers;
mod index;
mod index_controller;
pub mod option;
pub mod routes;

#[cfg(all(not(debug_assertions), feature = "analytics"))]
pub mod analytics;

pub use self::data::Data;
pub use option::Opt;

use actix_web::web;

use extractors::payload::PayloadConfig;

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

    mod generated {
        include!(concat!(env!("OUT_DIR"), "/generated.rs"));
    }

    if enable_frontend {
        let generated = generated::generate();
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
        use meilisearch_http::{configure_data, dashboard};

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
            .wrap(middleware::NormalizePath::new(
                middleware::TrailingSlash::Trim,
            ))
    }};
}

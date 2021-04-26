pub mod data;
pub mod error;
pub mod helpers;
mod index;
mod index_controller;
pub mod option;
pub mod routes;

pub use self::data::Data;
pub use option::Opt;

#[macro_export]
macro_rules! create_app {
    ($data:expr, $enable_frontend:expr) => {{
        use actix_cors::Cors;
        use actix_web::middleware::TrailingSlash;
        use actix_web::App;
        use actix_web::{middleware, web};
        use meilisearch_http::error::payload_error_handler;
        use meilisearch_http::routes::*;

        #[cfg(feature = "mini-dashboard")]
        use actix_web_static_files::ResourceFiles;

        #[cfg(feature = "mini-dashboard")]
        mod dashboard {
            include!(concat!(env!("OUT_DIR"), "/generated.rs"));
        }

        let app = App::new()
            .data($data.clone())
            .app_data(
                web::JsonConfig::default()
                    .limit($data.http_payload_size_limit())
                    .content_type(|_mime| true) // Accept all mime types
                    .error_handler(|err, _req| payload_error_handler(err).into()),
            )
            .app_data(
                web::QueryConfig::default()
                    .error_handler(|err, _req| payload_error_handler(err).into()),
            )
            .configure(document::services)
            .configure(index::services)
            .configure(search::services)
            .configure(settings::services)
            .configure(synonym::services)
            .configure(health::services)
            .configure(stats::services)
            .configure(key::services);
        //.configure(routes::dump::services);
        #[cfg(feature = "mini-dashboard")]
        let app = if $enable_frontend {
            let generated = dashboard::generate();
            let service = ResourceFiles::new("/", generated);
            app.service(service)
        } else {
            app.service(running)
        };

        #[cfg(not(feature = "mini-dashboard"))]
        let app = app.service(running);

        app.wrap(
            Cors::default()
            .send_wildcard()
            .allowed_headers(vec!["content-type", "x-meili-api-key"])
            .allow_any_origin()
            .allow_any_method()
            .max_age(86_400) // 24h
        )
        .wrap(middleware::Logger::default())
        .wrap(middleware::Compress::default())
        .wrap(middleware::NormalizePath::new(TrailingSlash::Trim))
    }};
}

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

pub use self::data::Data;
pub use option::Opt;

#[macro_export]
macro_rules! create_app {
    ($data:expr, $enable_frontend:expr) => {
        {
            use actix_cors::Cors;
            use actix_web::middleware::TrailingSlash;
            use actix_web::{App, HttpResponse};
            use actix_web::{middleware, web};
            use meilisearch_http::error::payload_error_handler;
            use meilisearch_http::routes::*;

            #[cfg(feature = "mini-dashboard")]
            use actix_web_static_files::Resource;

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
                .configure(health::services)
                .configure(stats::services)
                .configure(key::services)
                .configure(dump::services);
            #[cfg(feature = "mini-dashboard")]
            let app = if $enable_frontend {
                let mut app = app;
                let generated = dashboard::generate();
                let mut scope = web::scope("/");
                // Generate routes for mini-dashboard assets
                for (path, resource) in generated.into_iter() {
                    let Resource {mime_type, data, ..} = resource;
                    // Redirect index.html to /
                    if path == "index.html" {
                        app = app.service(web::resource("/").route(web::get().to(move || {
                            HttpResponse::Ok().content_type(mime_type).body(data)
                        })));
                    } else {
                        scope = scope.service(web::resource(path).route(web::get().to(move || {
                            HttpResponse::Ok().content_type(mime_type).body(data)
                        })));
                    }
                }
                app.service(scope)
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
                .max_age(86_400), // 24h
            )
                .wrap(middleware::Logger::default())
                .wrap(middleware::Compress::default())
                .wrap(middleware::NormalizePath::new(TrailingSlash::Trim))
                .default_service(
                    web::route().to(|| HttpResponse::NotFound()))
        }
    };
}

#![allow(clippy::or_fun_call)]

pub mod data;
pub mod error;
pub mod helpers;
pub mod models;
pub mod option;
pub mod routes;
pub mod analytics;
pub mod snapshot;
pub mod dump;

use actix_http::Error;
use actix_service::ServiceFactory;
use actix_web::{dev, web, App};
use chrono::Utc;
use log::error;

use meilisearch_core::{Index, MainWriter, ProcessedUpdateResult};

pub use option::Opt;
pub use self::data::Data;
use self::error::{payload_error_handler, ResponseError};

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
                .limit(data.http_payload_size_limit)
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
        .configure(routes::setting::services)
        .configure(routes::stop_words::services)
        .configure(routes::synonym::services)
        .configure(routes::health::services)
        .configure(routes::stats::services)
        .configure(routes::key::services)
        .configure(routes::dump::services);
    if enable_frontend {
        app
            .service(routes::load_html)
            .service(routes::load_css)
    } else {
        app
    }
}

pub fn index_update_callback_txn(index: Index, index_uid: &str, data: &Data, mut writer: &mut MainWriter) -> Result<(), String> {
    if let Err(e) = data.db.compute_stats(&mut writer, index_uid) {
        return Err(format!("Impossible to compute stats; {}", e));
    }

    if let Err(e) = data.db.set_last_update(&mut writer, &Utc::now()) {
        return Err(format!("Impossible to update last_update; {}", e));
    }

    if let Err(e) = index.main.put_updated_at(&mut writer) {
        return Err(format!("Impossible to update updated_at; {}", e));
    }

    Ok(())
}

pub fn index_update_callback(index_uid: &str, data: &Data, status: ProcessedUpdateResult) {
    if status.error.is_some() {
        return;
    }

    if let Some(index) = data.db.open_index(index_uid) {
        let db = &data.db;
        let res = db.main_write::<_, _, ResponseError>(|mut writer| {
            if let Err(e) = index_update_callback_txn(index, index_uid, data, &mut writer) {
                error!("{}", e);
            }

            Ok(())
        });
        match res {
            Ok(_) => (),
            Err(e) => error!("{}", e),
        }
    }
}

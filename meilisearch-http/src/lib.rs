#![allow(clippy::or_fun_call)]

pub mod data;
pub mod error;
pub mod helpers;
pub mod models;
pub mod option;
pub mod routes;

pub use self::data::Data;
use actix_http::Error;
use actix_service::ServiceFactory;
use actix_web::{dev, web, App};
use log::error;
use meilisearch_core::ProcessedUpdateResult;

pub fn create_app(
    data: &Data,
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
    App::new()
        .app_data(web::Data::new(data.clone()))
        .app_data(web::JsonConfig::default().limit(1024 * 1024 * 10)) // Json Limit of 10Mb
        .service(routes::load_html)
        .service(routes::load_css)
        .configure(routes::document::services)
        .configure(routes::index::services)
        .configure(routes::search::services)
        .configure(routes::setting::services)
        .configure(routes::stop_words::services)
        .configure(routes::synonym::services)
        .configure(routes::health::services)
        .configure(routes::stats::services)
        .configure(routes::key::services)
}

pub fn index_update_callback(index_uid: &str, data: &Data, status: ProcessedUpdateResult) {
    if status.error.is_some() {
        return;
    }

    if let Some(index) = data.db.open_index(&index_uid) {
        let db = &data.db;
        let mut writer = match db.main_write_txn() {
            Ok(writer) => writer,
            Err(e) => {
                error!("Impossible to get write_txn; {}", e);
                return;
            }
        };

        if let Err(e) = data.compute_stats(&mut writer, &index_uid) {
            error!("Impossible to compute stats; {}", e)
        }

        if let Err(e) = data.set_last_update(&mut writer) {
            error!("Impossible to update last_update; {}", e)
        }

        if let Err(e) = index.main.put_updated_at(&mut writer) {
            error!("Impossible to update updated_at; {}", e)
        }

        if let Err(e) = writer.commit() {
            error!("Impossible to get write_txn; {}", e);
        }
    }
}

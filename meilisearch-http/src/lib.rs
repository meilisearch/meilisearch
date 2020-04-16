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
        .wrap(helpers::Authentication::Public)
        .service(routes::load_html)
        .service(routes::load_css)
        .service(routes::search::search_with_url_query)
        .service(routes::search::search_multi_index)
        .service(routes::document::get_document)
        .service(routes::document::get_all_documents)
        .wrap(helpers::Authentication::Private)
        .service(routes::index::list_indexes)
        .service(routes::index::get_index)
        .service(routes::index::create_index)
        .service(routes::index::update_index)
        .service(routes::index::delete_index)
        .service(routes::index::get_update_status)
        .service(routes::index::get_all_updates_status)
        .service(routes::document::delete_document)
        .service(routes::document::add_documents)
        .service(routes::document::update_documents)
        .service(routes::document::delete_documents)
        .service(routes::document::clear_all_documents)
        .service(routes::setting::update_all)
        .service(routes::setting::get_all)
        .service(routes::setting::delete_all)
        .service(routes::setting::get_rules)
        .service(routes::setting::update_rules)
        .service(routes::setting::delete_rules)
        .service(routes::setting::get_distinct)
        .service(routes::setting::update_distinct)
        .service(routes::setting::delete_distinct)
        .service(routes::setting::get_searchable)
        .service(routes::setting::update_searchable)
        .service(routes::setting::delete_searchable)
        .service(routes::setting::get_displayed)
        .service(routes::setting::update_displayed)
        .service(routes::setting::delete_displayed)
        .service(routes::setting::get_accept_new_fields)
        .service(routes::setting::update_accept_new_fields)
        .service(routes::stop_words::get)
        .service(routes::stop_words::update)
        .service(routes::stop_words::delete)
        .service(routes::synonym::get)
        .service(routes::synonym::update)
        .service(routes::synonym::delete)
        .service(routes::stats::index_stats)
        .service(routes::stats::get_stats)
        .service(routes::stats::get_version)
        .service(routes::stats::get_sys_info)
        .service(routes::stats::get_sys_info_pretty)
        .service(routes::health::get_health)
        .service(routes::health::change_healthyness)
        .wrap(helpers::Authentication::Admin)
        .service(routes::key::list)
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

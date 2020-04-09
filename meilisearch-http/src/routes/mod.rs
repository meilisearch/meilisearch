use actix_web::{get, HttpResponse};
use serde::{Serialize, Deserialize};
use log::error;
use meilisearch_core::ProcessedUpdateResult;

use crate::Data;

pub mod document;
pub mod health;
pub mod index;
pub mod key;
pub mod search;
pub mod stats;
pub mod setting;
// pub mod stop_words;
// pub mod synonym;

#[derive(Default, Deserialize)]
pub struct IndexParam {
    index_uid: String
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

impl IndexUpdateResponse {
    pub fn with_id(update_id: u64) -> Self {
        Self {
            update_id,
        }
    }
}

#[get("/")]
pub async fn load_html() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../../public/interface.html").to_string())
}

#[get("/bulma.min.css")]
pub async fn load_css() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/css; charset=utf-8")
        .body(include_str!("../../public/bulma.min.css").to_string())
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


// pub fn load_routes(app: &mut tide::Server<Data>) {
//     app.at("/").get(|_| async {
//         tide::Response::new(200)
//             .body_string()
//             .set_mime(mime::TEXT_HTML_UTF_8)
//     });
//     app.at("/bulma.min.css").get(|_| async {
//         tide::Response::new(200)
//             .body_string(include_str!("../../public/bulma.min.css").to_string())
//             .set_mime(mime::TEXT_CSS_UTF_8)
//     });

//     app.at("/indexes")
//         .get(|ctx| into_response(index::list_indexes(ctx)))
//         .post(|ctx| into_response(index::create_index(ctx)));

//     app.at("/indexes/search")
//         .post(|ctx| into_response(search::search_multi_index(ctx)));

//     app.at("/indexes/:index")
//         .get(|ctx| into_response(index::get_index(ctx)))
//         .put(|ctx| into_response(index::update_index(ctx)))
//         .delete(|ctx| into_response(index::delete_index(ctx)));

//     app.at("/indexes/:index/search")
//         .get(|ctx| into_response(search::search_with_url_query(ctx)));

//     app.at("/indexes/:index/updates")
//         .get(|ctx| into_response(index::get_all_updates_status(ctx)));

//     app.at("/indexes/:index/updates/:update_id")
//         .get(|ctx| into_response(index::get_update_status(ctx)));

//     app.at("/indexes/:index/documents")
//         .get(|ctx| into_response(document::get_all_documents(ctx)))
//         .post(|ctx| into_response(document::add_or_replace_multiple_documents(ctx)))
//         .put(|ctx| into_response(document::add_or_update_multiple_documents(ctx)))
//         .delete(|ctx| into_response(document::clear_all_documents(ctx)));

//     app.at("/indexes/:index/documents/:document_id")
//         .get(|ctx| into_response(document::get_document(ctx)))
//         .delete(|ctx| into_response(document::delete_document(ctx)));

//     app.at("/indexes/:index/documents/delete-batch")
//         .post(|ctx| into_response(document::delete_multiple_documents(ctx)));

//     app.at("/indexes/:index/settings")
//         .get(|ctx| into_response(setting::get_all(ctx)))
//         .post(|ctx| into_response(setting::update_all(ctx)))
//         .delete(|ctx| into_response(setting::delete_all(ctx)));

//     app.at("/indexes/:index/settings/ranking-rules")
//         .get(|ctx| into_response(setting::get_rules(ctx)))
//         .post(|ctx| into_response(setting::update_rules(ctx)))
//         .delete(|ctx| into_response(setting::delete_rules(ctx)));

//     app.at("/indexes/:index/settings/distinct-attribute")
//         .get(|ctx| into_response(setting::get_distinct(ctx)))
//         .post(|ctx| into_response(setting::update_distinct(ctx)))
//         .delete(|ctx| into_response(setting::delete_distinct(ctx)));

//     app.at("/indexes/:index/settings/searchable-attributes")
//         .get(|ctx| into_response(setting::get_searchable(ctx)))
//         .post(|ctx| into_response(setting::update_searchable(ctx)))
//         .delete(|ctx| into_response(setting::delete_searchable(ctx)));

//     app.at("/indexes/:index/settings/displayed-attributes")
//         .get(|ctx| into_response(setting::displayed(ctx)))
//         .post(|ctx| into_response(setting::update_displayed(ctx)))
//         .delete(|ctx| into_response(setting::delete_displayed(ctx)));

//     app.at("/indexes/:index/settings/accept-new-fields")
//         .get(|ctx| into_response(setting::get_accept_new_fields(ctx)))
//         .post(|ctx| into_response(setting::update_accept_new_fields(ctx)));

//     app.at("/indexes/:index/settings/synonyms")
//         .get(|ctx| into_response(synonym::get(ctx)))
//         .post(|ctx| into_response(synonym::update(ctx)))
//         .delete(|ctx| into_response(synonym::delete(ctx)));

//     app.at("/indexes/:index/settings/stop-words")
//         .get(|ctx| into_response(stop_words::get(ctx)))
//         .post(|ctx| into_response(stop_words::update(ctx)))
//         .delete(|ctx| into_response(stop_words::delete(ctx)));

//     app.at("/indexes/:index/stats")
//         .get(|ctx| into_response(stats::index_stats(ctx)));

//     app.at("/keys").get(|ctx| into_response(key::list(ctx)));

//     app.at("/health")
//         .get(|ctx| into_response(health::get_health(ctx)))
//         .put(|ctx| into_response(health::change_healthyness(ctx)));

//     app.at("/stats")
//         .get(|ctx| into_response(stats::get_stats(ctx)));

//     app.at("/version")
//         .get(|ctx| into_response(stats::get_version(ctx)));

//     app.at("/sys-info")
//         .get(|ctx| into_response(stats::get_sys_info(ctx)));

//     app.at("/sys-info/pretty")
//         .get(|ctx| into_response(stats::get_sys_info_pretty(ctx)));
// }

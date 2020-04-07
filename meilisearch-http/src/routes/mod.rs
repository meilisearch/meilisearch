
use serde::Serialize;

pub mod document;
// pub mod health;
// pub mod index;
// pub mod key;
// pub mod search;
// pub mod setting;
// pub mod stats;
// pub mod stop_words;
// pub mod synonym;

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
    pub see_more: String,
}

impl IndexUpdateResponse {
    pub fn with_id(update_id: u64) -> Self {
        Self {
            update_id,
            see_more: "https://docs.meilisearch.com/guides/advanced_guides/asynchronous_updates.html".to_string()
        }
    }
}

// pub fn load_routes(app: &mut tide::Server<Data>) {
//     app.at("/").get(|_| async {
//         tide::Response::new(200)
//             .body_string(include_str!("../../public/interface.html").to_string())
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

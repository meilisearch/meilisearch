use crate::data::Data;
use std::future::Future;
use tide::IntoResponse;
use tide::Response;

pub mod document;
pub mod health;
pub mod index;
pub mod key;
pub mod search;
pub mod setting;
pub mod stats;
pub mod stop_words;
pub mod synonym;

async fn into_response<T: IntoResponse, U: IntoResponse>(
    x: impl Future<Output = Result<T, U>>,
) -> Response {
    match x.await {
        Ok(resp) => resp.into_response(),
        Err(resp) => resp.into_response(),
    }
}

pub fn load_routes(app: &mut tide::Server<Data>) {
    app.at("").nest(|router| {
        // expose the web interface static files
        router.at("/").get(|_| {
            async move {
                let response = include_str!("../../public/interface.html");
                response
            }
        });
        router.at("/bulma.min.css").get(|_| {
            async {
                let response = include_str!("../../public/bulma.min.css");
                response
            }
        });

        router.at("/indexes").nest(|router| {
            router
                .at("/")
                .get(|ctx| into_response(index::list_indexes(ctx)))
                .post(|ctx| into_response(index::create_index(ctx)));

            router
                .at("/search")
                .post(|ctx| into_response(search::search_multi_index(ctx)));

            router.at("/:index").nest(|router| {
                router
                    .at("/search")
                    .get(|ctx| into_response(search::search_with_url_query(ctx)));

                router.at("/updates").nest(|router| {
                    router
                        .at("/")
                        .get(|ctx| into_response(index::get_all_updates_status(ctx)));

                    router
                        .at("/:update_id")
                        .get(|ctx| into_response(index::get_update_status(ctx)));
                });

                router
                    .at("/")
                    .get(|ctx| into_response(index::get_index(ctx)))
                    .put(|ctx| into_response(index::update_index(ctx)))
                    .delete(|ctx| into_response(index::delete_index(ctx)));

                router.at("/documents").nest(|router| {
                    router
                        .at("/")
                        .get(|ctx| into_response(document::get_all_documents(ctx)))
                        .post(|ctx| into_response(document::add_or_replace_multiple_documents(ctx)))
                        .put(|ctx| into_response(document::add_or_update_multiple_documents(ctx)))
                        .delete(|ctx| into_response(document::clear_all_documents(ctx)));

                    router.at("/:identifier").nest(|router| {
                        router
                            .at("/")
                            .get(|ctx| into_response(document::get_document(ctx)))
                            .delete(|ctx| into_response(document::delete_document(ctx)));
                    });

                    router
                        .at("/delete-batch")
                        .post(|ctx| into_response(document::delete_multiple_documents(ctx)));
                });

                router.at("/settings").nest(|router| {
                    router
                        .get(|ctx| into_response(setting::get_all(ctx)))
                        .post(|ctx| into_response(setting::update_all(ctx)))
                        .delete(|ctx| into_response(setting::delete_all(ctx)));

                    router.at("/ranking").nest(|router| {
                        router
                            .get(|ctx| into_response(setting::get_ranking(ctx)))
                            .post(|ctx| into_response(setting::update_ranking(ctx)))
                            .delete(|ctx| into_response(setting::delete_ranking(ctx)));

                        router
                            .at("/rules")
                            .get(|ctx| into_response(setting::get_rules(ctx)))
                            .post(|ctx| into_response(setting::update_rules(ctx)))
                            .delete(|ctx| into_response(setting::delete_rules(ctx)));

                        router
                            .at("/distinct")
                            .get(|ctx| into_response(setting::get_distinct(ctx)))
                            .post(|ctx| into_response(setting::update_distinct(ctx)))
                            .delete(|ctx| into_response(setting::delete_distinct(ctx)));
                    });

                    router.at("/attributes").nest(|router| {
                        router
                            .get(|ctx| into_response(setting::get_attributes(ctx)))
                            .post(|ctx| into_response(setting::update_attributes(ctx)))
                            .delete(|ctx| into_response(setting::delete_attributes(ctx)));

                        router
                            .at("/identifier")
                            .get(|ctx| into_response(setting::get_identifier(ctx)));

                        router
                            .at("/searchable")
                            .get(|ctx| into_response(setting::get_searchable(ctx)))
                            .post(|ctx| into_response(setting::update_searchable(ctx)))
                            .delete(|ctx| into_response(setting::delete_searchable(ctx)));

                        router
                            .at("/displayed")
                            .get(|ctx| into_response(setting::get_displayed(ctx)))
                            .post(|ctx| into_response(setting::update_displayed(ctx)))
                            .delete(|ctx| into_response(setting::delete_displayed(ctx)));
                    });
                    router.at("/index-new-fields")
                            .get(|ctx| into_response(setting::get_index_new_fields(ctx)))
                            .post(|ctx| into_response(setting::update_index_new_fields(ctx)));

                    router
                        .at("/synonyms")
                        .get(|ctx| into_response(synonym::get(ctx)))
                        .post(|ctx| into_response(synonym::update(ctx)))
                        .delete(|ctx| into_response(synonym::delete(ctx)));

                    router
                        .at("/stop-words")
                        .get(|ctx| into_response(stop_words::get(ctx)))
                        .post(|ctx| into_response(stop_words::update(ctx)))
                        .delete(|ctx| into_response(stop_words::delete(ctx)));
                });

                router
                    .at("/stats")
                    .get(|ctx| into_response(stats::index_stat(ctx)));
            });
        });

        router.at("/keys").nest(|router| {
            router
                .at("/")
                .get(|ctx| into_response(key::list(ctx)))
                .post(|ctx| into_response(key::create(ctx)));

            router
                .at("/:key")
                .get(|ctx| into_response(key::get(ctx)))
                .put(|ctx| into_response(key::update(ctx)))
                .delete(|ctx| into_response(key::delete(ctx)));
        });
    });

    app.at("").nest(|router| {
        router
            .at("/health")
            .get(|ctx| into_response(health::get_health(ctx)))
            .put(|ctx| into_response(health::change_healthyness(ctx)));

        router
            .at("/stats")
            .get(|ctx| into_response(stats::get_stats(ctx)));
        router
            .at("/version")
            .get(|ctx| into_response(stats::get_version(ctx)));
        router
            .at("/sys-info")
            .get(|ctx| into_response(stats::get_sys_info(ctx)));
        router
            .at("/sys-info/pretty")
            .get(|ctx| into_response(stats::get_sys_info_pretty(ctx)));
    });
}

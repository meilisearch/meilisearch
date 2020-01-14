use crate::data::Data;

pub mod document;
pub mod health;
pub mod index;
pub mod key;
pub mod search;
pub mod setting;
pub mod stats;
pub mod stop_words;
pub mod synonym;

pub fn load_routes(app: &mut tide::App<Data>) {
    app.at("").nest(|router| {
        // expose the web interface static files
        router.at("/").get(|_| async {
            let content = include_str!("../../public/interface.html").to_owned();
            tide::http::Response::builder()
                .header(tide::http::header::CONTENT_TYPE, "text/html; charset=utf-8")
                .status(tide::http::StatusCode::OK)
                .body(content).unwrap()
        });
        router.at("/bulma.min.css").get(|_| async {
            let content = include_str!("../../public/bulma.min.css");
            tide::http::Response::builder()
                .header(tide::http::header::CONTENT_TYPE, "text/css; charset=utf-8")
                .status(tide::http::StatusCode::OK)
                .body(content).unwrap()
        });

        router.at("/indexes").nest(|router| {
            router
                .at("/")
                .get(index::list_indexes)
                .post(index::create_index);

            router.at("/search").post(search::search_multi_index);

            router.at("/:index").nest(|router| {
                router.at("/search").get(search::search_with_url_query);

                router.at("/updates").nest(|router| {
                    router.at("/").get(index::get_all_updates_status);

                    router.at("/:update_id").get(index::get_update_status);
                });

                router
                    .at("/")
                    .get(index::get_index)
                    .put(index::update_index)
                    .delete(index::delete_index);

                // router
                //     .at("/schema")
                //     .get(index::get_index_schema)
                //     .put(index::update_schema);

                router.at("/documents").nest(|router| {
                    router
                        .at("/")
                        .get(document::get_all_documents)
                        .post(document::add_or_replace_multiple_documents)
                        .put(document::add_or_update_multiple_documents)
                        .delete(document::clear_all_documents);

                    router.at("/:identifier").nest(|router| {
                        router
                            .at("/")
                            .get(document::get_document)
                            .delete(document::delete_document);
                    });

                    router
                        .at("/delete-batch")
                        .post(document::delete_multiple_documents);
                });

                router.at("/settings").nest(|router| {
                    router.at("/synonyms")
                        .get(synonym::get)
                        .post(synonym::update)
                        .delete(synonym::delete);

                    router.at("/stop-words")
                        .get(stop_words::get)
                        .post(stop_words::update)
                        .delete(stop_words::delete);
                })
                .get(setting::get)
                .post(setting::update);


                router.at("/stats").get(stats::index_stat);
            });
        });

        router.at("/keys").nest(|router| {
            router.at("/").get(key::list).post(key::create);

            router
                .at("/:key")
                .get(key::get)
                .put(key::update)
                .delete(key::delete);
        });
    });

    app.at("").nest(|router| {
        router
            .at("/health")
            .get(health::get_health)
            .put(health::change_healthyness);

        router.at("/stats").get(stats::get_stats);
        router.at("/version").get(stats::get_version);
        router.at("/sys-info").get(stats::get_sys_info);
        router
            .at("/sys-info/pretty")
            .get(stats::get_sys_info_pretty);
    });
}

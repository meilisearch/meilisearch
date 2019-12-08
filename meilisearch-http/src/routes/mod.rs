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

                router
                    .at("/schema")
                    .get(index::get_index_schema)
                    .put(index::update_schema);

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
                        .at("/delete")
                        .post(document::delete_multiple_documents);
                });

                router.at("/synonyms").nest(|router| {
                    router
                        .at("/")
                        .get(synonym::list)
                        .post(synonym::create)
                        .delete(synonym::clear);

                    router
                        .at("/:synonym")
                        .get(synonym::get)
                        .put(synonym::update)
                        .delete(synonym::delete);

                    router.at("/batch").post(synonym::batch_write);
                });

                router.at("/stop-words").nest(|router| {
                    router
                        .at("/")
                        .get(stop_words::list)
                        .patch(stop_words::add)
                        .post(stop_words::delete);
                });

                router
                    .at("/settings")
                    .get(setting::get)
                    .post(setting::update);
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
        router.at("/stats/:index").get(stats::index_stat);
        router.at("/version").get(stats::get_version);
        router.at("/sys-info").get(stats::get_sys_info);
        router
            .at("/sys-info/pretty")
            .get(stats::get_sys_info_pretty);
    });
}

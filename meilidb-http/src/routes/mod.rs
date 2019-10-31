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
            router.at("/").get(index::list_indexes);

            router.at("/search").post(search::search_multi_index);

            router.at("/:index").nest(|router| {
                router.at("/search").get(search::search_with_url_query);

                router.at("/updates").nest(|router| {
                    router.at("/").get(index::get_all_updates_status);

                    router.at("/:update_id").get(index::get_update_status);
                });

                router
                    .at("/")
                    .get(index::get_index_schema)
                    .post(index::create_index)
                    .put(index::update_schema)
                    .delete(index::delete_index);

                router.at("/documents").nest(|router| {
                    router
                        .at("/")
                        .get(document::browse_documents)
                        .post(document::add_or_update_multiple_documents)
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

                router.at("/synonym").nest(|router| {
                    router.at("/").get(synonym::list).post(synonym::create);

                    router
                        .at("/:synonym")
                        .get(synonym::get)
                        .put(synonym::update)
                        .delete(synonym::delete);

                    router.at("/batch").post(synonym::batch_write);
                    router.at("/clear").post(synonym::clear);
                });

                router.at("/stop-words").nest(|router| {
                    router
                        .at("/")
                        .get(stop_words::list)
                        .put(stop_words::add)
                        .delete(stop_words::delete);
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

    // Private
    app.at("").nest(|router| {
        router
            .at("/health")
            .get(health::get_health)
            .post(health::set_healthy)
            .put(health::change_healthyness)
            .delete(health::set_unhealthy);

        router.at("/stats").get(stats::get_stats);
        router.at("/stats/:index").get(stats::index_stat);
        router.at("/version").get(stats::get_version);
        router.at("/sys-info").get(stats::get_sys_info);
        router
            .at("/sys-info/pretty")
            .get(stats::get_sys_info_pretty);
    });
}

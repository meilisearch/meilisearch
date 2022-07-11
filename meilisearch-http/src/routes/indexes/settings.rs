use log::debug;

use actix_web::{web, HttpRequest, HttpResponse};
use meilisearch_lib::index::{Settings, Unchecked};
use meilisearch_lib::index_controller::Update;
use meilisearch_lib::MeiliSearch;
use meilisearch_types::error::ResponseError;
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::task::SummarizedTaskView;

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $update_verb:ident, $type:ty, $attr:ident, $camelcase_attr:literal, $analytics_var:ident, $analytics:expr) => {
        pub mod $attr {
            use actix_web::{web, HttpRequest, HttpResponse, Resource};
            use log::debug;

            use meilisearch_lib::milli::update::Setting;
            use meilisearch_lib::{index::Settings, index_controller::Update, MeiliSearch};

            use meilisearch_types::error::ResponseError;
            use $crate::analytics::Analytics;
            use $crate::extractors::authentication::{policies::*, GuardedData};
            use $crate::extractors::sequential_extractor::SeqHandler;
            use $crate::task::SummarizedTaskView;

            pub async fn delete(
                meilisearch: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, MeiliSearch>,
                index_uid: web::Path<String>,
            ) -> Result<HttpResponse, ResponseError> {
                let settings = Settings {
                    $attr: Setting::Reset,
                    ..Default::default()
                };

                let allow_index_creation = meilisearch.filters().allow_index_creation;
                let update = Update::Settings {
                    settings,
                    is_deletion: true,
                    allow_index_creation,
                };
                let task: SummarizedTaskView = meilisearch
                    .register_update(index_uid.into_inner(), update)
                    .await?
                    .into();

                debug!("returns: {:?}", task);
                Ok(HttpResponse::Accepted().json(task))
            }

            pub async fn update(
                meilisearch: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, MeiliSearch>,
                index_uid: actix_web::web::Path<String>,
                body: actix_web::web::Json<Option<$type>>,
                req: HttpRequest,
                $analytics_var: web::Data<dyn Analytics>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let body = body.into_inner();

                $analytics(&body, &req);

                let settings = Settings {
                    $attr: match body {
                        Some(inner_body) => Setting::Set(inner_body),
                        None => Setting::Reset,
                    },
                    ..Default::default()
                };

                let allow_index_creation = meilisearch.filters().allow_index_creation;
                let update = Update::Settings {
                    settings,
                    is_deletion: false,
                    allow_index_creation,
                };
                let task: SummarizedTaskView = meilisearch
                    .register_update(index_uid.into_inner(), update)
                    .await?
                    .into();

                debug!("returns: {:?}", task);
                Ok(HttpResponse::Accepted().json(task))
            }

            pub async fn get(
                meilisearch: GuardedData<ActionPolicy<{ actions::SETTINGS_GET }>, MeiliSearch>,
                index_uid: actix_web::web::Path<String>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let settings = meilisearch.settings(index_uid.into_inner()).await?;
                debug!("returns: {:?}", settings);
                let mut json = serde_json::json!(&settings);
                let val = json[$camelcase_attr].take();

                Ok(HttpResponse::Ok().json(val))
            }

            pub fn resources() -> Resource {
                Resource::new($route)
                    .route(web::get().to(SeqHandler(get)))
                    .route(web::$update_verb().to(SeqHandler(update)))
                    .route(web::delete().to(SeqHandler(delete)))
            }
        }
    };
    ($route:literal, $update_verb:ident, $type:ty, $attr:ident, $camelcase_attr:literal) => {
        make_setting_route!(
            $route,
            $update_verb,
            $type,
            $attr,
            $camelcase_attr,
            _analytics,
            |_, _| {}
        );
    };
}

make_setting_route!(
    "/filterable-attributes",
    put,
    std::collections::BTreeSet<String>,
    filterable_attributes,
    "filterableAttributes",
    analytics,
    |setting: &Option<std::collections::BTreeSet<String>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "FilterableAttributes Updated".to_string(),
            json!({
                "filterable_attributes": {
                    "total": setting.as_ref().map(|filter| filter.len()).unwrap_or(0),
                    "has_geo": setting.as_ref().map(|filter| filter.contains("_geo")).unwrap_or(false),
                }
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/sortable-attributes",
    put,
    std::collections::BTreeSet<String>,
    sortable_attributes,
    "sortableAttributes",
    analytics,
    |setting: &Option<std::collections::BTreeSet<String>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "SortableAttributes Updated".to_string(),
            json!({
                "sortable_attributes": {
                    "total": setting.as_ref().map(|sort| sort.len()),
                    "has_geo": setting.as_ref().map(|sort| sort.contains("_geo")),
                },
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/displayed-attributes",
    put,
    Vec<String>,
    displayed_attributes,
    "displayedAttributes"
);

make_setting_route!(
    "/typo-tolerance",
    patch,
    meilisearch_lib::index::updates::TypoSettings,
    typo_tolerance,
    "typoTolerance",
    analytics,
    |setting: &Option<meilisearch_lib::index::updates::TypoSettings>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "TypoTolerance Updated".to_string(),
            json!({
                "typo_tolerance": {
                    "enabled": setting.as_ref().map(|s| !matches!(s.enabled, Setting::Set(false))),
                    "disable_on_attributes": setting
                        .as_ref()
                        .and_then(|s| s.disable_on_attributes.as_ref().set().map(|m| !m.is_empty())),
                    "disable_on_words": setting
                        .as_ref()
                        .and_then(|s| s.disable_on_words.as_ref().set().map(|m| !m.is_empty())),
                    "min_word_size_for_one_typo": setting
                        .as_ref()
                        .and_then(|s| s.min_word_size_for_typos
                            .as_ref()
                            .set()
                            .map(|s| s.one_typo.set()))
                        .flatten(),
                    "min_word_size_for_two_typos": setting
                        .as_ref()
                        .and_then(|s| s.min_word_size_for_typos
                            .as_ref()
                            .set()
                            .map(|s| s.two_typos.set()))
                        .flatten(),
                },
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/searchable-attributes",
    put,
    Vec<String>,
    searchable_attributes,
    "searchableAttributes",
    analytics,
    |setting: &Option<Vec<String>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "SearchableAttributes Updated".to_string(),
            json!({
                "searchable_attributes": {
                    "total": setting.as_ref().map(|searchable| searchable.len()),
                },
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/stop-words",
    put,
    std::collections::BTreeSet<String>,
    stop_words,
    "stopWords"
);

make_setting_route!(
    "/synonyms",
    put,
    std::collections::BTreeMap<String, Vec<String>>,
    synonyms,
    "synonyms"
);

make_setting_route!(
    "/distinct-attribute",
    put,
    String,
    distinct_attribute,
    "distinctAttribute"
);

make_setting_route!(
    "/ranking-rules",
    put,
    Vec<String>,
    ranking_rules,
    "rankingRules",
    analytics,
    |setting: &Option<Vec<String>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "RankingRules Updated".to_string(),
            json!({
                "ranking_rules": {
                    "sort_position": setting.as_ref().map(|sort| sort.iter().position(|s| s == "sort")),
                }
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/faceting",
    patch,
    meilisearch_lib::index::updates::FacetingSettings,
    faceting,
    "faceting",
    analytics,
    |setting: &Option<meilisearch_lib::index::updates::FacetingSettings>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "Faceting Updated".to_string(),
            json!({
                "faceting": {
                    "max_values_per_facet": setting.as_ref().and_then(|s| s.max_values_per_facet.set()),
                },
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/pagination",
    patch,
    meilisearch_lib::index::updates::PaginationSettings,
    pagination,
    "pagination",
    analytics,
    |setting: &Option<meilisearch_lib::index::updates::PaginationSettings>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "Pagination Updated".to_string(),
            json!({
                "pagination": {
                    "max_total_hits": setting.as_ref().and_then(|s| s.max_total_hits.set()),
                },
            }),
            Some(req),
        );
    }
);

macro_rules! generate_configure {
    ($($mod:ident),*) => {
        pub fn configure(cfg: &mut web::ServiceConfig) {
            use crate::extractors::sequential_extractor::SeqHandler;
            cfg.service(
                web::resource("")
                .route(web::patch().to(SeqHandler(update_all)))
                .route(web::get().to(SeqHandler(get_all)))
                .route(web::delete().to(SeqHandler(delete_all))))
                $(.service($mod::resources()))*;
        }
    };
}

generate_configure!(
    filterable_attributes,
    sortable_attributes,
    displayed_attributes,
    searchable_attributes,
    distinct_attribute,
    stop_words,
    synonyms,
    ranking_rules,
    typo_tolerance,
    pagination,
    faceting
);

pub async fn update_all(
    meilisearch: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, MeiliSearch>,
    index_uid: web::Path<String>,
    body: web::Json<Settings<Unchecked>>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let settings = body.into_inner();

    analytics.publish(
        "Settings Updated".to_string(),
        json!({
           "ranking_rules": {
                "sort_position": settings.ranking_rules.as_ref().set().map(|sort| sort.iter().position(|s| s == "sort")),
            },
            "searchable_attributes": {
                "total": settings.searchable_attributes.as_ref().set().map(|searchable| searchable.len()),
            },
           "sortable_attributes": {
                "total": settings.sortable_attributes.as_ref().set().map(|sort| sort.len()),
                "has_geo": settings.sortable_attributes.as_ref().set().map(|sort| sort.iter().any(|s| s == "_geo")),
            },
           "filterable_attributes": {
                "total": settings.filterable_attributes.as_ref().set().map(|filter| filter.len()),
                "has_geo": settings.filterable_attributes.as_ref().set().map(|filter| filter.iter().any(|s| s == "_geo")),
            },
            "typo_tolerance": {
                "enabled": settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.enabled.as_ref().set())
                    .copied(),
                "disable_on_attributes": settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.disable_on_attributes.as_ref().set().map(|m| !m.is_empty())),
                "disable_on_words": settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.disable_on_words.as_ref().set().map(|m| !m.is_empty())),
                "min_word_size_for_one_typo": settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.min_word_size_for_typos
                        .as_ref()
                        .set()
                        .map(|s| s.one_typo.set()))
                    .flatten(),
                "min_word_size_for_two_typos": settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.min_word_size_for_typos
                        .as_ref()
                        .set()
                        .map(|s| s.two_typos.set()))
                    .flatten(),
            },
            "faceting": {
                "max_values_per_facet": settings.faceting
                    .as_ref()
                    .set()
                    .and_then(|s| s.max_values_per_facet.as_ref().set()),
            },
            "pagination": {
                "max_total_hits": settings.pagination
                    .as_ref()
                    .set()
                    .and_then(|s| s.max_total_hits.as_ref().set()),
            },
        }),
        Some(&req),
    );

    let allow_index_creation = meilisearch.filters().allow_index_creation;
    let update = Update::Settings {
        settings,
        is_deletion: false,
        allow_index_creation,
    };
    let task: SummarizedTaskView = meilisearch
        .register_update(index_uid.into_inner(), update)
        .await?
        .into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn get_all(
    data: GuardedData<ActionPolicy<{ actions::SETTINGS_GET }>, MeiliSearch>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let settings = data.settings(index_uid.into_inner()).await?;
    debug!("returns: {:?}", settings);
    Ok(HttpResponse::Ok().json(settings))
}

pub async fn delete_all(
    data: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, MeiliSearch>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let settings = Settings::cleared().into_unchecked();

    let allow_index_creation = data.filters().allow_index_creation;
    let update = Update::Settings {
        settings,
        is_deletion: true,
        allow_index_creation,
    };
    let task: SummarizedTaskView = data
        .register_update(index_uid.into_inner(), update)
        .await?
        .into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

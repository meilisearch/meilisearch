use std::collections::BTreeSet;
use std::marker::PhantomData;

use actix_web::web::Data;
use fst::IntoStreamer;
use log::debug;

use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::{IndexScheduler, KindWithContent};
use meilisearch_types::error::ResponseError;
use meilisearch_types::heed::RoTxn;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::milli::{self, DEFAULT_VALUES_PER_FACET};
use meilisearch_types::settings::{
    Checked, FacetingSettings, MinWordSizeTyposSetting, PaginationSettings, Settings, TypoSettings,
    Unchecked,
};
use meilisearch_types::Index;
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::search::DEFAULT_PAGINATION_MAX_TOTAL_HITS;

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $update_verb:ident, $type:ty, $attr:ident, $camelcase_attr:literal, $analytics_var:ident, $analytics:expr) => {
        pub mod $attr {
            use actix_web::web::Data;
            use actix_web::{web, HttpRequest, HttpResponse, Resource};
            use log::debug;

            use index_scheduler::{IndexScheduler, KindWithContent};
            use meilisearch_types::milli::update::Setting;
            use meilisearch_types::settings::Settings;

            use meilisearch_types::error::ResponseError;
            use $crate::analytics::Analytics;
            use $crate::extractors::authentication::{policies::*, GuardedData};
            use $crate::extractors::sequential_extractor::SeqHandler;
            use $crate::routes::indexes::settings::settings;

            pub async fn delete(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_UPDATE }>,
                    Data<IndexScheduler>,
                >,
                index_uid: web::Path<String>,
            ) -> Result<HttpResponse, ResponseError> {
                let new_settings = Settings {
                    $attr: Setting::Reset,
                    ..Default::default()
                };

                let allow_index_creation = index_scheduler.filters().allow_index_creation;
                let task = KindWithContent::Settings {
                    index_uid: index_uid.into_inner(),
                    new_settings,
                    is_deletion: true,
                    allow_index_creation,
                };
                let task =
                    tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

                debug!("returns: {:?}", task);
                Ok(HttpResponse::Accepted().json(task))
            }

            pub async fn update(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_UPDATE }>,
                    Data<IndexScheduler>,
                >,
                index_uid: actix_web::web::Path<String>,
                body: actix_web::web::Json<Option<$type>>,
                req: HttpRequest,
                $analytics_var: web::Data<dyn Analytics>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let body = body.into_inner();

                $analytics(&body, &req);

                let new_settings = Settings {
                    $attr: match body {
                        Some(inner_body) => Setting::Set(inner_body),
                        None => Setting::Reset,
                    },
                    ..Default::default()
                };

                let allow_index_creation = index_scheduler.filters().allow_index_creation;
                let task = KindWithContent::Settings {
                    index_uid: index_uid.into_inner(),
                    new_settings,
                    is_deletion: false,
                    allow_index_creation,
                };
                let task =
                    tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

                debug!("returns: {:?}", task);
                Ok(HttpResponse::Accepted().json(task))
            }

            pub async fn get(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_GET }>,
                    Data<IndexScheduler>,
                >,
                index_uid: actix_web::web::Path<String>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let index = index_scheduler.index(&index_uid)?;
                let rtxn = index.read_txn()?;
                let settings = settings(&index, &rtxn)?;

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
    meilisearch_types::settings::TypoSettings,
    typo_tolerance,
    "typoTolerance",
    analytics,
    |setting: &Option<meilisearch_types::settings::TypoSettings>, req: &HttpRequest| {
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
    meilisearch_types::settings::FacetingSettings,
    faceting,
    "faceting",
    analytics,
    |setting: &Option<meilisearch_types::settings::FacetingSettings>, req: &HttpRequest| {
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
    meilisearch_types::settings::PaginationSettings,
    pagination,
    "pagination",
    analytics,
    |setting: &Option<meilisearch_types::settings::PaginationSettings>, req: &HttpRequest| {
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
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: web::Json<Settings<Unchecked>>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let new_settings = body.into_inner();

    analytics.publish(
        "Settings Updated".to_string(),
        json!({
           "ranking_rules": {
                "sort_position": new_settings.ranking_rules.as_ref().set().map(|sort| sort.iter().position(|s| s == "sort")),
            },
            "searchable_attributes": {
                "total": new_settings.searchable_attributes.as_ref().set().map(|searchable| searchable.len()),
            },
           "sortable_attributes": {
                "total": new_settings.sortable_attributes.as_ref().set().map(|sort| sort.len()),
                "has_geo": new_settings.sortable_attributes.as_ref().set().map(|sort| sort.iter().any(|s| s == "_geo")),
            },
           "filterable_attributes": {
                "total": new_settings.filterable_attributes.as_ref().set().map(|filter| filter.len()),
                "has_geo": new_settings.filterable_attributes.as_ref().set().map(|filter| filter.iter().any(|s| s == "_geo")),
            },
            "typo_tolerance": {
                "enabled": new_settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.enabled.as_ref().set())
                    .copied(),
                "disable_on_attributes": new_settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.disable_on_attributes.as_ref().set().map(|m| !m.is_empty())),
                "disable_on_words": new_settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.disable_on_words.as_ref().set().map(|m| !m.is_empty())),
                "min_word_size_for_one_typo": new_settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.min_word_size_for_typos
                        .as_ref()
                        .set()
                        .map(|s| s.one_typo.set()))
                    .flatten(),
                "min_word_size_for_two_typos": new_settings.typo_tolerance
                    .as_ref()
                    .set()
                    .and_then(|s| s.min_word_size_for_typos
                        .as_ref()
                        .set()
                        .map(|s| s.two_typos.set()))
                    .flatten(),
            },
            "faceting": {
                "max_values_per_facet": new_settings.faceting
                    .as_ref()
                    .set()
                    .and_then(|s| s.max_values_per_facet.as_ref().set()),
            },
            "pagination": {
                "max_total_hits": new_settings.pagination
                    .as_ref()
                    .set()
                    .and_then(|s| s.max_total_hits.as_ref().set()),
            },
        }),
        Some(&req),
    );

    let allow_index_creation = index_scheduler.filters().allow_index_creation;
    let task = KindWithContent::Settings {
        index_uid: index_uid.into_inner(),
        new_settings,
        is_deletion: false,
        allow_index_creation,
    };
    let task = tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn get_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = index_scheduler.index(&index_uid)?;
    let rtxn = index.read_txn()?;
    let new_settings = settings(&index, &rtxn)?;
    debug!("returns: {:?}", new_settings);
    Ok(HttpResponse::Ok().json(new_settings))
}

pub async fn delete_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let new_settings = Settings::cleared().into_unchecked();

    let allow_index_creation = index_scheduler.filters().allow_index_creation;
    let task = KindWithContent::Settings {
        index_uid: index_uid.into_inner(),
        new_settings,
        is_deletion: true,
        allow_index_creation,
    };
    let task = tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

pub fn settings(index: &Index, rtxn: &RoTxn) -> Result<Settings<Checked>, milli::Error> {
    let displayed_attributes = index
        .displayed_fields(rtxn)?
        .map(|fields| fields.into_iter().map(String::from).collect());

    let searchable_attributes = index
        .user_defined_searchable_fields(rtxn)?
        .map(|fields| fields.into_iter().map(String::from).collect());

    let filterable_attributes = index.filterable_fields(rtxn)?.into_iter().collect();

    let sortable_attributes = index.sortable_fields(rtxn)?.into_iter().collect();

    let criteria = index
        .criteria(rtxn)?
        .into_iter()
        .map(|c| c.to_string())
        .collect();

    let stop_words = index
        .stop_words(rtxn)?
        .map(|stop_words| -> Result<BTreeSet<_>, milli::Error> {
            Ok(stop_words.stream().into_strs()?.into_iter().collect())
        })
        .transpose()?
        .unwrap_or_default();
    let distinct_field = index.distinct_field(rtxn)?.map(String::from);

    // in milli each word in the synonyms map were split on their separator. Since we lost
    // this information we are going to put space between words.
    let synonyms = index
        .synonyms(rtxn)?
        .iter()
        .map(|(key, values)| {
            (
                key.join(" "),
                values.iter().map(|value| value.join(" ")).collect(),
            )
        })
        .collect();

    let min_typo_word_len = MinWordSizeTyposSetting {
        one_typo: Setting::Set(index.min_word_len_one_typo(rtxn)?),
        two_typos: Setting::Set(index.min_word_len_two_typos(rtxn)?),
    };

    let disabled_words = match index.exact_words(rtxn)? {
        Some(fst) => fst.into_stream().into_strs()?.into_iter().collect(),
        None => BTreeSet::new(),
    };

    let disabled_attributes = index
        .exact_attributes(rtxn)?
        .into_iter()
        .map(String::from)
        .collect();

    let typo_tolerance = TypoSettings {
        enabled: Setting::Set(index.authorize_typos(rtxn)?),
        min_word_size_for_typos: Setting::Set(min_typo_word_len),
        disable_on_words: Setting::Set(disabled_words),
        disable_on_attributes: Setting::Set(disabled_attributes),
    };

    let faceting = FacetingSettings {
        max_values_per_facet: Setting::Set(
            index
                .max_values_per_facet(rtxn)?
                .unwrap_or(DEFAULT_VALUES_PER_FACET),
        ),
    };

    let pagination = PaginationSettings {
        max_total_hits: Setting::Set(
            index
                .pagination_max_total_hits(rtxn)?
                .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS),
        ),
    };

    Ok(Settings {
        displayed_attributes: match displayed_attributes {
            Some(attrs) => Setting::Set(attrs),
            None => Setting::Reset,
        },
        searchable_attributes: match searchable_attributes {
            Some(attrs) => Setting::Set(attrs),
            None => Setting::Reset,
        },
        filterable_attributes: Setting::Set(filterable_attributes),
        sortable_attributes: Setting::Set(sortable_attributes),
        ranking_rules: Setting::Set(criteria),
        stop_words: Setting::Set(stop_words),
        distinct_attribute: match distinct_field {
            Some(field) => Setting::Set(field),
            None => Setting::Reset,
        },
        synonyms: Setting::Set(synonyms),
        typo_tolerance: Setting::Set(typo_tolerance),
        faceting: Setting::Set(faceting),
        pagination: Setting::Set(pagination),
        _kind: PhantomData,
    })
}

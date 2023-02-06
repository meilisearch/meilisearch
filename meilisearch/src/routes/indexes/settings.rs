use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use log::debug;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::settings::{settings, RankingRuleView, Settings, Unchecked};
use meilisearch_types::tasks::KindWithContent;
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::json::ValidatedJson;
use crate::routes::SummarizedTaskView;

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $update_verb:ident, $type:ty, $err_ty:ty, $attr:ident, $camelcase_attr:literal, $analytics_var:ident, $analytics:expr) => {
        pub mod $attr {
            use actix_web::web::Data;
            use actix_web::{web, HttpRequest, HttpResponse, Resource};
            use index_scheduler::IndexScheduler;
            use log::debug;
            use meilisearch_types::error::ResponseError;
            use meilisearch_types::index_uid::IndexUid;
            use meilisearch_types::milli::update::Setting;
            use meilisearch_types::settings::{settings, Settings};
            use meilisearch_types::tasks::KindWithContent;
            use $crate::analytics::Analytics;
            use $crate::extractors::authentication::policies::*;
            use $crate::extractors::authentication::GuardedData;
            use $crate::extractors::sequential_extractor::SeqHandler;
            use $crate::routes::SummarizedTaskView;

            pub async fn delete(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_UPDATE }>,
                    Data<IndexScheduler>,
                >,
                index_uid: web::Path<String>,
            ) -> Result<HttpResponse, ResponseError> {
                let index_uid = IndexUid::try_from(index_uid.into_inner())?;

                let new_settings = Settings { $attr: Setting::Reset.into(), ..Default::default() };

                let allow_index_creation = index_scheduler.filters().allow_index_creation;

                let task = KindWithContent::SettingsUpdate {
                    index_uid: index_uid.to_string(),
                    new_settings: Box::new(new_settings),
                    is_deletion: true,
                    allow_index_creation,
                };
                let task: SummarizedTaskView =
                    tokio::task::spawn_blocking(move || index_scheduler.register(task))
                        .await??
                        .into();

                debug!("returns: {:?}", task);
                Ok(HttpResponse::Accepted().json(task))
            }

            pub async fn update(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_UPDATE }>,
                    Data<IndexScheduler>,
                >,
                index_uid: actix_web::web::Path<String>,
                body: $crate::routes::indexes::ValidatedJson<Option<$type>, $err_ty>,
                req: HttpRequest,
                $analytics_var: web::Data<dyn Analytics>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let index_uid = IndexUid::try_from(index_uid.into_inner())?;

                let body = body.into_inner();

                $analytics(&body, &req);

                let new_settings = Settings {
                    $attr: match body {
                        Some(inner_body) => Setting::Set(inner_body).into(),
                        None => Setting::Reset.into(),
                    },
                    ..Default::default()
                };

                let allow_index_creation = index_scheduler.filters().allow_index_creation;

                let task = KindWithContent::SettingsUpdate {
                    index_uid: index_uid.to_string(),
                    new_settings: Box::new(new_settings),
                    is_deletion: false,
                    allow_index_creation,
                };
                let task: SummarizedTaskView =
                    tokio::task::spawn_blocking(move || index_scheduler.register(task))
                        .await??
                        .into();

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
                let index_uid = IndexUid::try_from(index_uid.into_inner())?;

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
}

make_setting_route!(
    "/filterable-attributes",
    put,
    std::collections::BTreeSet<String>,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsFilterableAttributes,
    >,
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
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsSortableAttributes,
    >,
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
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsDisplayedAttributes,
    >,
    displayed_attributes,
    "displayedAttributes",
    analytics,
    |displayed: &Option<Vec<String>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "DisplayedAttributes Updated".to_string(),
            json!({
                "displayed_attributes": {
                    "total": displayed.as_ref().map(|displayed| displayed.len()),
                    "with_wildcard": displayed.as_ref().map(|displayed| displayed.iter().any(|displayed| displayed == "*")),
                },
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/typo-tolerance",
    patch,
    meilisearch_types::settings::TypoSettings,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsTypoTolerance,
    >,
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
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsSearchableAttributes,
    >,
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
                    "with_wildcard": setting.as_ref().map(|searchable| searchable.iter().any(|searchable| searchable == "*")),
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
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsStopWords,
    >,
    stop_words,
    "stopWords",
    analytics,
    |stop_words: &Option<std::collections::BTreeSet<String>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "StopWords Updated".to_string(),
            json!({
                "stop_words": {
                    "total": stop_words.as_ref().map(|stop_words| stop_words.len()),
                },
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/synonyms",
    put,
    std::collections::BTreeMap<String, Vec<String>>,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsSynonyms,
    >,
    synonyms,
    "synonyms",
    analytics,
    |synonyms: &Option<std::collections::BTreeMap<String, Vec<String>>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "Synonyms Updated".to_string(),
            json!({
                "synonyms": {
                    "total": synonyms.as_ref().map(|synonyms| synonyms.len()),
                },
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/distinct-attribute",
    put,
    String,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsDistinctAttribute,
    >,
    distinct_attribute,
    "distinctAttribute",
    analytics,
    |distinct: &Option<String>, req: &HttpRequest| {
        use serde_json::json;
        analytics.publish(
            "DistinctAttribute Updated".to_string(),
            json!({
                "distinct_attribute": {
                    "set": distinct.is_some(),
                }
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/ranking-rules",
    put,
    Vec<meilisearch_types::settings::RankingRuleView>,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsRankingRules,
    >,
    ranking_rules,
    "rankingRules",
    analytics,
    |setting: &Option<Vec<meilisearch_types::settings::RankingRuleView>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "RankingRules Updated".to_string(),
            json!({
                "ranking_rules": {
                    "words_position": setting.as_ref().map(|rr| rr.iter().position(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Words))),
                    "typo_position": setting.as_ref().map(|rr| rr.iter().position(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Typo))),
                    "proximity_position": setting.as_ref().map(|rr| rr.iter().position(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Proximity))),
                    "attribute_position": setting.as_ref().map(|rr| rr.iter().position(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Attribute))),
                    "sort_position": setting.as_ref().map(|rr| rr.iter().position(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Sort))),
                    "exactness_position": setting.as_ref().map(|rr| rr.iter().position(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Exactness))),
                    "values": setting.as_ref().map(|rr| rr.iter().filter(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Asc(_) | meilisearch_types::settings::RankingRuleView::Desc(_)) ).map(|x| x.to_string()).collect::<Vec<_>>().join(", ")),
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
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsFaceting,
    >,
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
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsPagination,
    >,
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
    body: ValidatedJson<Settings<Unchecked>, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let new_settings = body.into_inner();

    analytics.publish(
        "Settings Updated".to_string(),
        json!({
           "ranking_rules": {
                "words_position": new_settings.ranking_rules.as_ref().set().map(|rr| rr.iter().position(|s| matches!(s, RankingRuleView::Words))),
                "typo_position": new_settings.ranking_rules.as_ref().set().map(|rr| rr.iter().position(|s| matches!(s, RankingRuleView::Typo))),
                "proximity_position": new_settings.ranking_rules.as_ref().set().map(|rr| rr.iter().position(|s| matches!(s, RankingRuleView::Proximity))),
                "attribute_position": new_settings.ranking_rules.as_ref().set().map(|rr| rr.iter().position(|s| matches!(s, RankingRuleView::Attribute))),
                "sort_position": new_settings.ranking_rules.as_ref().set().map(|rr| rr.iter().position(|s| matches!(s, RankingRuleView::Sort))),
                "exactness_position": new_settings.ranking_rules.as_ref().set().map(|rr| rr.iter().position(|s| matches!(s, RankingRuleView::Exactness))),
                "values": new_settings.ranking_rules.as_ref().set().map(|rr| rr.iter().filter(|s| !matches!(s, RankingRuleView::Asc(_) | RankingRuleView::Desc(_)) ).map(|x| x.to_string()).collect::<Vec<_>>().join(", ")),
            },
            "searchable_attributes": {
                "total": new_settings.searchable_attributes.as_ref().set().map(|searchable| searchable.len()),
                "with_wildcard": new_settings.searchable_attributes.as_ref().set().map(|searchable| searchable.iter().any(|searchable| searchable == "*")),
            },
            "displayed_attributes": {
                "total": new_settings.displayed_attributes.as_ref().set().map(|displayed| displayed.len()),
                "with_wildcard": new_settings.displayed_attributes.as_ref().set().map(|displayed| displayed.iter().any(|displayed| displayed == "*")),
            },
           "sortable_attributes": {
                "total": new_settings.sortable_attributes.as_ref().set().map(|sort| sort.len()),
                "has_geo": new_settings.sortable_attributes.as_ref().set().map(|sort| sort.iter().any(|s| s == "_geo")),
            },
           "filterable_attributes": {
                "total": new_settings.filterable_attributes.as_ref().set().map(|filter| filter.len()),
                "has_geo": new_settings.filterable_attributes.as_ref().set().map(|filter| filter.iter().any(|s| s == "_geo")),
            },
            "distinct_attribute": {
                "set": new_settings.distinct_attribute.as_ref().set().is_some()
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
            "stop_words": {
                "total": new_settings.stop_words.as_ref().set().map(|stop_words| stop_words.len()),
            },
            "synonyms": {
                "total": new_settings.synonyms.as_ref().set().map(|synonyms| synonyms.len()),
            },
        }),
        Some(&req),
    );

    let allow_index_creation = index_scheduler.filters().allow_index_creation;
    let index_uid = IndexUid::try_from(index_uid.into_inner())?.into_inner();
    let task = KindWithContent::SettingsUpdate {
        index_uid,
        new_settings: Box::new(new_settings),
        is_deletion: false,
        allow_index_creation,
    };
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn get_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

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
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let new_settings = Settings::cleared().into_unchecked();

    let allow_index_creation = index_scheduler.filters().allow_index_creation;
    let index_uid = IndexUid::try_from(index_uid.into_inner())?.into_inner();
    let task = KindWithContent::SettingsUpdate {
        index_uid,
        new_settings: Box::new(new_settings),
        is_deletion: true,
        allow_index_creation,
    };
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

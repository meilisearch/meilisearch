use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::facet_values_sort::FacetValuesSort;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::settings::{settings, RankingRuleView, SecretPolicy, Settings, Unchecked};
use meilisearch_types::tasks::KindWithContent;
use serde_json::json;
use tracing::debug;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::Opt;

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $update_verb:ident, $type:ty, $err_ty:ty, $attr:ident, $camelcase_attr:literal, $analytics_var:ident, $analytics:expr) => {
        pub mod $attr {
            use actix_web::web::Data;
            use actix_web::{web, HttpRequest, HttpResponse, Resource};
            use index_scheduler::IndexScheduler;
            use meilisearch_types::error::ResponseError;
            use meilisearch_types::index_uid::IndexUid;
            use meilisearch_types::milli::update::Setting;
            use meilisearch_types::settings::{settings, Settings};
            use meilisearch_types::tasks::KindWithContent;
            use tracing::debug;
            use $crate::analytics::Analytics;
            use $crate::extractors::authentication::policies::*;
            use $crate::extractors::authentication::GuardedData;
            use $crate::extractors::sequential_extractor::SeqHandler;
            use $crate::Opt;
            use $crate::routes::{is_dry_run, get_task_id, SummarizedTaskView};

            pub async fn delete(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_UPDATE }>,
                    Data<IndexScheduler>,
                >,
                index_uid: web::Path<String>,
                req: HttpRequest,
                opt: web::Data<Opt>,
            ) -> Result<HttpResponse, ResponseError> {
                let index_uid = IndexUid::try_from(index_uid.into_inner())?;

                let new_settings = Settings { $attr: Setting::Reset.into(), ..Default::default() };

                let allow_index_creation =
                    index_scheduler.filters().allow_index_creation(&index_uid);

                let task = KindWithContent::SettingsUpdate {
                    index_uid: index_uid.to_string(),
                    new_settings: Box::new(new_settings),
                    is_deletion: true,
                    allow_index_creation,
                };
                let uid = get_task_id(&req, &opt)?;
                let dry_run = is_dry_run(&req, &opt)?;
                let task: SummarizedTaskView =
                    tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
                        .await??
                        .into();

                debug!(returns = ?task, "Delete settings");
                Ok(HttpResponse::Accepted().json(task))
            }

            pub async fn update(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_UPDATE }>,
                    Data<IndexScheduler>,
                >,
                index_uid: actix_web::web::Path<String>,
                body: deserr::actix_web::AwebJson<Option<$type>, $err_ty>,
                req: HttpRequest,
                opt: web::Data<Opt>,
                $analytics_var: web::Data<dyn Analytics>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let index_uid = IndexUid::try_from(index_uid.into_inner())?;

                let body = body.into_inner();
                debug!(parameters = ?body, "Update settings");

                #[allow(clippy::redundant_closure_call)]
                $analytics(&body, &req);

                let new_settings = Settings {
                    $attr: match body {
                        Some(inner_body) => Setting::Set(inner_body).into(),
                        None => Setting::Reset.into(),
                    },
                    ..Default::default()
                };

                let new_settings = $crate::routes::indexes::settings::validate_settings(
                    new_settings,
                    &index_scheduler,
                )?;

                let allow_index_creation =
                    index_scheduler.filters().allow_index_creation(&index_uid);

                let task = KindWithContent::SettingsUpdate {
                    index_uid: index_uid.to_string(),
                    new_settings: Box::new(new_settings),
                    is_deletion: false,
                    allow_index_creation,
                };
                let uid = get_task_id(&req, &opt)?;
                let dry_run = is_dry_run(&req, &opt)?;
                let task: SummarizedTaskView =
                    tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
                        .await??
                        .into();

                debug!(returns = ?task, "Update settings");
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
                let settings = settings(&index, &rtxn, meilisearch_types::settings::SecretPolicy::HideSecrets)?;

                debug!(returns = ?settings, "Update settings");

                Ok(HttpResponse::Ok().json(settings.$attr))
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
    "/non-separator-tokens",
    put,
    std::collections::BTreeSet<String>,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsNonSeparatorTokens,
    >,
    non_separator_tokens,
    "nonSeparatorTokens",
    analytics,
    |non_separator_tokens: &Option<std::collections::BTreeSet<String>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "nonSeparatorTokens Updated".to_string(),
            json!({
                "non_separator_tokens": {
                    "total": non_separator_tokens.as_ref().map(|non_separator_tokens| non_separator_tokens.len()),
                },
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/separator-tokens",
    put,
    std::collections::BTreeSet<String>,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsSeparatorTokens,
    >,
    separator_tokens,
    "separatorTokens",
    analytics,
    |separator_tokens: &Option<std::collections::BTreeSet<String>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "separatorTokens Updated".to_string(),
            json!({
                "separator_tokens": {
                    "total": separator_tokens.as_ref().map(|separator_tokens| separator_tokens.len()),
                },
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/dictionary",
    put,
    std::collections::BTreeSet<String>,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsDictionary,
    >,
    dictionary,
    "dictionary",
    analytics,
    |dictionary: &Option<std::collections::BTreeSet<String>>, req: &HttpRequest| {
        use serde_json::json;

        analytics.publish(
            "dictionary Updated".to_string(),
            json!({
                "dictionary": {
                    "total": dictionary.as_ref().map(|dictionary| dictionary.len()),
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
    "/proximity-precision",
    put,
    meilisearch_types::settings::ProximityPrecisionView,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsProximityPrecision,
    >,
    proximity_precision,
    "proximityPrecision",
    analytics,
    |precision: &Option<meilisearch_types::settings::ProximityPrecisionView>, req: &HttpRequest| {
        use serde_json::json;
        analytics.publish(
            "ProximityPrecision Updated".to_string(),
            json!({
                "proximity_precision": {
                    "set": precision.is_some(),
                    "value": precision.unwrap_or_default(),
                }
            }),
            Some(req),
        );
    }
);

make_setting_route!(
    "/localized-attributes",
    put,
    Vec<meilisearch_types::locales::LocalizedAttributesRuleView>,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsLocalizedAttributes,
    >,
    localized_attributes,
    "localizedAttributes",
    analytics,
    |rules: &Option<Vec<meilisearch_types::locales::LocalizedAttributesRuleView>>, req: &HttpRequest| {
        use serde_json::json;
        analytics.publish(
            "LocalizedAttributesRules Updated".to_string(),
            json!({
                "locales": rules.as_ref().map(|rules| rules.iter().flat_map(|rule| rule.locales.iter().cloned()).collect::<std::collections::BTreeSet<_>>())
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
        use meilisearch_types::facet_values_sort::FacetValuesSort;

        analytics.publish(
            "Faceting Updated".to_string(),
            json!({
                "faceting": {
                    "max_values_per_facet": setting.as_ref().and_then(|s| s.max_values_per_facet.set()),
                    "sort_facet_values_by_star_count": setting.as_ref().and_then(|s| {
                        s.sort_facet_values_by.as_ref().set().map(|s| s.iter().any(|(k, v)| k == "*" && v == &FacetValuesSort::Count))
                    }),
                    "sort_facet_values_by_total": setting.as_ref().and_then(|s| s.sort_facet_values_by.as_ref().set().map(|s| s.len())),
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

make_setting_route!(
    "/embedders",
    patch,
    std::collections::BTreeMap<String, Setting<meilisearch_types::milli::vector::settings::EmbeddingSettings>>,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsEmbedders,
    >,
    embedders,
    "embedders",
    analytics,
    |setting: &Option<std::collections::BTreeMap<String, Setting<meilisearch_types::milli::vector::settings::EmbeddingSettings>>>, req: &HttpRequest| {


        analytics.publish(
            "Embedders Updated".to_string(),
            serde_json::json!({"embedders": crate::routes::indexes::settings::embedder_analytics(setting.as_ref())}),
            Some(req),
        );
    }
);

fn embedder_analytics(
    setting: Option<
        &std::collections::BTreeMap<
            String,
            Setting<meilisearch_types::milli::vector::settings::EmbeddingSettings>,
        >,
    >,
) -> serde_json::Value {
    let mut sources = std::collections::HashSet::new();

    if let Some(s) = &setting {
        for source in s
            .values()
            .filter_map(|config| config.clone().set())
            .filter_map(|config| config.source.set())
        {
            use meilisearch_types::milli::vector::settings::EmbedderSource;
            match source {
                EmbedderSource::OpenAi => sources.insert("openAi"),
                EmbedderSource::HuggingFace => sources.insert("huggingFace"),
                EmbedderSource::UserProvided => sources.insert("userProvided"),
                EmbedderSource::Ollama => sources.insert("ollama"),
                EmbedderSource::Rest => sources.insert("rest"),
            };
        }
    };

    let document_template_used = setting.as_ref().map(|map| {
        map.values()
            .filter_map(|config| config.clone().set())
            .any(|config| config.document_template.set().is_some())
    });

    json!(
        {
            "total": setting.as_ref().map(|s| s.len()),
            "sources": sources,
            "document_template_used": document_template_used,
        }
    )
}

make_setting_route!(
    "/search-cutoff-ms",
    put,
    u64,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsSearchCutoffMs,
    >,
    search_cutoff_ms,
    "searchCutoffMs",
    analytics,
    |setting: &Option<u64>, req: &HttpRequest| {
        analytics.publish(
            "Search Cutoff Updated".to_string(),
            serde_json::json!({"search_cutoff_ms": setting }),
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
    proximity_precision,
    stop_words,
    separator_tokens,
    non_separator_tokens,
    dictionary,
    synonyms,
    ranking_rules,
    typo_tolerance,
    pagination,
    faceting,
    embedders,
    search_cutoff_ms
);

pub async fn update_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<Settings<Unchecked>, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let new_settings = body.into_inner();
    debug!(parameters = ?new_settings, "Update all settings");
    let new_settings = validate_settings(new_settings, &index_scheduler)?;

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
            "proximity_precision": {
                "set": new_settings.proximity_precision.as_ref().set().is_some(),
                "value": new_settings.proximity_precision.as_ref().set().copied().unwrap_or_default()
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
                "sort_facet_values_by_star_count": new_settings.faceting
                    .as_ref()
                    .set()
                    .and_then(|s| {
                        s.sort_facet_values_by.as_ref().set().map(|s| s.iter().any(|(k, v)| k == "*" && v == &FacetValuesSort::Count))
                    }),
                "sort_facet_values_by_total": new_settings.faceting
                    .as_ref()
                    .set()
                    .and_then(|s| s.sort_facet_values_by.as_ref().set().map(|s| s.len())),
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
            "embedders": crate::routes::indexes::settings::embedder_analytics(new_settings.embedders.as_ref().set()),
            "search_cutoff_ms": new_settings.search_cutoff_ms.as_ref().set(),
            "locales": new_settings.localized_attributes.as_ref().set().map(|rules| rules.iter().flat_map(|rule| rule.locales.iter().cloned()).collect::<std::collections::BTreeSet<_>>()),
        }),
        Some(&req),
    );

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let index_uid = IndexUid::try_from(index_uid.into_inner())?.into_inner();
    let task = KindWithContent::SettingsUpdate {
        index_uid,
        new_settings: Box::new(new_settings),
        is_deletion: false,
        allow_index_creation,
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Update all settings");
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn get_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let index = index_scheduler.index(&index_uid)?;
    let rtxn = index.read_txn()?;
    let new_settings = settings(&index, &rtxn, SecretPolicy::HideSecrets)?;
    debug!(returns = ?new_settings, "Get all settings");
    Ok(HttpResponse::Ok().json(new_settings))
}

pub async fn delete_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    opt: web::Data<Opt>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let new_settings = Settings::cleared().into_unchecked();

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let index_uid = IndexUid::try_from(index_uid.into_inner())?.into_inner();
    let task = KindWithContent::SettingsUpdate {
        index_uid,
        new_settings: Box::new(new_settings),
        is_deletion: true,
        allow_index_creation,
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Delete all settings");
    Ok(HttpResponse::Accepted().json(task))
}

fn validate_settings(
    settings: Settings<Unchecked>,
    index_scheduler: &IndexScheduler,
) -> Result<Settings<Unchecked>, ResponseError> {
    if matches!(settings.embedders, Setting::Set(_)) {
        index_scheduler.features().check_vector("Passing `embedders` in settings")?
    }
    Ok(settings.validate()?)
}

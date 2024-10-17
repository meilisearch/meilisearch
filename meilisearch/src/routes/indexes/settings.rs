use std::collections::{BTreeSet, HashSet};

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::facet_values_sort::FacetValuesSort;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::locales::Locale;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::settings::{
    settings, ProximityPrecisionView, RankingRuleView, SecretPolicy, Settings, Unchecked,
};
use meilisearch_types::tasks::KindWithContent;
use serde::Serialize;
use tracing::debug;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::Opt;

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $update_verb:ident, $type:ty, $err_ty:ty, $attr:ident, $camelcase_attr:literal, $analytics:ident) => {
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
                analytics: web::Data<Analytics>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let index_uid = IndexUid::try_from(index_uid.into_inner())?;

                let body = body.into_inner();
                debug!(parameters = ?body, "Update settings");

                #[allow(clippy::redundant_closure_call)]
                analytics.publish(
                    $crate::routes::indexes::settings::$analytics::new(body.as_ref()).into_settings(),
                    &req,
                );

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
    FilterableAttributesAnalytics
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
    SortableAttributesAnalytics
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
    DisplayedAttributesAnalytics
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
    TypoToleranceAnalytics
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
    SearchableAttributesAnalytics
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
    StopWordsAnalytics
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
    NonSeparatorTokensAnalytics
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
    SeparatorTokensAnalytics
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
    DictionaryAnalytics
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
    SynonymsAnalytics
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
    DistinctAttributeAnalytics
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
    ProximityPrecisionAnalytics
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
    LocalesAnalytics
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
    RankingRulesAnalytics
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
    FacetingAnalytics
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
    PaginationAnalytics
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
    EmbeddersAnalytics
);

make_setting_route!(
    "/search-cutoff-ms",
    put,
    u64,
    meilisearch_types::deserr::DeserrJsonError<
        meilisearch_types::error::deserr_codes::InvalidSettingsSearchCutoffMs,
    >,
    search_cutoff_ms,
    "searchCutoffMs",
    SearchCutoffMsAnalytics
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
    localized_attributes,
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

#[derive(Serialize, Default)]
struct SettingsAnalytics {
    ranking_rules: RankingRulesAnalytics,
    searchable_attributes: SearchableAttributesAnalytics,
    displayed_attributes: DisplayedAttributesAnalytics,
    sortable_attributes: SortableAttributesAnalytics,
    filterable_attributes: FilterableAttributesAnalytics,
    distinct_attribute: DistinctAttributeAnalytics,
    proximity_precision: ProximityPrecisionAnalytics,
    typo_tolerance: TypoToleranceAnalytics,
    faceting: FacetingAnalytics,
    pagination: PaginationAnalytics,
    stop_words: StopWordsAnalytics,
    synonyms: SynonymsAnalytics,
    embedders: EmbeddersAnalytics,
    search_cutoff_ms: SearchCutoffMsAnalytics,
    locales: LocalesAnalytics,
    dictionary: DictionaryAnalytics,
    separator_tokens: SeparatorTokensAnalytics,
    non_separator_tokens: NonSeparatorTokensAnalytics,
}

impl Aggregate for SettingsAnalytics {
    fn event_name(&self) -> &'static str {
        "Settings Updated"
    }

    fn aggregate(self: Box<Self>, other: Box<Self>) -> Box<Self> {
        Box::new(Self {
            ranking_rules: RankingRulesAnalytics {
                words_position: self
                    .ranking_rules
                    .words_position
                    .or(other.ranking_rules.words_position),
                typo_position: self
                    .ranking_rules
                    .typo_position
                    .or(other.ranking_rules.typo_position),
                proximity_position: self
                    .ranking_rules
                    .proximity_position
                    .or(other.ranking_rules.proximity_position),
                attribute_position: self
                    .ranking_rules
                    .attribute_position
                    .or(other.ranking_rules.attribute_position),
                sort_position: self
                    .ranking_rules
                    .sort_position
                    .or(other.ranking_rules.sort_position),
                exactness_position: self
                    .ranking_rules
                    .exactness_position
                    .or(other.ranking_rules.exactness_position),
                values: self.ranking_rules.values.or(other.ranking_rules.values),
            },
            searchable_attributes: SearchableAttributesAnalytics {
                total: self.searchable_attributes.total.or(other.searchable_attributes.total),
                with_wildcard: self
                    .searchable_attributes
                    .with_wildcard
                    .or(other.searchable_attributes.with_wildcard),
            },
            displayed_attributes: DisplayedAttributesAnalytics {
                total: self.displayed_attributes.total.or(other.displayed_attributes.total),
                with_wildcard: self
                    .displayed_attributes
                    .with_wildcard
                    .or(other.displayed_attributes.with_wildcard),
            },
            sortable_attributes: SortableAttributesAnalytics {
                total: self.sortable_attributes.total.or(other.sortable_attributes.total),
                has_geo: self.sortable_attributes.has_geo.or(other.sortable_attributes.has_geo),
            },
            filterable_attributes: FilterableAttributesAnalytics {
                total: self.filterable_attributes.total.or(other.filterable_attributes.total),
                has_geo: self.filterable_attributes.has_geo.or(other.filterable_attributes.has_geo),
            },
            distinct_attribute: DistinctAttributeAnalytics {
                set: self.distinct_attribute.set | other.distinct_attribute.set,
            },
            proximity_precision: ProximityPrecisionAnalytics {
                set: self.proximity_precision.set | other.proximity_precision.set,
                value: self.proximity_precision.value.or(other.proximity_precision.value),
            },
            typo_tolerance: TypoToleranceAnalytics {
                enabled: self.typo_tolerance.enabled.or(other.typo_tolerance.enabled),
                disable_on_attributes: self
                    .typo_tolerance
                    .disable_on_attributes
                    .or(other.typo_tolerance.disable_on_attributes),
                disable_on_words: self
                    .typo_tolerance
                    .disable_on_words
                    .or(other.typo_tolerance.disable_on_words),
                min_word_size_for_one_typo: self
                    .typo_tolerance
                    .min_word_size_for_one_typo
                    .or(other.typo_tolerance.min_word_size_for_one_typo),
                min_word_size_for_two_typos: self
                    .typo_tolerance
                    .min_word_size_for_two_typos
                    .or(other.typo_tolerance.min_word_size_for_two_typos),
            },
            faceting: FacetingAnalytics {
                max_values_per_facet: self
                    .faceting
                    .max_values_per_facet
                    .or(other.faceting.max_values_per_facet),
                sort_facet_values_by_star_count: self
                    .faceting
                    .sort_facet_values_by_star_count
                    .or(other.faceting.sort_facet_values_by_star_count),
                sort_facet_values_by_total: self
                    .faceting
                    .sort_facet_values_by_total
                    .or(other.faceting.sort_facet_values_by_total),
            },
            pagination: PaginationAnalytics {
                max_total_hits: self.pagination.max_total_hits.or(other.pagination.max_total_hits),
            },
            stop_words: StopWordsAnalytics {
                total: self.stop_words.total.or(other.stop_words.total),
            },
            synonyms: SynonymsAnalytics { total: self.synonyms.total.or(other.synonyms.total) },
            embedders: EmbeddersAnalytics {
                total: self.embedders.total.or(other.embedders.total),
                sources: match (self.embedders.sources, other.embedders.sources) {
                    (None, None) => None,
                    (Some(sources), None) | (None, Some(sources)) => Some(sources),
                    (Some(this), Some(other)) => Some(this.union(&other).cloned().collect()),
                },
                document_template_used: match (
                    self.embedders.document_template_used,
                    other.embedders.document_template_used,
                ) {
                    (None, None) => None,
                    (Some(used), None) | (None, Some(used)) => Some(used),
                    (Some(this), Some(other)) => Some(this | other),
                },
                document_template_max_bytes: match (
                    self.embedders.document_template_max_bytes,
                    other.embedders.document_template_max_bytes,
                ) {
                    (None, None) => None,
                    (Some(bytes), None) | (None, Some(bytes)) => Some(bytes),
                    (Some(this), Some(other)) => Some(this.max(other)),
                },
                binary_quantization_used: match (
                    self.embedders.binary_quantization_used,
                    other.embedders.binary_quantization_used,
                ) {
                    (None, None) => None,
                    (Some(bq), None) | (None, Some(bq)) => Some(bq),
                    (Some(this), Some(other)) => Some(this | other),
                },
            },
            search_cutoff_ms: SearchCutoffMsAnalytics {
                search_cutoff_ms: self
                    .search_cutoff_ms
                    .search_cutoff_ms
                    .or(other.search_cutoff_ms.search_cutoff_ms),
            },
            locales: LocalesAnalytics { locales: self.locales.locales.or(other.locales.locales) },
            dictionary: DictionaryAnalytics {
                total: self.dictionary.total.or(other.dictionary.total),
            },
            separator_tokens: SeparatorTokensAnalytics {
                total: self.separator_tokens.total.or(other.non_separator_tokens.total),
            },
            non_separator_tokens: NonSeparatorTokensAnalytics {
                total: self.non_separator_tokens.total.or(other.non_separator_tokens.total),
            },
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

#[derive(Serialize, Default)]
struct RankingRulesAnalytics {
    words_position: Option<usize>,
    typo_position: Option<usize>,
    proximity_position: Option<usize>,
    attribute_position: Option<usize>,
    sort_position: Option<usize>,
    exactness_position: Option<usize>,
    values: Option<String>,
}

impl RankingRulesAnalytics {
    pub fn new(rr: Option<&Vec<RankingRuleView>>) -> Self {
        RankingRulesAnalytics {
            words_position: rr.as_ref().and_then(|rr| {
                rr.iter()
                    .position(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Words))
            }),
            typo_position: rr.as_ref().and_then(|rr| {
                rr.iter()
                    .position(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Typo))
            }),
            proximity_position: rr.as_ref().and_then(|rr| {
                rr.iter().position(|s| {
                    matches!(s, meilisearch_types::settings::RankingRuleView::Proximity)
                })
            }),
            attribute_position: rr.as_ref().and_then(|rr| {
                rr.iter().position(|s| {
                    matches!(s, meilisearch_types::settings::RankingRuleView::Attribute)
                })
            }),
            sort_position: rr.as_ref().and_then(|rr| {
                rr.iter()
                    .position(|s| matches!(s, meilisearch_types::settings::RankingRuleView::Sort))
            }),
            exactness_position: rr.as_ref().and_then(|rr| {
                rr.iter().position(|s| {
                    matches!(s, meilisearch_types::settings::RankingRuleView::Exactness)
                })
            }),
            values: rr.as_ref().map(|rr| {
                rr.iter()
                    .filter(|s| {
                        matches!(
                            s,
                            meilisearch_types::settings::RankingRuleView::Asc(_)
                                | meilisearch_types::settings::RankingRuleView::Desc(_)
                        )
                    })
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            }),
        }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { ranking_rules: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct SearchableAttributesAnalytics {
    total: Option<usize>,
    with_wildcard: Option<bool>,
}

impl SearchableAttributesAnalytics {
    pub fn new(setting: Option<&Vec<String>>) -> Self {
        Self {
            total: setting.as_ref().map(|searchable| searchable.len()),
            with_wildcard: setting
                .as_ref()
                .map(|searchable| searchable.iter().any(|searchable| searchable == "*")),
        }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { searchable_attributes: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct DisplayedAttributesAnalytics {
    total: Option<usize>,
    with_wildcard: Option<bool>,
}

impl DisplayedAttributesAnalytics {
    pub fn new(displayed: Option<&Vec<String>>) -> Self {
        Self {
            total: displayed.as_ref().map(|displayed| displayed.len()),
            with_wildcard: displayed
                .as_ref()
                .map(|displayed| displayed.iter().any(|displayed| displayed == "*")),
        }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { displayed_attributes: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct SortableAttributesAnalytics {
    total: Option<usize>,
    has_geo: Option<bool>,
}

impl SortableAttributesAnalytics {
    pub fn new(setting: Option<&std::collections::BTreeSet<String>>) -> Self {
        Self {
            total: setting.as_ref().map(|sort| sort.len()),
            has_geo: setting.as_ref().map(|sort| sort.contains("_geo")),
        }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { sortable_attributes: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct FilterableAttributesAnalytics {
    total: Option<usize>,
    has_geo: Option<bool>,
}

impl FilterableAttributesAnalytics {
    pub fn new(setting: Option<&std::collections::BTreeSet<String>>) -> Self {
        Self {
            total: setting.as_ref().map(|filter| filter.len()),
            has_geo: setting.as_ref().map(|filter| filter.contains("_geo")),
        }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { filterable_attributes: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct DistinctAttributeAnalytics {
    set: bool,
}

impl DistinctAttributeAnalytics {
    pub fn new(distinct: Option<&String>) -> Self {
        Self { set: distinct.is_some() }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { distinct_attribute: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct ProximityPrecisionAnalytics {
    set: bool,
    value: Option<ProximityPrecisionView>,
}

impl ProximityPrecisionAnalytics {
    pub fn new(precision: Option<&meilisearch_types::settings::ProximityPrecisionView>) -> Self {
        Self { set: precision.is_some(), value: precision.cloned() }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { proximity_precision: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct TypoToleranceAnalytics {
    enabled: Option<bool>,
    disable_on_attributes: Option<bool>,
    disable_on_words: Option<bool>,
    min_word_size_for_one_typo: Option<u8>,
    min_word_size_for_two_typos: Option<u8>,
}

impl TypoToleranceAnalytics {
    pub fn new(setting: Option<&meilisearch_types::settings::TypoSettings>) -> Self {
        Self {
            enabled: setting.as_ref().map(|s| !matches!(s.enabled, Setting::Set(false))),
            disable_on_attributes: setting
                .as_ref()
                .and_then(|s| s.disable_on_attributes.as_ref().set().map(|m| !m.is_empty())),
            disable_on_words: setting
                .as_ref()
                .and_then(|s| s.disable_on_words.as_ref().set().map(|m| !m.is_empty())),
            min_word_size_for_one_typo: setting
                .as_ref()
                .and_then(|s| s.min_word_size_for_typos.as_ref().set().map(|s| s.one_typo.set()))
                .flatten(),
            min_word_size_for_two_typos: setting
                .as_ref()
                .and_then(|s| s.min_word_size_for_typos.as_ref().set().map(|s| s.two_typos.set()))
                .flatten(),
        }
    }
    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { typo_tolerance: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct FacetingAnalytics {
    max_values_per_facet: Option<usize>,
    sort_facet_values_by_star_count: Option<bool>,
    sort_facet_values_by_total: Option<usize>,
}

impl FacetingAnalytics {
    pub fn new(setting: Option<&meilisearch_types::settings::FacetingSettings>) -> Self {
        Self {
            max_values_per_facet: setting.as_ref().and_then(|s| s.max_values_per_facet.set()),
            sort_facet_values_by_star_count: setting.as_ref().and_then(|s| {
                s.sort_facet_values_by
                    .as_ref()
                    .set()
                    .map(|s| s.iter().any(|(k, v)| k == "*" && v == &FacetValuesSort::Count))
            }),
            sort_facet_values_by_total: setting
                .as_ref()
                .and_then(|s| s.sort_facet_values_by.as_ref().set().map(|s| s.len())),
        }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { faceting: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct PaginationAnalytics {
    max_total_hits: Option<usize>,
}

impl PaginationAnalytics {
    pub fn new(setting: Option<&meilisearch_types::settings::PaginationSettings>) -> Self {
        Self { max_total_hits: setting.as_ref().and_then(|s| s.max_total_hits.set()) }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { pagination: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct StopWordsAnalytics {
    total: Option<usize>,
}

impl StopWordsAnalytics {
    pub fn new(stop_words: Option<&BTreeSet<String>>) -> Self {
        Self { total: stop_words.as_ref().map(|stop_words| stop_words.len()) }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { stop_words: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct SynonymsAnalytics {
    total: Option<usize>,
}

impl SynonymsAnalytics {
    pub fn new(synonyms: Option<&std::collections::BTreeMap<String, Vec<String>>>) -> Self {
        Self { total: synonyms.as_ref().map(|synonyms| synonyms.len()) }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { synonyms: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct EmbeddersAnalytics {
    // last
    total: Option<usize>,
    // Merge the sources
    sources: Option<HashSet<String>>,
    // |=
    document_template_used: Option<bool>,
    // max
    document_template_max_bytes: Option<usize>,
    // |=
    binary_quantization_used: Option<bool>,
}

impl EmbeddersAnalytics {
    pub fn new(
        setting: Option<
            &std::collections::BTreeMap<
                String,
                Setting<meilisearch_types::milli::vector::settings::EmbeddingSettings>,
            >,
        >,
    ) -> Self {
        let mut sources = std::collections::HashSet::new();

        if let Some(s) = &setting {
            for source in s
                .values()
                .filter_map(|config| config.clone().set())
                .filter_map(|config| config.source.set())
            {
                use meilisearch_types::milli::vector::settings::EmbedderSource;
                match source {
                    EmbedderSource::OpenAi => sources.insert("openAi".to_string()),
                    EmbedderSource::HuggingFace => sources.insert("huggingFace".to_string()),
                    EmbedderSource::UserProvided => sources.insert("userProvided".to_string()),
                    EmbedderSource::Ollama => sources.insert("ollama".to_string()),
                    EmbedderSource::Rest => sources.insert("rest".to_string()),
                };
            }
        };

        Self {
            total: setting.as_ref().map(|s| s.len()),
            sources: Some(sources),
            document_template_used: setting.as_ref().map(|map| {
                map.values()
                    .filter_map(|config| config.clone().set())
                    .any(|config| config.document_template.set().is_some())
            }),
            document_template_max_bytes: setting.as_ref().and_then(|map| {
                map.values()
                    .filter_map(|config| config.clone().set())
                    .filter_map(|config| config.document_template_max_bytes.set())
                    .max()
            }),
            binary_quantization_used: setting.as_ref().map(|map| {
                map.values()
                    .filter_map(|config| config.clone().set())
                    .any(|config| config.binary_quantized.set().is_some())
            }),
        }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { embedders: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
#[serde(transparent)]
struct SearchCutoffMsAnalytics {
    search_cutoff_ms: Option<u64>,
}

impl SearchCutoffMsAnalytics {
    pub fn new(setting: Option<&u64>) -> Self {
        Self { search_cutoff_ms: setting.copied() }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { search_cutoff_ms: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
#[serde(transparent)]
struct LocalesAnalytics {
    locales: Option<BTreeSet<Locale>>,
}

impl LocalesAnalytics {
    pub fn new(
        rules: Option<&Vec<meilisearch_types::locales::LocalizedAttributesRuleView>>,
    ) -> Self {
        LocalesAnalytics {
            locales: rules.as_ref().map(|rules| {
                rules
                    .iter()
                    .flat_map(|rule| rule.locales.iter().cloned())
                    .collect::<std::collections::BTreeSet<_>>()
            }),
        }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { locales: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct DictionaryAnalytics {
    total: Option<usize>,
}

impl DictionaryAnalytics {
    pub fn new(dictionary: Option<&std::collections::BTreeSet<String>>) -> Self {
        Self { total: dictionary.as_ref().map(|dictionary| dictionary.len()) }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { dictionary: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct SeparatorTokensAnalytics {
    total: Option<usize>,
}

impl SeparatorTokensAnalytics {
    pub fn new(separator_tokens: Option<&std::collections::BTreeSet<String>>) -> Self {
        Self { total: separator_tokens.as_ref().map(|separator_tokens| separator_tokens.len()) }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { separator_tokens: self, ..Default::default() }
    }
}

#[derive(Serialize, Default)]
struct NonSeparatorTokensAnalytics {
    total: Option<usize>,
}

impl NonSeparatorTokensAnalytics {
    pub fn new(non_separator_tokens: Option<&std::collections::BTreeSet<String>>) -> Self {
        Self {
            total: non_separator_tokens
                .as_ref()
                .map(|non_separator_tokens| non_separator_tokens.len()),
        }
    }

    pub fn into_settings(self) -> SettingsAnalytics {
        SettingsAnalytics { non_separator_tokens: self, ..Default::default() }
    }
}

pub async fn update_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<Settings<Unchecked>, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let new_settings = body.into_inner();
    debug!(parameters = ?new_settings, "Update all settings");
    let new_settings = validate_settings(new_settings, &index_scheduler)?;

    analytics.publish(
        SettingsAnalytics {
            ranking_rules: RankingRulesAnalytics::new(new_settings.ranking_rules.as_ref().set()),
            searchable_attributes: SearchableAttributesAnalytics::new(
                new_settings.searchable_attributes.as_ref().set(),
            ),
            displayed_attributes: DisplayedAttributesAnalytics::new(
                new_settings.displayed_attributes.as_ref().set(),
            ),
            sortable_attributes: SortableAttributesAnalytics::new(
                new_settings.sortable_attributes.as_ref().set(),
            ),
            filterable_attributes: FilterableAttributesAnalytics::new(
                new_settings.filterable_attributes.as_ref().set(),
            ),
            distinct_attribute: DistinctAttributeAnalytics::new(
                new_settings.distinct_attribute.as_ref().set(),
            ),
            proximity_precision: ProximityPrecisionAnalytics::new(
                new_settings.proximity_precision.as_ref().set(),
            ),
            typo_tolerance: TypoToleranceAnalytics::new(new_settings.typo_tolerance.as_ref().set()),
            faceting: FacetingAnalytics::new(new_settings.faceting.as_ref().set()),
            pagination: PaginationAnalytics::new(new_settings.pagination.as_ref().set()),
            stop_words: StopWordsAnalytics::new(new_settings.stop_words.as_ref().set()),
            synonyms: SynonymsAnalytics::new(new_settings.synonyms.as_ref().set()),
            embedders: EmbeddersAnalytics::new(new_settings.embedders.as_ref().set()),
            search_cutoff_ms: SearchCutoffMsAnalytics::new(
                new_settings.search_cutoff_ms.as_ref().set(),
            ),
            locales: LocalesAnalytics::new(new_settings.localized_attributes.as_ref().set()),
            dictionary: DictionaryAnalytics::new(new_settings.dictionary.as_ref().set()),
            separator_tokens: SeparatorTokensAnalytics::new(
                new_settings.separator_tokens.as_ref().set(),
            ),
            non_separator_tokens: NonSeparatorTokensAnalytics::new(
                new_settings.non_separator_tokens.as_ref().set(),
            ),
        },
        &req,
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

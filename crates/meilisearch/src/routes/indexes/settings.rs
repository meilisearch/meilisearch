use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::settings::{settings, SecretPolicy, Settings, Unchecked};
use meilisearch_types::tasks::KindWithContent;
use tracing::debug;

use super::settings_analytics::*;
use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::Opt;

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $update_verb:ident, $type:ty, $err_ty:ty, $attr:ident, $camelcase_attr:literal, $analytics:ident) => {
        #[allow(dead_code)]
        
        pub fn verify_field_exists_for_$attr<FH>(settings: Settings<FH>) {
            match settings {
                Settings { $attr: _, .. } => {}
            }
        }

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
                    $crate::routes::indexes::settings_analytics::$analytics::new(body.as_ref()).into_settings(),
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

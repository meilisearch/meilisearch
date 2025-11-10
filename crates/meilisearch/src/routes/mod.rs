use std::collections::BTreeMap;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use export::Export;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::batch_view::BatchView;
use meilisearch_types::batches::BatchStats;
use meilisearch_types::error::{Code, ErrorType, ResponseError};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::CreateApiKey;
use meilisearch_types::milli::{
    AttributePatterns, FilterFeatures, FilterableAttributesFeatures, FilterableAttributesPatterns,
    FilterableAttributesRule,
};
use meilisearch_types::settings::{
    Checked, FacetingSettings, MinWordSizeTyposSetting, PaginationSettings, Settings, TypoSettings,
    Unchecked,
};
use meilisearch_types::task_view::{DetailsView, TaskView};
use meilisearch_types::tasks::{Kind, Status, Task, TaskId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use self::api_key::KeyView;
use self::indexes::documents::BrowseQuery;
use self::indexes::{IndexCreateRequest, IndexStats, UpdateIndexRequest};
use self::logs::{GetLogs, LogMode, UpdateStderrLogs};
use self::open_api_utils::OpenApiAuth;
use self::tasks::AllTasks;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::milli::progress::{ProgressStepView, ProgressView};
use crate::routes::batches::AllBatches;
use crate::routes::features::RuntimeTogglableFeatures;
use crate::routes::indexes::documents::{DocumentDeletionByFilter, DocumentEditionByFunction};
use crate::routes::indexes::IndexView;
use crate::routes::multi_search::SearchResults;
use crate::routes::network::{Network, Remote};
use crate::routes::swap_indexes::SwapIndexesPayload;
use crate::routes::webhooks::{
    WebhookResults, WebhookSettings, WebhookWithMetadataRedactedAuthorization,
};
use crate::search::{
    FederatedSearch, FederatedSearchResult, Federation, FederationOptions, MergeFacets,
    SearchQueryWithIndex, SearchResultWithIndex, SimilarQuery, SimilarResult,
    INCLUDE_METADATA_HEADER,
};
use crate::search_queue::SearchQueue;
use crate::Opt;

const PAGINATION_DEFAULT_LIMIT: usize = 20;
const PAGINATION_DEFAULT_LIMIT_FN: fn() -> usize = || 20;

mod api_key;
pub mod batches;
pub mod chats;
mod dump;
mod export;
mod export_analytics;
pub mod features;
pub mod indexes;
mod logs;
mod metrics;
mod multi_search;
mod multi_search_analytics;
pub mod network;
mod open_api_utils;
mod snapshot;
mod swap_indexes;
pub mod tasks;
#[cfg(test)]
mod tasks_test;
mod webhooks;

#[derive(OpenApi)]
#[openapi(
    nest(
        (path = "/tasks", api = tasks::TaskApi),
        (path = "/batches", api = batches::BatchesApi),
        (path = "/indexes", api = indexes::IndexesApi),
        // We must stop the search path here because the rest must be configured by each route individually
        (path = "/indexes", api = indexes::search::SearchApi),
        (path = "/snapshots", api = snapshot::SnapshotApi),
        (path = "/dumps", api = dump::DumpApi),
        (path = "/keys", api = api_key::ApiKeyApi),
        (path = "/metrics", api = metrics::MetricApi),
        (path = "/logs", api = logs::LogsApi),
        (path = "/multi-search", api = multi_search::MultiSearchApi),
        (path = "/swap-indexes", api = swap_indexes::SwapIndexesApi),
        (path = "/experimental-features", api = features::ExperimentalFeaturesApi),
        (path = "/export", api = export::ExportApi),
        (path = "/network", api = network::NetworkApi),
        (path = "/webhooks", api = webhooks::WebhooksApi),
    ),
    paths(get_health, get_version, get_stats),
    tags(
        (name = "Stats", description = "Stats gives extended information and metrics about indexes and the Meilisearch database."),
    ),
    modifiers(&OpenApiAuth),
    servers((
        url = "/",
        description = "Local server",
    )),
    components(schemas(PaginationView<KeyView>, PaginationView<IndexView>, IndexView, DocumentDeletionByFilter, AllBatches, BatchStats, ProgressStepView, ProgressView, BatchView, RuntimeTogglableFeatures, SwapIndexesPayload, DocumentEditionByFunction, MergeFacets, FederationOptions, SearchQueryWithIndex, Federation, FederatedSearch, FederatedSearchResult, SearchResults, SearchResultWithIndex, SimilarQuery, SimilarResult, PaginationView<serde_json::Value>, BrowseQuery, UpdateIndexRequest, IndexUid, IndexCreateRequest, KeyView, Action, CreateApiKey, UpdateStderrLogs, LogMode, GetLogs, IndexStats, Stats, HealthStatus, HealthResponse, VersionResponse, Code, ErrorType, AllTasks, TaskView, Status, DetailsView, ResponseError, Settings<Unchecked>, Settings<Checked>, TypoSettings, MinWordSizeTyposSetting, FacetingSettings, PaginationSettings, SummarizedTaskView, Kind, Network, Remote, FilterableAttributesRule, FilterableAttributesPatterns, AttributePatterns, FilterableAttributesFeatures, FilterFeatures, Export, WebhookSettings, WebhookResults, WebhookWithMetadataRedactedAuthorization, meilisearch_types::milli::vector::VectorStoreBackend))
)]
pub struct MeilisearchApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/tasks").configure(tasks::configure))
        .service(web::scope("/batches").configure(batches::configure))
        .service(web::resource("/health").route(web::get().to(get_health)))
        .service(web::scope("/logs").configure(logs::configure))
        .service(web::scope("/keys").configure(api_key::configure))
        .service(web::scope("/dumps").configure(dump::configure))
        .service(web::scope("/snapshots").configure(snapshot::configure))
        .service(web::resource("/stats").route(web::get().to(get_stats)))
        .service(web::resource("/version").route(web::get().to(get_version)))
        .service(web::scope("/indexes").configure(indexes::configure))
        .service(web::scope("/multi-search").configure(multi_search::configure))
        .service(web::scope("/swap-indexes").configure(swap_indexes::configure))
        .service(web::scope("/metrics").configure(metrics::configure))
        .service(web::scope("/experimental-features").configure(features::configure))
        .service(web::scope("/network").configure(network::configure))
        .service(web::scope("/export").configure(export::configure))
        .service(web::scope("/chats").configure(chats::configure))
        .service(web::scope("/webhooks").configure(webhooks::configure));

    #[cfg(feature = "swagger")]
    {
        use utoipa_scalar::{Scalar, Servable as ScalarServable};
        let openapi = MeilisearchApi::openapi();
        cfg.service(Scalar::with_url("/scalar", openapi.clone()));
    }
}

pub fn get_task_id(req: &HttpRequest, opt: &Opt) -> Result<Option<TaskId>, ResponseError> {
    if !opt.experimental_replication_parameters {
        return Ok(None);
    }
    let task_id = req
        .headers()
        .get("TaskId")
        .map(|header| {
            header.to_str().map_err(|e| {
                ResponseError::from_msg(
                    format!("TaskId is not a valid utf-8 string: {e}"),
                    Code::BadRequest,
                )
            })
        })
        .transpose()?
        .map(|s| {
            s.parse::<TaskId>().map_err(|e| {
                ResponseError::from_msg(
                    format!(
                        "Could not parse the TaskId as a {}: {e}",
                        std::any::type_name::<TaskId>(),
                    ),
                    Code::BadRequest,
                )
            })
        })
        .transpose()?;
    Ok(task_id)
}

pub fn is_dry_run(req: &HttpRequest, opt: &Opt) -> Result<bool, ResponseError> {
    if !opt.experimental_replication_parameters {
        return Ok(false);
    }
    Ok(req
        .headers()
        .get("DryRun")
        .map(|header| {
            header.to_str().map_err(|e| {
                ResponseError::from_msg(
                    format!("DryRun is not a valid utf-8 string: {e}"),
                    Code::BadRequest,
                )
            })
        })
        .transpose()?
        .is_some_and(|s| s.to_lowercase() == "true"))
}

/// Parse the `Meili-Include-Metadata` header from an HTTP request.
///
/// Returns `true` if the header is present and set to "true" or "1" (case-insensitive).
/// Returns `false` if the header is not present or has any other value.
pub fn parse_include_metadata_header(req: &HttpRequest) -> bool {
    req.headers()
        .get(INCLUDE_METADATA_HEADER)
        .and_then(|h| h.to_str().ok())
        .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1"))
        .unwrap_or(false)
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SummarizedTaskView {
    /// The task unique identifier.
    #[schema(value_type = u32)]
    task_uid: TaskId,
    /// The index affected by this task. May be `null` if the task is not linked to any index.
    index_uid: Option<String>,
    /// The status of the task.
    status: Status,
    /// The type of the task.
    #[serde(rename = "type")]
    kind: Kind,
    /// The date on which the task was enqueued.
    #[serde(
        serialize_with = "time::serde::rfc3339::serialize",
        deserialize_with = "time::serde::rfc3339::deserialize"
    )]
    enqueued_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    custom_metadata: Option<String>,
}

impl From<Task> for SummarizedTaskView {
    fn from(task: Task) -> Self {
        SummarizedTaskView {
            task_uid: task.uid,
            index_uid: task.index_uid().map(|s| s.to_string()),
            status: task.status,
            kind: task.kind.as_kind(),
            enqueued_at: task.enqueued_at,
            custom_metadata: task.custom_metadata,
        }
    }
}

pub struct Pagination {
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct PaginationView<T> {
    pub results: Vec<T>,
    pub offset: usize,
    pub limit: usize,
    pub total: usize,
}

impl Pagination {
    /// Given the full data to paginate, returns the selected section.
    pub fn auto_paginate_sized<T>(
        self,
        content: impl IntoIterator<Item = T> + ExactSizeIterator,
    ) -> PaginationView<T>
    where
        T: Serialize,
    {
        let total = content.len();
        let content: Vec<_> = content.into_iter().skip(self.offset).take(self.limit).collect();
        self.format_with(total, content)
    }

    /// Given an iterator and the total number of elements, returns the selected section.
    pub fn auto_paginate_unsized<T>(
        self,
        total: usize,
        content: impl IntoIterator<Item = T>,
    ) -> PaginationView<T>
    where
        T: Serialize,
    {
        let content: Vec<_> = content.into_iter().skip(self.offset).take(self.limit).collect();
        self.format_with(total, content)
    }

    /// Given the data already paginated + the total number of elements, it stores
    /// everything in a [PaginationResult].
    pub fn format_with<T>(self, total: usize, results: Vec<T>) -> PaginationView<T>
    where
        T: Serialize,
    {
        PaginationView { results, offset: self.offset, limit: self.limit, total }
    }
}

impl<T> PaginationView<T> {
    pub fn new(offset: usize, limit: usize, total: usize, results: Vec<T>) -> Self {
        Self { offset, limit, results, total }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
#[serde(tag = "name")]
pub enum UpdateType {
    ClearAll,
    Customs,
    DocumentsAddition {
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<usize>,
    },
    DocumentsPartial {
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<usize>,
    },
    DocumentsDeletion {
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<usize>,
    },
    Settings {
        settings: Settings<Unchecked>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    pub duration: f64, // in seconds
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub processed_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    pub error: ResponseError,
    pub duration: f64, // in seconds
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub processed_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnqueuedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(skip_serializing_if = "Option::is_none", with = "time::serde::rfc3339::option")]
    pub started_processing_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum UpdateStatusResponse {
    Enqueued {
        #[serde(flatten)]
        content: EnqueuedUpdateResult,
    },
    Processing {
        #[serde(flatten)]
        content: EnqueuedUpdateResult,
    },
    Failed {
        #[serde(flatten)]
        content: FailedUpdateResult,
    },
    Processed {
        #[serde(flatten)]
        content: ProcessedUpdateResult,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

impl IndexUpdateResponse {
    pub fn with_id(update_id: u64) -> Self {
        Self { update_id }
    }
}

/// Always return a 200 with:
/// ```json
/// {
///     "status": "Meilisearch is running"
/// }
/// ```
pub async fn running() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({ "status": "Meilisearch is running" }))
}

#[derive(Serialize, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    /// The disk space used by the database, in bytes.
    pub database_size: u64,
    /// The size of the database, in bytes.
    pub used_database_size: u64,
    /// The date of the last update in the RFC 3339 formats. Can be `null` if no update has ever been processed.
    #[serde(serialize_with = "time::serde::rfc3339::option::serialize")]
    pub last_update: Option<OffsetDateTime>,
    /// The stats of every individual index your API key lets you access.
    #[schema(value_type = HashMap<String, indexes::IndexStats>)]
    pub indexes: BTreeMap<String, indexes::IndexStats>,
}

/// Get stats of all indexes.
///
/// Get stats of all indexes.
#[utoipa::path(
    get,
    path = "/stats",
    tag = "Stats",
    security(("Bearer" = ["stats.get", "stats.*", "*"])),
    responses(
        (status = 200, description = "The stats of the instance", body = Stats, content_type = "application/json", example = json!(
            {
                "databaseSize": 567,
                "usedDatabaseSize": 456,
                "lastUpdate": "2019-11-20T09:40:33.711324Z",
                "indexes": {
                    "movies": {
                        "numberOfDocuments": 10,
                        "rawDocumentDbSize": 100,
                        "maxDocumentSize": 16,
                        "avgDocumentSize": 10,
                        "isIndexing": true,
                        "fieldDistribution": {
                            "genre": 10,
                            "author": 9
                        }
                    }
                }
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
async fn get_stats(
    index_scheduler: GuardedData<ActionPolicy<{ actions::STATS_GET }>, Data<IndexScheduler>>,
    auth_controller: GuardedData<ActionPolicy<{ actions::STATS_GET }>, Data<AuthController>>,
) -> Result<HttpResponse, ResponseError> {
    let filters = index_scheduler.filters();

    let stats = create_all_stats((*index_scheduler).clone(), (*auth_controller).clone(), filters)?;

    debug!(returns = ?stats, "Get stats");
    Ok(HttpResponse::Ok().json(stats))
}

pub fn create_all_stats(
    index_scheduler: Data<IndexScheduler>,
    auth_controller: Data<AuthController>,
    filters: &meilisearch_auth::AuthFilter,
) -> Result<Stats, ResponseError> {
    let mut last_task: Option<OffsetDateTime> = None;
    let mut indexes = BTreeMap::new();
    let mut database_size = 0;
    let mut used_database_size = 0;

    for index_uid in index_scheduler.index_names()? {
        // Accumulate the size of all indexes, even unauthorized ones, so
        // as to return a database_size representative of the correct database size on disk.
        // See <https://github.com/meilisearch/meilisearch/pull/3541#discussion_r1126747643> for context.
        let stats = index_scheduler.index_stats(&index_uid)?;
        database_size += stats.inner_stats.database_size;
        used_database_size += stats.inner_stats.used_database_size;

        if !filters.is_index_authorized(&index_uid) {
            continue;
        }

        last_task = last_task.map_or(Some(stats.inner_stats.updated_at), |last| {
            Some(last.max(stats.inner_stats.updated_at))
        });
        indexes.insert(index_uid.to_string(), stats.into());
    }

    database_size += index_scheduler.size()?;
    used_database_size += index_scheduler.used_size()?;
    database_size += auth_controller.size()?;
    used_database_size += auth_controller.used_size()?;

    let stats = Stats { database_size, used_database_size, last_update: last_task, indexes };
    Ok(stats)
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct VersionResponse {
    /// The commit used to compile this build of Meilisearch.
    commit_sha: String,
    /// The date of this build.
    commit_date: String,
    /// The version of Meilisearch.
    pkg_version: String,
}

/// Get version
///
/// Current version of Meilisearch.
#[utoipa::path(
    get,
    path = "/version",
    tag = "Version",
    security(("Bearer" = ["version", "*"])),
    responses(
        (status = 200, description = "Instance is healthy", body = VersionResponse, content_type = "application/json", example = json!(
            {
                "commitSha": "b46889b5f0f2f8b91438a08a358ba8f05fc09fc1",
                "commitDate": "2021-07-08",
                "pkgVersion": "0.23.0"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
async fn get_version(
    _index_scheduler: GuardedData<ActionPolicy<{ actions::VERSION }>, Data<IndexScheduler>>,
) -> HttpResponse {
    let build_info = build_info::BuildInfo::from_build();

    HttpResponse::Ok().json(VersionResponse {
        commit_sha: build_info.commit_sha1.unwrap_or("unknown").to_string(),
        commit_date: build_info
            .commit_timestamp
            .and_then(|commit_timestamp| {
                commit_timestamp
                    .format(&time::format_description::well_known::Iso8601::DEFAULT)
                    .ok()
            })
            .unwrap_or("unknown".into()),
        pkg_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

#[derive(Default, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    /// The status of the instance.
    status: HealthStatus,
}

#[derive(Default, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
enum HealthStatus {
    #[default]
    Available,
}

/// Get Health
///
/// The health check endpoint enables you to periodically test the health of your Meilisearch instance.
#[utoipa::path(
    get,
    path = "/health",
    tag = "Health",
    responses(
        (status = 200, description = "Instance is healthy", body = HealthResponse, content_type = "application/json", example = json!(
            {
                "status": "available"
            }
        )),
    )
)]
pub async fn get_health(
    index_scheduler: Data<IndexScheduler>,
    auth_controller: Data<AuthController>,
    search_queue: Data<SearchQueue>,
) -> Result<HttpResponse, ResponseError> {
    search_queue.health().unwrap();
    index_scheduler.health().unwrap();
    auth_controller.health().unwrap();

    Ok(HttpResponse::Ok().json(HealthResponse::default()))
}

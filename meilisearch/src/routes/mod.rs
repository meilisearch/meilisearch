use std::collections::BTreeMap;
use std::fmt;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::settings::{Settings, Unchecked};
use meilisearch_types::tasks::{Kind, Status, Task, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;
use tracing::debug;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::search_queue::SearchQueue;
use crate::Opt;

const PAGINATION_DEFAULT_LIMIT: usize = 20;

mod api_key;
mod dump;
pub mod features;
pub mod indexes;
mod logs;
mod metrics;
mod multi_search;
mod snapshot;
mod swap_indexes;
pub mod tasks;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/tasks").configure(tasks::configure))
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
        .service(web::scope("/experimental-features").configure(features::configure));
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
        .map_or(false, |s| s.to_lowercase() == "true"))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SummarizedTaskView {
    task_uid: TaskId,
    index_uid: Option<String>,
    status: Status,
    #[serde(rename = "type")]
    kind: Kind,
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    enqueued_at: OffsetDateTime,
}

impl From<Task> for SummarizedTaskView {
    fn from(task: Task) -> Self {
        SummarizedTaskView {
            task_uid: task.uid,
            index_uid: task.index_uid().map(|s| s.to_string()),
            status: task.status,
            kind: task.kind.as_kind(),
            enqueued_at: task.enqueued_at,
        }
    }
}

pub struct Pagination {
    pub offset: usize,
    pub limit: usize,
}

#[derive(Clone, Serialize)]
pub struct PaginationView<T: Serialize> {
    pub results: T,
    pub offset: usize,
    pub limit: usize,
    pub total: usize,
}

impl<T: Serialize> fmt::Debug for PaginationView<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PaginationView")
            .field("offset", &self.offset)
            .field("limit", &self.limit)
            .field("total", &self.total)
            .field("results", &"[...]")
            .finish()
    }
}

impl Pagination {
    /// Given the full data to paginate, returns the selected section.
    pub fn auto_paginate_sized<T>(
        self,
        content: impl IntoIterator<Item = T> + ExactSizeIterator,
    ) -> PaginationView<Vec<T>>
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
    ) -> PaginationView<Vec<T>>
    where
        T: Serialize,
    {
        let content: Vec<_> = content.into_iter().skip(self.offset).take(self.limit).collect();
        self.format_with(total, content)
    }

    /// Given the data already paginated + the total number of elements, it stores
    /// everything in a [PaginationResult].
    pub fn format_with<T>(self, total: usize, results: Vec<T>) -> PaginationView<Vec<T>>
    where
        T: Serialize,
    {
        PaginationView { results, offset: self.offset, limit: self.limit, total }
    }
}

impl<T: Serialize> PaginationView<T> {
    pub fn new(offset: usize, limit: usize, total: usize, results: T) -> Self {
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

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub database_size: u64,
    #[serde(skip)]
    pub used_database_size: u64,
    #[serde(serialize_with = "time::serde::rfc3339::option::serialize")]
    pub last_update: Option<OffsetDateTime>,
    pub indexes: BTreeMap<String, indexes::IndexStats>,
}

async fn get_stats(
    index_scheduler: GuardedData<ActionPolicy<{ actions::STATS_GET }>, Data<IndexScheduler>>,
    auth_controller: GuardedData<ActionPolicy<{ actions::STATS_GET }>, Data<AuthController>>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish("Stats Seen".to_string(), json!({ "per_index_uid": false }), Some(&req));
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionResponse {
    commit_sha: String,
    commit_date: String,
    pkg_version: String,
}

async fn get_version(
    _index_scheduler: GuardedData<ActionPolicy<{ actions::VERSION }>, Data<IndexScheduler>>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> HttpResponse {
    analytics.publish("Version Seen".to_string(), json!(null), Some(&req));

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

#[derive(Serialize)]
struct KeysResponse {
    private: Option<String>,
    public: Option<String>,
}

pub async fn get_health(
    req: HttpRequest,
    index_scheduler: Data<IndexScheduler>,
    auth_controller: Data<AuthController>,
    search_queue: Data<SearchQueue>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.health_seen(&req);

    search_queue.health().unwrap();
    index_scheduler.health().unwrap();
    auth_controller.health().unwrap();

    Ok(HttpResponse::Ok().json(serde_json::json!({ "status": "available" })))
}

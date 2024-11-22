use std::collections::BTreeSet;
use std::convert::Infallible;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::{DeserializeError, Deserr, ValuePointerRef};
use index_scheduler::{Error, IndexScheduler};
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{immutable_field_error, DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::{self, FieldDistribution, Index};
use meilisearch_types::tasks::KindWithContent;
use serde::Serialize;
use time::OffsetDateTime;
use tracing::debug;

use super::{get_task_id, Pagination, SummarizedTaskView, PAGINATION_DEFAULT_LIMIT};
use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::is_dry_run;
use crate::Opt;

pub mod documents;
pub mod facet_search;
pub mod search;
mod search_analytics;
pub mod settings;
mod settings_analytics;
pub mod similar;
mod similar_analytics;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(list_indexes))
            .route(web::post().to(SeqHandler(create_index))),
    )
    .service(
        web::scope("/{index_uid}")
            .service(
                web::resource("")
                    .route(web::get().to(SeqHandler(get_index)))
                    .route(web::patch().to(SeqHandler(update_index)))
                    .route(web::delete().to(SeqHandler(delete_index))),
            )
            .service(web::resource("/stats").route(web::get().to(SeqHandler(get_index_stats))))
            .service(web::scope("/documents").configure(documents::configure))
            .service(web::scope("/search").configure(search::configure))
            .service(web::scope("/facet-search").configure(facet_search::configure))
            .service(web::scope("/similar").configure(similar::configure))
            .service(web::scope("/settings").configure(settings::configure)),
    );
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexView {
    pub uid: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    pub primary_key: Option<String>,
}

impl IndexView {
    fn new(uid: String, index: &Index) -> Result<IndexView, milli::Error> {
        // It is important that this function does not keep the Index handle or a clone of it, because
        // `list_indexes` relies on this property to avoid opening all indexes at once.
        let rtxn = index.read_txn()?;
        Ok(IndexView {
            uid,
            created_at: index.created_at(&rtxn)?,
            updated_at: index.updated_at(&rtxn)?,
            primary_key: index.primary_key(&rtxn)?.map(String::from),
        })
    }
}

#[derive(Deserr, Debug, Clone, Copy)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
pub struct ListIndexes {
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexOffset>)]
    pub offset: Param<usize>,
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidIndexLimit>)]
    pub limit: Param<usize>,
}
impl ListIndexes {
    fn as_pagination(self) -> Pagination {
        Pagination { offset: self.offset.0, limit: self.limit.0 }
    }
}

pub async fn list_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, Data<IndexScheduler>>,
    paginate: AwebQueryParameter<ListIndexes, DeserrQueryParamError>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?paginate, "List indexes");
    let filters = index_scheduler.filters();
    let indexes: Vec<Option<IndexView>> =
        index_scheduler.try_for_each_index(|uid, index| -> Result<Option<IndexView>, _> {
            if !filters.is_index_authorized(uid) {
                return Ok(None);
            }
            Ok(Some(IndexView::new(uid.to_string(), index).map_err(|e| Error::from_milli(e, Some(uid.to_string())))?))
        })?;
    // Won't cause to open all indexes because IndexView doesn't keep the `Index` opened.
    let indexes: Vec<IndexView> = indexes.into_iter().flatten().collect();
    let ret = paginate.as_pagination().auto_paginate_sized(indexes.into_iter());

    debug!(returns = ?ret, "List indexes");
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(Deserr, Debug)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct IndexCreateRequest {
    #[deserr(error = DeserrJsonError<InvalidIndexUid>, missing_field_error = DeserrJsonError::missing_index_uid)]
    uid: IndexUid,
    #[deserr(default, error = DeserrJsonError<InvalidIndexPrimaryKey>)]
    primary_key: Option<String>,
}

#[derive(Serialize)]
struct IndexCreatedAggregate {
    primary_key: BTreeSet<String>,
}

impl Aggregate for IndexCreatedAggregate {
    fn event_name(&self) -> &'static str {
        "Index Created"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self { primary_key: self.primary_key.union(&new.primary_key).cloned().collect() })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

pub async fn create_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_CREATE }>, Data<IndexScheduler>>,
    body: AwebJson<IndexCreateRequest, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Create index");
    let IndexCreateRequest { primary_key, uid } = body.into_inner();

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&uid);
    if allow_index_creation {
        analytics.publish(
            IndexCreatedAggregate { primary_key: primary_key.iter().cloned().collect() },
            &req,
        );

        let task = KindWithContent::IndexCreation { index_uid: uid.to_string(), primary_key };
        let uid = get_task_id(&req, &opt)?;
        let dry_run = is_dry_run(&req, &opt)?;
        let task: SummarizedTaskView =
            tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
                .await??
                .into();
        debug!(returns = ?task, "Create index");

        Ok(HttpResponse::Accepted().json(task))
    } else {
        Err(AuthenticationError::InvalidToken.into())
    }
}

fn deny_immutable_fields_index(
    field: &str,
    accepted: &[&str],
    location: ValuePointerRef,
) -> DeserrJsonError {
    match field {
        "uid" => immutable_field_error(field, accepted, Code::ImmutableIndexUid),
        "createdAt" => immutable_field_error(field, accepted, Code::ImmutableIndexCreatedAt),
        "updatedAt" => immutable_field_error(field, accepted, Code::ImmutableIndexUpdatedAt),
        _ => deserr::take_cf_content(DeserrJsonError::<BadRequest>::error::<Infallible>(
            None,
            deserr::ErrorKind::UnknownKey { key: field, accepted },
            location,
        )),
    }
}

#[derive(Deserr, Debug)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields = deny_immutable_fields_index)]
pub struct UpdateIndexRequest {
    #[deserr(default, error = DeserrJsonError<InvalidIndexPrimaryKey>)]
    primary_key: Option<String>,
}

pub async fn get_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let index = index_scheduler.index(&index_uid)?;
    let index_view = IndexView::new(index_uid.into_inner(), &index)?;

    debug!(returns = ?index_view, "Get index");

    Ok(HttpResponse::Ok().json(index_view))
}

#[derive(Serialize)]
struct IndexUpdatedAggregate {
    primary_key: BTreeSet<String>,
}

impl Aggregate for IndexUpdatedAggregate {
    fn event_name(&self) -> &'static str {
        "Index Updated"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self { primary_key: self.primary_key.union(&new.primary_key).cloned().collect() })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}
pub async fn update_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_UPDATE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<UpdateIndexRequest, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Update index");
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let body = body.into_inner();
    analytics.publish(
        IndexUpdatedAggregate { primary_key: body.primary_key.iter().cloned().collect() },
        &req,
    );

    let task = KindWithContent::IndexUpdate {
        index_uid: index_uid.into_inner(),
        primary_key: body.primary_key,
    };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Update index");
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn delete_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    opt: web::Data<Opt>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let task = KindWithContent::IndexDeletion { index_uid: index_uid.into_inner() };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();
    debug!(returns = ?task, "Delete index");

    Ok(HttpResponse::Accepted().json(task))
}

/// Stats of an `Index`, as known to the `stats` route.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct IndexStats {
    /// Number of documents in the index
    pub number_of_documents: u64,
    /// Whether the index is currently performing indexation, according to the scheduler.
    pub is_indexing: bool,
    /// Association of every field name with the number of times it occurs in the documents.
    pub field_distribution: FieldDistribution,
}

impl From<index_scheduler::IndexStats> for IndexStats {
    fn from(stats: index_scheduler::IndexStats) -> Self {
        IndexStats {
            number_of_documents: stats.inner_stats.number_of_documents,
            is_indexing: stats.is_indexing,
            field_distribution: stats.inner_stats.field_distribution,
        }
    }
}

pub async fn get_index_stats(
    index_scheduler: GuardedData<ActionPolicy<{ actions::STATS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let stats = IndexStats::from(index_scheduler.index_stats(&index_uid)?);

    debug!(returns = ?stats, "Get index stats");
    Ok(HttpResponse::Ok().json(stats))
}

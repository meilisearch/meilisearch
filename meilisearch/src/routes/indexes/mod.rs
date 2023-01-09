use std::convert::Infallible;
use std::num::ParseIntError;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::{
    DeserializeError, DeserializeFromValue, ErrorKind, IntoValue, MergeWithError, ValuePointerRef,
};
use index_scheduler::IndexScheduler;
use log::debug;
use meilisearch_types::error::{unwrap_any, Code, ErrorCode, ResponseError};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::{self, FieldDistribution, Index};
use meilisearch_types::tasks::KindWithContent;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;

use super::{Pagination, SummarizedTaskView};
use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::json::ValidatedJson;
use crate::extractors::query_parameters::QueryParameter;
use crate::extractors::sequential_extractor::SeqHandler;

pub mod documents;
pub mod search;
pub mod settings;

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
            .service(web::scope("/settings").configure(settings::configure)),
    );
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
        let rtxn = index.read_txn()?;
        Ok(IndexView {
            uid,
            created_at: index.created_at(&rtxn)?,
            updated_at: index.updated_at(&rtxn)?,
            primary_key: index.primary_key(&rtxn)?.map(String::from),
        })
    }
}

pub async fn list_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, Data<IndexScheduler>>,
    paginate: QueryParameter<Pagination, ListIndexesDeserrError>,
) -> Result<HttpResponse, ResponseError> {
    let search_rules = &index_scheduler.filters().search_rules;
    let indexes: Vec<_> = index_scheduler.indexes()?;
    let indexes = indexes
        .into_iter()
        .filter(|(name, _)| search_rules.is_index_authorized(name))
        .map(|(name, index)| IndexView::new(name, &index))
        .collect::<Result<Vec<_>, _>>()?;

    let ret = paginate.auto_paginate_sized(indexes.into_iter());

    debug!("returns: {:?}", ret);
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(Debug)]
pub struct ListIndexesDeserrError {
    error: String,
    code: Code,
}

impl std::fmt::Display for ListIndexesDeserrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for ListIndexesDeserrError {}
impl ErrorCode for ListIndexesDeserrError {
    fn error_code(&self) -> Code {
        self.code
    }
}

impl MergeWithError<ListIndexesDeserrError> for ListIndexesDeserrError {
    fn merge(
        _self_: Option<Self>,
        other: ListIndexesDeserrError,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Err(other)
    }
}

impl deserr::DeserializeError for ListIndexesDeserrError {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let code = match location.last_field() {
            Some("offset") => Code::InvalidIndexLimit,
            Some("limit") => Code::InvalidIndexOffset,
            _ => Code::BadRequest,
        };
        let error = unwrap_any(deserr::serde_json::JsonError::error(None, error, location)).0;

        Err(ListIndexesDeserrError { error, code })
    }
}

impl MergeWithError<ParseIntError> for ListIndexesDeserrError {
    fn merge(
        _self_: Option<Self>,
        other: ParseIntError,
        merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        ListIndexesDeserrError::error::<Infallible>(
            None,
            ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

#[derive(DeserializeFromValue, Debug)]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct IndexCreateRequest {
    uid: String,
    primary_key: Option<String>,
}

pub async fn create_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_CREATE }>, Data<IndexScheduler>>,
    body: ValidatedJson<IndexCreateRequest, CreateIndexesDeserrError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let IndexCreateRequest { primary_key, uid } = body.into_inner();
    let uid = IndexUid::try_from(uid)?.into_inner();

    let allow_index_creation = index_scheduler.filters().search_rules.is_index_authorized(&uid);
    if allow_index_creation {
        analytics.publish(
            "Index Created".to_string(),
            json!({ "primary_key": primary_key }),
            Some(&req),
        );

        let task = KindWithContent::IndexCreation { index_uid: uid, primary_key };
        let task: SummarizedTaskView =
            tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();

        Ok(HttpResponse::Accepted().json(task))
    } else {
        Err(AuthenticationError::InvalidToken.into())
    }
}

#[derive(Debug)]
pub struct CreateIndexesDeserrError {
    error: String,
    code: Code,
}

impl std::fmt::Display for CreateIndexesDeserrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for CreateIndexesDeserrError {}
impl ErrorCode for CreateIndexesDeserrError {
    fn error_code(&self) -> Code {
        self.code
    }
}

impl MergeWithError<CreateIndexesDeserrError> for CreateIndexesDeserrError {
    fn merge(
        _self_: Option<Self>,
        other: CreateIndexesDeserrError,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Err(other)
    }
}

impl deserr::DeserializeError for CreateIndexesDeserrError {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let code = match location.last_field() {
            Some("uid") => Code::InvalidIndexUid,
            Some("primaryKey") => Code::InvalidIndexPrimaryKey,
            None if matches!(error, ErrorKind::MissingField { field } if field == "uid") => {
                Code::MissingIndexUid
            }
            _ => Code::BadRequest,
        };
        let error = unwrap_any(deserr::serde_json::JsonError::error(None, error, location)).0;

        Err(CreateIndexesDeserrError { error, code })
    }
}

#[derive(DeserializeFromValue, Debug)]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct UpdateIndexRequest {
    primary_key: Option<String>,
}

pub async fn get_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = index_scheduler.index(&index_uid)?;
    let index_view = IndexView::new(index_uid.into_inner(), &index)?;

    debug!("returns: {:?}", index_view);

    Ok(HttpResponse::Ok().json(index_view))
}

pub async fn update_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_UPDATE }>, Data<IndexScheduler>>,
    path: web::Path<String>,
    body: ValidatedJson<UpdateIndexRequest, UpdateIndexesDeserrError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", body);
    let body = body.into_inner();
    analytics.publish(
        "Index Updated".to_string(),
        json!({ "primary_key": body.primary_key }),
        Some(&req),
    );

    let task = KindWithContent::IndexUpdate {
        index_uid: path.into_inner(),
        primary_key: body.primary_key,
    };

    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Debug)]
pub struct UpdateIndexesDeserrError {
    error: String,
    code: Code,
}

impl std::fmt::Display for UpdateIndexesDeserrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for UpdateIndexesDeserrError {}
impl ErrorCode for UpdateIndexesDeserrError {
    fn error_code(&self) -> Code {
        self.code
    }
}

impl MergeWithError<UpdateIndexesDeserrError> for UpdateIndexesDeserrError {
    fn merge(
        _self_: Option<Self>,
        other: UpdateIndexesDeserrError,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Err(other)
    }
}

impl deserr::DeserializeError for UpdateIndexesDeserrError {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let code = match location.last_field() {
            Some("primaryKey") => Code::InvalidIndexPrimaryKey,
            _ => Code::BadRequest,
        };
        let error = unwrap_any(deserr::serde_json::JsonError::error(None, error, location)).0;

        Err(UpdateIndexesDeserrError { error, code })
    }
}

pub async fn delete_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let task = KindWithContent::IndexDeletion { index_uid: index_uid.into_inner() };
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();

    Ok(HttpResponse::Accepted().json(task))
}

pub async fn get_index_stats(
    index_scheduler: GuardedData<ActionPolicy<{ actions::STATS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish("Stats Seen".to_string(), json!({ "per_index_uid": true }), Some(&req));

    let stats = IndexStats::new((*index_scheduler).clone(), index_uid.into_inner())?;

    debug!("returns: {:?}", stats);
    Ok(HttpResponse::Ok().json(stats))
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct IndexStats {
    pub number_of_documents: u64,
    pub is_indexing: bool,
    pub field_distribution: FieldDistribution,
}

impl IndexStats {
    pub fn new(
        index_scheduler: Data<IndexScheduler>,
        index_uid: String,
    ) -> Result<Self, ResponseError> {
        // we check if there is currently a task processing associated with this index.
        let is_processing = index_scheduler.is_index_processing(&index_uid)?;
        let index = index_scheduler.index(&index_uid)?;
        let rtxn = index.read_txn()?;
        Ok(IndexStats {
            number_of_documents: index.number_of_documents(&rtxn)?,
            is_indexing: is_processing,
            field_distribution: index.field_distribution(&rtxn)?,
        })
    }
}

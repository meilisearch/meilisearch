use std::convert::Infallible;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::{DeserializeError, DeserializeFromValue, ValuePointerRef};
use index_scheduler::IndexScheduler;
use log::debug;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{unwrap_any, Code, DeserrError, ResponseError, TakeErrorMessage};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::{self, FieldDistribution, Index};
use meilisearch_types::tasks::KindWithContent;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;

use self::search::parse_usize_take_error_message;
use super::{Pagination, SummarizedTaskView, PAGINATION_DEFAULT_LIMIT};
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

#[derive(DeserializeFromValue, Deserialize, Debug, Clone, Copy)]
#[deserr(error = DeserrError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListIndexes {
    #[serde(default)]
    #[deserr(error = DeserrError<InvalidIndexOffset>, default, from(&String) = parse_usize_take_error_message -> TakeErrorMessage<std::num::ParseIntError>)]
    pub offset: usize,
    #[serde(default = "PAGINATION_DEFAULT_LIMIT")]
    #[deserr(error = DeserrError<InvalidIndexLimit>, default = PAGINATION_DEFAULT_LIMIT(), from(&String) = parse_usize_take_error_message -> TakeErrorMessage<std::num::ParseIntError>)]
    pub limit: usize,
}
impl ListIndexes {
    fn as_pagination(self) -> Pagination {
        Pagination { offset: self.offset, limit: self.limit }
    }
}

pub async fn list_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, Data<IndexScheduler>>,
    paginate: QueryParameter<ListIndexes, DeserrError>,
) -> Result<HttpResponse, ResponseError> {
    let search_rules = &index_scheduler.filters().search_rules;
    let indexes: Vec<_> = index_scheduler.indexes()?;
    let indexes = indexes
        .into_iter()
        .filter(|(name, _)| search_rules.is_index_authorized(name))
        .map(|(name, index)| IndexView::new(name, &index))
        .collect::<Result<Vec<_>, _>>()?;

    let ret = paginate.as_pagination().auto_paginate_sized(indexes.into_iter());

    debug!("returns: {:?}", ret);
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(DeserializeFromValue, Debug)]
#[deserr(error = DeserrError, rename_all = camelCase, deny_unknown_fields)]
pub struct IndexCreateRequest {
    #[deserr(error = DeserrError<InvalidIndexUid>, missing_field_error = DeserrError::missing_index_uid)]
    uid: String,
    #[deserr(error = DeserrError<InvalidIndexPrimaryKey>)]
    primary_key: Option<String>,
}

pub async fn create_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_CREATE }>, Data<IndexScheduler>>,
    body: ValidatedJson<IndexCreateRequest, DeserrError>,
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

fn deny_immutable_fields_index(
    field: &str,
    accepted: &[&str],
    location: ValuePointerRef,
) -> DeserrError {
    let mut error = unwrap_any(DeserrError::<BadRequest>::error::<Infallible>(
        None,
        deserr::ErrorKind::UnknownKey { key: field, accepted },
        location,
    ));

    error.code = match field {
        "uid" => Code::ImmutableIndexUid,
        "createdAt" => Code::ImmutableIndexCreatedAt,
        "updatedAt" => Code::ImmutableIndexUpdatedAt,
        _ => Code::BadRequest,
    };
    error
}
#[derive(DeserializeFromValue, Debug)]
#[deserr(error = DeserrError, rename_all = camelCase, deny_unknown_fields = deny_immutable_fields_index)]
pub struct UpdateIndexRequest {
    #[deserr(error = DeserrError<InvalidIndexPrimaryKey>)]
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
    body: ValidatedJson<UpdateIndexRequest, DeserrError>,
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

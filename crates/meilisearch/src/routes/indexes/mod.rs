use std::collections::BTreeSet;
use std::convert::Infallible;

use super::{
    get_task_id, Pagination, PaginationView, SummarizedTaskView, PAGINATION_DEFAULT_LIMIT,
};
use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::proxy::{proxy, task_network_and_check_leader_and_version, Body};
use crate::routes::is_dry_run;
use crate::Opt;
use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::{DeserializeError, Deserr, ValuePointerRef};
use globset::{Glob, GlobMatcher};
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{immutable_field_error, DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::tokenizer::Language;
use meilisearch_types::milli::{
    self, FieldDistribution, FilterableAttributesRule, Index, MetadataBuilder, OrderBy,
};
use meilisearch_types::tasks::KindWithContent;
use regex::Regex;
use serde::Serialize;
use time::OffsetDateTime;
use tracing::debug;
use utoipa::{IntoParams, OpenApi, ToSchema};

pub mod compact;
pub mod documents;

pub mod facet_search;
pub mod search;
mod search_analytics;
#[cfg(test)]
mod search_test;
pub mod settings;
mod settings_analytics;
pub mod similar;
mod similar_analytics;

#[derive(OpenApi)]
#[openapi(
    nest(
        (path = "/", api = documents::DocumentsApi),
        (path = "/", api = facet_search::FacetSearchApi),
        (path = "/", api = similar::SimilarApi),
        (path = "/", api = settings::SettingsApi),
        (path = "/", api = compact::CompactApi),
    ),
    paths(list_indexes, create_index, get_index, update_index, delete_index, get_index_stats, compact::compact),
    tags(
        (
            name = "Indexes",
            description = "An index is an entity that gathers a set of [documents](https://www.meilisearch.com/docs/learn/getting_started/documents) with its own [settings](https://www.meilisearch.com/docs/reference/api/settings). Learn more about indexes.",
            external_docs(url = "https://www.meilisearch.com/docs/reference/api/indexes"),
        ),
    ),
)]
pub struct IndexesApi;

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
            .service(web::resource("/fields").route(web::post().to(SeqHandler(post_index_fields))))
            .service(web::scope("/documents").configure(documents::configure))
            .service(web::scope("/search").configure(search::configure))
            .service(web::scope("/facet-search").configure(facet_search::configure))
            .service(web::scope("/similar").configure(similar::configure))
            .service(web::scope("/settings").configure(settings::configure))
            .service(web::scope("/compact").configure(compact::configure)),
    );
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IndexView {
    /// Unique identifier for the index
    pub uid: String,
    /// An `RFC 3339` format for date/time/duration.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// An `RFC 3339` format for date/time/duration.
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    /// Custom primaryKey for documents
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

#[derive(Deserr, Debug, Clone, Copy, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct ListIndexes {
    /// The number of indexes to skip before starting to retrieve anything
    #[param(value_type = Option<usize>, default, example = 100)]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexOffset>)]
    pub offset: Param<usize>,
    /// The number of indexes to retrieve
    #[param(value_type = Option<usize>, default = 20, example = 1)]
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidIndexLimit>)]
    pub limit: Param<usize>,
}

impl ListIndexes {
    fn as_pagination(self) -> Pagination {
        Pagination { offset: self.offset.0, limit: self.limit.0 }
    }
}

/// List indexes
///
/// List all indexes.
#[utoipa::path(
    get,
    path = "",
    tag = "Indexes",
    security(("Bearer" = ["indexes.get", "indexes.*", "*"])),
    params(ListIndexes),
    responses(
        (status = 200, description = "Indexes are returned", body = PaginationView<IndexView>, content_type = "application/json", example = json!(
            {
                "results": [
                    {
                        "uid": "movies",
                        "primaryKey": "movie_id",
                        "createdAt": "2019-11-20T09:40:33.711324Z",
                        "updatedAt": "2019-11-20T09:40:33.711324Z"
                    }
                ],
                "limit": 1,
                "offset": 0,
                "total": 1
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
pub async fn list_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, Data<IndexScheduler>>,
    paginate: AwebQueryParameter<ListIndexes, DeserrQueryParamError>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?paginate, "List indexes");
    let filters = index_scheduler.filters();
    let (total, indexes) =
        index_scheduler.paginated_indexes_stats(filters, *paginate.offset, *paginate.limit)?;
    let indexes = indexes
        .into_iter()
        .map(|(name, stats)| IndexView {
            uid: name,
            created_at: stats.created_at,
            updated_at: stats.updated_at,
            primary_key: stats.primary_key,
        })
        .collect::<Vec<_>>();
    let ret = paginate.as_pagination().format_with(total, indexes);

    debug!(returns = ?ret, "List indexes");
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(Deserr, Serialize, Debug, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct IndexCreateRequest {
    /// The name of the index
    #[schema(example = "movies")]
    #[deserr(error = DeserrJsonError<InvalidIndexUid>, missing_field_error = DeserrJsonError::missing_index_uid)]
    uid: IndexUid,
    /// The primary key of the index
    #[schema(example = "id")]
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

/// Create index
///
/// Create an index.
#[utoipa::path(
    post,
    path = "",
    tag = "Indexes",
    security(("Bearer" = ["indexes.create", "indexes.*", "*"])),
    request_body = IndexCreateRequest,
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": "movies",
                "status": "enqueued",
                "type": "indexCreation",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
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
pub async fn create_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_CREATE }>, Data<IndexScheduler>>,
    body: AwebJson<IndexCreateRequest, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Create index");

    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    let IndexCreateRequest { primary_key, uid } = body.into_inner();

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&uid);
    if allow_index_creation {
        analytics.publish(
            IndexCreatedAggregate { primary_key: primary_key.iter().cloned().collect() },
            &req,
        );

        let task = KindWithContent::IndexCreation {
            index_uid: uid.to_string(),
            primary_key: primary_key.clone(),
        };
        let tuid = get_task_id(&req, &opt)?;
        let dry_run = is_dry_run(&req, &opt)?;
        let scheduler = index_scheduler.clone();
        let mut task = tokio::task::spawn_blocking(move || {
            scheduler.register_with_custom_metadata(task, tuid, None, dry_run, task_network)
        })
        .await??;

        if let Some(task_network) = task.network.take() {
            proxy(
                &index_scheduler,
                None,
                &req,
                task_network,
                network,
                Body::inline(IndexCreateRequest { primary_key, uid }),
                &task,
            )
            .await?;
        }

        let task = SummarizedTaskView::from(task);
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
        "createdAt" => immutable_field_error(field, accepted, Code::ImmutableIndexCreatedAt),
        "updatedAt" => immutable_field_error(field, accepted, Code::ImmutableIndexUpdatedAt),
        _ => deserr::take_cf_content(DeserrJsonError::<BadRequest>::error::<Infallible>(
            None,
            deserr::ErrorKind::UnknownKey { key: field, accepted },
            location,
        )),
    }
}

/// Get index
///
/// Get information about an index.
#[utoipa::path(
    get,
    path = "/{indexUid}",
    tag = "Indexes",
    security(("Bearer" = ["indexes.get", "indexes.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    responses(
        (status = 200, description = "The index is returned", body = IndexView, content_type = "application/json", example = json!(
            {
                "uid": "movies",
                "primaryKey": "movie_id",
                "createdAt": "2019-11-20T09:40:33.711324Z",
                "updatedAt": "2019-11-20T09:40:33.711324Z"
            }
        )),
        (status = 404, description = "Index not found", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
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

#[derive(Deserr, Serialize, Debug, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields = deny_immutable_fields_index)]
#[schema(rename_all = "camelCase")]
pub struct UpdateIndexRequest {
    /// The new primary key of the index
    #[deserr(default, error = DeserrJsonError<InvalidIndexPrimaryKey>)]
    primary_key: Option<String>,
    /// The new uid of the index (for renaming)
    #[deserr(default, error = DeserrJsonError<InvalidIndexUid>)]
    uid: Option<String>,
}

/// Update index
///
/// Update the `primaryKey` of an index.
/// Return an error if the index doesn't exists yet or if it contains documents.
#[utoipa::path(
    patch,
    path = "/{indexUid}",
    tag = "Indexes",
    security(("Bearer" = ["indexes.update", "indexes.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    request_body = UpdateIndexRequest,
    responses(
        (status = ACCEPTED, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 0,
                "indexUid": "movies",
                "status": "enqueued",
                "type": "indexUpdate",
                "enqueuedAt": "2021-01-01T09:39:00.000000Z"
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
pub async fn update_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_UPDATE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<UpdateIndexRequest, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Update index");

    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let body = body.into_inner();

    // Validate new uid if provided
    if let Some(ref new_uid) = body.uid {
        let _ = IndexUid::try_from(new_uid.clone())?;
    }

    analytics.publish(
        IndexUpdatedAggregate { primary_key: body.primary_key.iter().cloned().collect() },
        &req,
    );

    let task = KindWithContent::IndexUpdate {
        index_uid: index_uid.clone().into_inner(),
        primary_key: body.primary_key.clone(),
        new_index_uid: body.uid.clone(),
    };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let scheduler = index_scheduler.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        scheduler.register_with_custom_metadata(task, uid, None, dry_run, task_network)
    })
    .await??;

    if let Some(task_network) = task.network.take() {
        proxy(
            &index_scheduler,
            Some(&index_uid),
            &req,
            task_network,
            network,
            Body::inline(body),
            &task,
        )
        .await?;
    }

    let task = SummarizedTaskView::from(task);

    debug!(returns = ?task, "Update index");
    Ok(HttpResponse::Accepted().json(task))
}

/// Delete index
///
/// Delete an index.
#[utoipa::path(
    delete,
    path = "/{indexUid}",
    tag = "Indexes",
    security(("Bearer" = ["indexes.delete", "indexes.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    responses(
        (status = ACCEPTED, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 0,
                "indexUid": "movies",
                "status": "enqueued",
                "type": "indexDeletion",
                "enqueuedAt": "2021-01-01T09:39:00.000000Z"
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
pub async fn delete_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    opt: web::Data<Opt>,
) -> Result<HttpResponse, ResponseError> {
    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let task = KindWithContent::IndexDeletion { index_uid: index_uid.clone().into_inner() };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let scheduler = index_scheduler.clone();

    let mut task = tokio::task::spawn_blocking(move || {
        scheduler.register_with_custom_metadata(task, uid, None, dry_run, task_network)
    })
    .await??;

    if let Some(task_network) = task.network.take() {
        proxy(&index_scheduler, Some(&index_uid), &req, task_network, network, Body::none(), &task)
            .await?;
    }

    let task = SummarizedTaskView::from(task);

    debug!(returns = ?task, "Delete index");

    Ok(HttpResponse::Accepted().json(task))
}

/// Stats of an `Index`, as known to the `stats` route.
#[derive(Serialize, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IndexStats {
    /// Number of documents in the index
    pub number_of_documents: u64,
    /// Size of the documents database, in bytes.
    pub raw_document_db_size: u64,
    /// Average size of a document in the documents database.
    pub avg_document_size: u64,
    /// Whether or not the index is currently ingesting document
    pub is_indexing: bool,
    /// Number of embeddings in the index
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number_of_embeddings: Option<u64>,
    /// Number of embedded documents in the index
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number_of_embedded_documents: Option<u64>,
    /// Association of every field name with the number of times it occurs in the documents.
    #[schema(value_type = HashMap<String, u64>)]
    pub field_distribution: FieldDistribution,
}

impl From<index_scheduler::IndexStats> for IndexStats {
    fn from(stats: index_scheduler::IndexStats) -> Self {
        IndexStats {
            number_of_documents: stats
                .inner_stats
                .number_of_documents
                .unwrap_or(stats.inner_stats.documents_database_stats.number_of_entries()),
            raw_document_db_size: stats.inner_stats.documents_database_stats.total_size(),
            avg_document_size: stats.inner_stats.documents_database_stats.average_value_size(),
            is_indexing: stats.is_indexing,
            number_of_embeddings: stats.inner_stats.number_of_embeddings,
            number_of_embedded_documents: stats.inner_stats.number_of_embedded_documents,
            field_distribution: stats.inner_stats.field_distribution,
        }
    }
}

/// Get stats of index
///
/// Get the stats of an index.
#[utoipa::path(
    get,
    path = "/{indexUid}/stats",
    tag = "Stats",
    security(("Bearer" = ["stats.get", "stats.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    responses(
        (status = OK, description = "The stats of the index", body = IndexStats, content_type = "application/json", example = json!(
            {
                "numberOfDocuments": 10,
                "rawDocumentDbSize": 10,
                "avgDocumentSize": 10,
                "numberOfEmbeddings": 10,
                "numberOfEmbeddedDocuments": 10,
                "isIndexing": true,
                "fieldDistribution": {
                    "genre": 10,
                    "author": 9
                }
            }
        )),
        (status = 404, description = "Index not found", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
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
pub async fn get_index_stats(
    index_scheduler: GuardedData<ActionPolicy<{ actions::STATS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let stats = IndexStats::from(index_scheduler.index_stats(&index_uid)?);

    debug!(returns = ?stats, "Get index stats");
    Ok(HttpResponse::Ok().json(stats))
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Field<'a> {
    pub name: &'a str,
    pub displayed: FieldDisplayConfig,
    pub searchable: FieldSearchConfig,
    pub distinct: FieldDistinctConfig,
    pub filterable: FieldFilterableConfig<'a>,
    pub localized: FieldLocalizedConfig<'a>,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldDisplayConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldSearchConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldDistinctConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldFacetSearchConfig<'a> {
    pub sort_by: &'a str,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldFilterConfig {
    pub equality: bool,
    pub comparison: bool,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldFilterableConfig<'a> {
    pub enabled: bool,
    pub facet_search: FieldFacetSearchConfig<'a>,
    pub filter: FieldFilterConfig,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldLocalizedConfig<'a> {
    #[schema(value_type = Vec<String>)]
    pub locales: &'a [Language],
}

#[derive(Deserr, Debug, Clone, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct ListFields {
    #[deserr(default, error = DeserrJsonError<InvalidIndexOffset>)]
    pub offset: usize,
    #[deserr(default = PAGINATION_DEFAULT_LIMIT, error = DeserrJsonError<InvalidIndexLimit>)]
    pub limit: usize,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilter>)]
    pub filter: Option<ListFieldsFilter>,
}

impl ListFields {
    fn apply_filter(&self, field: &Field) -> bool {
        if let Some(filter) = &self.filter {
            if let Some(value) = &filter.starts_with {
                if !field.name.starts_with(value) {
                    return false;
                }
            }

            if let Some(value) = &filter.contains {
                if !field.name.contains(value) {
                    return false;
                }
            }

            if let Some(regex) = &filter.regex {
                if !regex.is_match(field.name) {
                    return false;
                }
            }

            if let Some(glob) = &filter.glob {
                if !glob.is_match(field.name) {
                    return false;
                }
            }

            if let Some(displayed) = &filter.displayed {
                if *displayed != field.displayed.enabled {
                    return false;
                }
            }

            return true;
        }

        true
    }
}

#[derive(Deserr, Debug, Clone, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidIndexFieldsFilter>, rename_all = camelCase, deny_unknown_fields)]
pub struct ListFieldsFilter {
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterStartsWith>)]
    pub starts_with: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterContains>)]
    pub contains: Option<String>,
    #[deserr(default, try_from(&String) = from_string_regex -> DeserrJsonError<InvalidIndexFieldsFilterRegex>, error = DeserrJsonError<InvalidIndexFieldsFilterRegex>)]
    #[schema(value_type = String)]
    pub regex: Option<Regex>,
    #[deserr(default, try_from(&String) = from_string_glob -> DeserrJsonError<InvalidIndexFieldsFilterGlob>, error = DeserrJsonError<InvalidIndexFieldsFilterGlob>)]
    #[schema(value_type = String)]
    pub glob: Option<GlobMatcher>,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterDisplayed>)]
    pub displayed: Option<bool>,
}

fn from_string_regex(
    value: &str,
) -> Result<Option<Regex>, DeserrJsonError<InvalidIndexFieldsFilterRegex>> {
    Regex::new(value).map(Some).map_err(|err| {
        DeserrJsonError::new(
            format!("invalid regex pattern: {}", err),
            Code::InvalidIndexFieldsFilterRegex,
        )
    })
}

fn from_string_glob(
    value: &str,
) -> Result<Option<GlobMatcher>, DeserrJsonError<InvalidIndexFieldsFilterGlob>> {
    Glob::new(value).map(|glob| Some(glob.compile_matcher())).map_err(|err| {
        DeserrJsonError::new(
            format!("invalid glob pattern: {}", err),
            Code::InvalidIndexFieldsFilterGlob,
        )
    })
}

#[utoipa::path(
    post,
    path = "/{indexUid}/fields",
    tag = "Fields",
    security(("Bearer" = ["fields.post", "fields.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    request_body = ListFields,
)]
pub async fn post_index_fields(
    index_scheduler: GuardedData<ActionPolicy<{ actions::FIELDS_POST }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<ListFields, DeserrJsonError>,
) -> Result<HttpResponse, ResponseError> {
    let index = index_scheduler.index(index_uid.as_str())?;
    let rtxn = index.read_txn()?;
    let builder = MetadataBuilder::from_index(&index, &rtxn)?;
    let fields = builder
        .fields_metadata()
        .iter()
        .filter_map(|(name, metadata)| {
            let is_filterable = builder.filterable_attributes().iter().any(|rule| match rule {
                FilterableAttributesRule::Pattern(p) => {
                    p.match_str(name) == milli::PatternMatch::Match
                }
                FilterableAttributesRule::Field(f) => f == name,
            });

            let locales = builder
                .localized_attributes_rules()
                .and_then(|rules| metadata.locales(rules))
                .unwrap_or_default();

            let field = Field {
                name,
                displayed: FieldDisplayConfig { enabled: metadata.displayed },
                searchable: FieldSearchConfig { enabled: metadata.searchable.is_some() },
                distinct: FieldDistinctConfig { enabled: metadata.distinct },
                filterable: FieldFilterableConfig {
                    enabled: is_filterable,
                    facet_search: FieldFacetSearchConfig {
                        sort_by: match metadata.sort_by {
                            OrderBy::Lexicographic => "alpha",
                            OrderBy::Count => "count",
                        },
                    },
                    filter: FieldFilterConfig {
                        equality: is_filterable,
                        comparison: is_filterable,
                    },
                },
                localized: FieldLocalizedConfig { locales },
            };

            if !body.0.apply_filter(&field) {
                return None;
            }

            Some(field)
        })
        .skip(body.0.offset)
        .take(body.0.limit)
        .collect::<Vec<_>>();

    Ok(HttpResponse::Ok().json(fields))
}

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::io::{ErrorKind, Seek as _};
use std::marker::PhantomData;
use std::str::FromStr;

use actix_web::http::header::CONTENT_TYPE;
use actix_web::web::Data;
use actix_web::{web, HttpMessage, HttpRequest, HttpResponse};
use bstr::ByteSlice as _;
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::Deserr;
use futures::StreamExt;
use index_scheduler::filter::{filter_into_index_filter, parse_filter, parse_local_index_filter};
use index_scheduler::{IndexScheduler, TaskId};
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::document_formats::{read_csv, read_json, read_ndjson, PayloadType};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::constants::{
    RESERVED_GEO_FIELD_NAME, RESERVED_GEO_LAT_FIELD_NAME, RESERVED_GEO_LNG_FIELD_NAME,
};
use meilisearch_types::milli::documents::sort::recursive_sort;
use meilisearch_types::milli::index::EmbeddingsWithMetadata;
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::score_details::{GeoSort, WeightedScoreValue};
use meilisearch_types::milli::update::{IndexDocumentsMethod, MissingDocumentPolicy};
use meilisearch_types::milli::vector::parsed_vectors::ExplicitVectors;
use meilisearch_types::milli::{make_document, AscDesc, DocumentId, IndexFilter, Member};
use meilisearch_types::network::Network;
use meilisearch_types::serde_cs::vec::CS;
use meilisearch_types::star_or::OptionStarOrList;
use meilisearch_types::tasks::KindWithContent;
use meilisearch_types::{milli, Document, Index};
use mime::Mime;
use once_cell::sync::Lazy;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use crate::analytics::{Aggregate, AggregateMethod, Analytics};
use crate::error::MeilisearchHttpError;
use crate::error::PayloadError::ReceivePayload;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::payload::Payload;
use crate::proxy::{proxy, task_network_and_check_leader_and_version, Body};
use crate::routes::indexes::search::fix_sort_query_parameters;
use crate::routes::{
    get_task_id, is_dry_run, PaginationView, SummarizedTaskView, PAGINATION_DEFAULT_LIMIT,
    PAGINATION_DEFAULT_LIMIT_FN,
};
use crate::search::federated::weighted_scores;
use crate::search::proxy::{
    json_proxy, ProxySearchError, ProxySearchParams, PROXY_SEARCH_HEADER, PROXY_SEARCH_HEADER_VALUE,
};
use crate::search::{
    ExternalDocumentId, NetworkableQuery, Partition, ProxyQuery, RetrieveVectors, VisitFacetValues,
};
use crate::{aggregate_methods, Opt};

static ACCEPTED_CONTENT_TYPE: Lazy<Vec<String>> = Lazy::new(|| {
    vec!["application/json".to_string(), "application/x-ndjson".to_string(), "text/csv".to_string()]
});
use crate::search::federated::types::{FEDERATION_HIT, WEIGHTED_SCORE_VALUES};

/// Extracts the mime type from the content type and return
/// a meilisearch error if anything bad happen.
fn extract_mime_type(req: &HttpRequest) -> Result<Option<Mime>, MeilisearchHttpError> {
    match req.mime_type() {
        Ok(Some(mime)) => Ok(Some(mime)),
        Ok(None) => Ok(None),
        Err(_) => match req.headers().get(CONTENT_TYPE) {
            Some(content_type) => Err(MeilisearchHttpError::InvalidContentType(
                content_type.as_bytes().as_bstr().to_string(),
                ACCEPTED_CONTENT_TYPE.clone(),
            )),
            None => Err(MeilisearchHttpError::MissingContentType(ACCEPTED_CONTENT_TYPE.clone())),
        },
    }
}

#[derive(Deserialize)]
pub struct DocumentParam {
    index_uid: String,
    document_id: String,
}

#[routes::routes(
    routes(
        "" => [get(get_documents), post(replace_documents), put(update_documents), delete(clear_all_documents)],
        "/delete-batch" => post(delete_documents_batch),
        "/delete" => post(delete_documents_by_filter),
        "/edit" => post(edit_documents_by_function),
        "/fetch" => post(documents_by_query_post),
        "/{document_id}" => [get(get_document), delete(delete_document)],
    ),
    tag = "Documents",
    tags(
        (
            name = "Documents",
            description = "Documents are objects composed of fields that can store any type of data. Each field contains an attribute and its associated value. Documents are stored inside [indexes](https://www.meilisearch.com/docs/learn/getting_started/indexes).",
        ),
    ),
)]
pub struct DocumentsApi;

#[derive(Debug, Deserr, IntoParams, ToSchema)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
#[schema(rename_all = "camelCase")]
pub struct GetDocument {
    /// Comma-separated list of document attributes to include in the
    /// response. Use `*` to retrieve all attributes. By default, all
    /// attributes listed in the `displayedAttributes` setting are returned.
    /// Example: `title,description,price`.
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentFields>)]
    #[param(required = false, value_type = Option<Vec<String>>)]
    #[schema(value_type = Option<Vec<String>>)]
    fields: OptionStarOrList<String>,
    /// When `true`, includes the vector embeddings in the response for this
    /// document. This is useful when you need to inspect or export vector
    /// data. Note that this can significantly increase response size if the
    /// document has multiple embedders configured. Defaults to `false`.
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentRetrieveVectors>)]
    #[param(required = false, value_type = Option<bool>)]
    #[schema(value_type = Option<bool>)]
    retrieve_vectors: Param<bool>,
    /// When `true`, runs the query on the whole network (all shards covered exactly once).
    ///
    /// When `false`, the query runs locally.
    ///
    /// When omitted or `null`, the default value depends on whether the sharding is enabled for the instance:
    ///
    /// - If the instance has sharding enabled (has a leader), defaults to `true`.
    /// - Otherwise defaults to `false`.
    ///
    /// It also requires the `network` [experimental feature](http://localhost:3000/reference/api/experimental-features/configure-experimental-features).
    ///
    /// Values: `true` = use the whole network; `false` = local, default = see above.
    ///
    /// When using the network, the index must exist with compatible settings on all remotes.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentUseNetwork>)]
    use_network: Option<bool>,
}

impl NetworkableQuery for GetDocument {
    fn use_network_field(&mut self) -> &mut Option<bool> {
        &mut self.use_network
    }

    fn has_remote(&self) -> bool {
        false
    }
}

aggregate_methods!(
    DocumentsGET => "Documents Fetched GET",
    DocumentsPOST => "Documents Fetched POST",
);

#[derive(Serialize)]
pub struct DocumentsFetchAggregator<Method: AggregateMethod> {
    // a call on ../documents/:doc_id
    per_document_id: bool,
    // if a filter was used
    per_filter: bool,
    with_vector_filter: bool,

    // if documents were sorted
    sort: bool,

    #[serde(rename = "vector.retrieve_vectors")]
    retrieve_vectors: bool,

    // maximum size of `ids` array. 0 if always empty or `null`
    max_document_ids: usize,

    // pagination
    #[serde(rename = "pagination.max_limit")]
    max_limit: usize,
    #[serde(rename = "pagination.max_offset")]
    max_offset: usize,

    marker: std::marker::PhantomData<Method>,
}

impl<Method: AggregateMethod> Aggregate for DocumentsFetchAggregator<Method> {
    fn event_name(&self) -> &'static str {
        Method::event_name()
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            per_document_id: self.per_document_id | new.per_document_id,
            per_filter: self.per_filter | new.per_filter,
            with_vector_filter: self.with_vector_filter | new.with_vector_filter,
            sort: self.sort | new.sort,
            retrieve_vectors: self.retrieve_vectors | new.retrieve_vectors,
            max_limit: self.max_limit.max(new.max_limit),
            max_offset: self.max_offset.max(new.max_offset),
            max_document_ids: self.max_document_ids.max(new.max_document_ids),
            marker: PhantomData,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Get document
///
/// Retrieve a single document by its [primary key](https://www.meilisearch.com/docs/learn/getting_started/primary_key) value.
#[routes::path(
    security(("Bearer" = ["documents.get", "documents.*", "*"])),
    params(
        ("index_uid" = String, Path, example = "movies", description = "Unique identifier of the index.", nullable = false),
        ("document_id" = String, Path, example = "85087", description = "The document identifier.", nullable = false),
        GetDocument,
   ),
    responses(
        (status = 200, description = "The document is returned.", body = serde_json::Value, content_type = "application/json", example = json!(
            {
                "id": 25684,
                "title": "American Ninja 5",
                "poster": "https://image.tmdb.org/t/p/w1280/iuAQVI4mvjI83wnirpD8GVNRVuY.jpg",
                "overview": "When a scientists daughter is kidnapped, American Ninja, attempts to find her, but this time he teams up with a youngster he has trained in the ways of the ninja.",
                "release_date": 725846400
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
        (status = 404, description = "Document not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
              "message": "Document :uid not found.",
              "code": "document_not_found",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#document_not_found"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn get_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    document_param: web::Path<DocumentParam>,
    params: AwebQueryParameter<GetDocument, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let DocumentParam { index_uid, document_id } = document_param.into_inner();
    debug!(parameters = ?params, "Get document");
    let index_uid = IndexUid::try_from(index_uid)?;

    let features = index_scheduler.features();
    let network = index_scheduler.network();
    let mut query = params.into_inner();
    let must_use_network = query.must_use_network(&network, &features)?;
    let GetDocument { fields, retrieve_vectors: param_retrieve_vectors, use_network: _ } = query;
    let attributes_to_retrieve = fields.merge_star_and_none();

    let retrieve_vectors = RetrieveVectors::new(param_retrieve_vectors.0);

    analytics.publish(
        DocumentsFetchAggregator::<DocumentsGET> {
            retrieve_vectors: retrieve_vectors == RetrieveVectors::Retrieve,
            per_document_id: true,
            per_filter: false,
            with_vector_filter: false,
            sort: false,
            max_limit: 0,
            max_offset: 0,
            max_document_ids: 0,
            marker: PhantomData,
        },
        &req,
    );

    let index = index_scheduler.user_index(&index_uid)?;

    // Try to retrieve the document locally first
    let local_result = retrieve_document(
        &index,
        &document_id,
        attributes_to_retrieve.as_deref(),
        retrieve_vectors,
    );

    // If the document is not found locally, try to retrieve it from the network if it is enabled
    let document = if must_use_network && local_result.is_err() {
        let query = BrowseQuery {
            offset: 0,
            limit: 1,
            fields: attributes_to_retrieve,
            retrieve_vectors: retrieve_vectors == RetrieveVectors::Retrieve,
            filter: None,
            ids: Some(vec![serde_json::Value::String(document_id.clone())]),
            sort: None,
            use_network: Some(true),
        };
        let mut ret =
            retrieve_documents_federated(index_scheduler.clone(), index_uid, query, network)
                .await?;
        ret.results.pop().ok_or_else(|| MeilisearchHttpError::DocumentNotFound(document_id))?
    } else {
        local_result?
    };

    debug!(returns = ?document, "Get document");
    Ok(HttpResponse::Ok().json(document))
}

#[derive(Serialize)]
pub struct DocumentsDeletionAggregator {
    per_document_id: bool,
    clear_all: bool,
    per_batch: bool,
    per_filter: bool,
}

impl Aggregate for DocumentsDeletionAggregator {
    fn event_name(&self) -> &'static str {
        "Documents Deleted"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            per_document_id: self.per_document_id | new.per_document_id,
            clear_all: self.clear_all | new.clear_all,
            per_batch: self.per_batch | new.per_batch,
            per_filter: self.per_filter | new.per_filter,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Delete document
///
/// Delete a single document by its [primary key](https://www.meilisearch.com/docs/learn/getting_started/primary_key).
#[routes::path(
    security(("Bearer" = ["documents.delete", "documents.*", "*"])),
    params(
        ("index_uid" = String, Path, example = "movies", description = "Unique identifier of the index.", nullable = false),
        ("document_id" = String, Path, example = "853", description = "Document identifier.", nullable = false),
        CustomMetadataQuery,
    ),
    responses(
        (status = 202, description = "Task successfully enqueued.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentAdditionOrUpdate",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
    )
)]
pub async fn delete_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    path: web::Path<DocumentParam>,
    params: AwebQueryParameter<CustomMetadataQuery, DeserrQueryParamError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let CustomMetadataQuery { custom_metadata } = params.into_inner();
    let DocumentParam { index_uid, document_id } = path.into_inner();
    let index_uid = IndexUid::try_from(index_uid)?;
    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    analytics.publish(
        DocumentsDeletionAggregator {
            per_document_id: true,
            clear_all: false,
            per_batch: false,
            per_filter: false,
        },
        &req,
    );

    let task = KindWithContent::DocumentDeletion {
        index_uid: index_uid.to_string(),
        documents_ids: vec![document_id],
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let mut task = {
        let index_scheduler = index_scheduler.clone();
        tokio::task::spawn_blocking(move || {
            index_scheduler.register_with_custom_metadata(
                task,
                uid,
                custom_metadata,
                dry_run,
                task_network,
            )
        })
        .await??
    };

    if let Some(task_network) = task.network.take() {
        proxy(&index_scheduler, Some(&index_uid), &req, task_network, network, Body::none(), &task)
            .await?;
    }

    let task: SummarizedTaskView = task.into();
    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Debug, Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct BrowseQueryGet {
    /// Number of documents to skip in the response. Use this parameter
    /// together with `limit` to paginate through large document sets. For
    /// example, to get documents 21-40, set `offset=20` and `limit=20`.
    /// Defaults to `0`.
    #[param(required = false, default, value_type = Option<usize>)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentOffset>)]
    offset: Param<usize>,
    /// Maximum number of documents to return in a single response. Use
    /// together with `offset` for pagination. Defaults to `20`.
    #[param(required = false, default, value_type = Option<usize>)]
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidDocumentLimit>)]
    limit: Param<usize>,
    /// Comma-separated list of document attributes to include in the
    /// response. Use `*` to retrieve all attributes. By default, all
    /// attributes are returned. Example: `title,description,price`.
    #[param(required = false, default, value_type = Option<Vec<String>>)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentFields>)]
    fields: OptionStarOrList<String>,
    /// When `true`, includes vector embeddings in the response for documents
    /// that have them. This is useful when you need to inspect or export
    /// vector data. Defaults to `false`.
    #[param(required = false, default, value_type = Option<bool>)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentRetrieveVectors>)]
    retrieve_vectors: Param<bool>,
    /// Comma-separated list of document IDs to retrieve. Only documents with
    /// matching IDs will be returned. If not specified, all documents
    /// matching other criteria are returned.
    #[param(required = false, default, value_type = Option<Vec<String>>)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentIds>)]
    ids: Option<CS<String>>,
    /// Filter expression to select which documents to return. Attributes must be added to the
    /// `filterableAttributes` index setting before they can be used in filters. Only accepts
    /// string expressions (not array syntax). Example: `genres = action AND rating > 4`.
    #[param(required = false, default, value_type = Option<String>, example = "popularity > 1000")]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentFilter>)]
    filter: Option<String>,
    /// Attribute(s) to sort the documents by. Format: `attribute:asc` or
    /// `attribute:desc`. Multiple sort criteria can be comma-separated.
    /// Example: `price:asc,rating:desc`.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentSort>)]
    sort: Option<String>,
    /// When `true`, runs the query on the whole network (all shards covered exactly once).
    ///
    /// When `false`, the query runs locally.
    ///
    /// When omitted or `null`, the default value depends on whether the sharding is enabled for the instance:
    ///
    /// - If the instance has sharding enabled (has a leader), defaults to `true`.
    /// - Otherwise defaults to `false`.
    ///
    /// It also requires the `network` [experimental feature](http://localhost:3000/reference/api/experimental-features/configure-experimental-features).
    ///
    /// Values: `true` = use the whole network; `false` = local, default = see above.
    ///
    /// When using the network, the index must exist with compatible settings on all remotes.
    #[param(required = false)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentUseNetwork>)]
    pub use_network: Option<bool>,
}

/// Request body for browsing and retrieving documents from an index. Use
/// this to fetch documents with optional filtering, sorting, and pagination.
/// This is useful for displaying document lists, exporting data, or
/// inspecting index contents.
#[routes::request(proxied)]
#[derive(Debug, Clone)]
pub struct BrowseQuery {
    /// Number of documents to skip in the response. Use together with `limit`
    /// for pagination through large document sets. For example, to get
    /// documents 151-170, set `offset=150` and `limit=20`. Defaults to `0`.
    #[request(default, schema_default = 0, error = DeserrJsonError<InvalidDocumentOffset>, example = 150)]
    offset: usize,
    /// Maximum number of documents to return in a single response. Use
    /// together with `offset` for pagination. Higher values return more
    /// results but may increase response time and memory usage. Defaults to
    /// `20`.
    #[request(default = PAGINATION_DEFAULT_LIMIT, schema_default = PAGINATION_DEFAULT_LIMIT_FN, error = DeserrJsonError<InvalidDocumentLimit>, example = 1)]
    limit: usize,
    /// Array of document attributes to include in the response. If not
    /// specified, all attributes listed in the `displayedAttributes` setting
    /// are returned. Use this to reduce response size by only requesting the
    /// fields you need. Example: `["title", "description", "price"]`.
    #[request(default, error = DeserrJsonError<InvalidDocumentFields>, example = json!(["title, description"]))]
    fields: Option<Vec<String>>,
    /// When `true`, includes the vector embeddings in the response for
    /// documents that have them. This is useful when you need to inspect or
    /// export vector data. Note that this can significantly increase response
    /// size. Defaults to `false`.
    #[request(default, error = DeserrJsonError<InvalidDocumentRetrieveVectors>, example = true)]
    retrieve_vectors: bool,
    /// Array of specific document IDs to retrieve. Only documents with
    /// matching [primary key](https://www.meilisearch.com/docs/learn/getting_started/primary_key) values will be returned. If not specified, all
    /// documents matching other criteria are returned. This is useful for
    /// fetching specific known documents.
    #[request(default, error = DeserrJsonError<InvalidDocumentIds>, schema_type = Option<Vec<String>>, example = json!(["cody", "finn", "brandy", "gambit"]))]
    ids: Option<Vec<serde_json::Value>>,
    /// Filter expression to select which documents to return. Attributes must be added to the
    /// `filterableAttributes` index setting before they can be used in filters. Accepts a string
    /// or an array of arrays of strings for AND/OR combinations.
    /// Example string: `"genres = action AND rating > 4"`.
    /// Example array: `[["genres = action", "genres = comedy"], "rating > 4"]` (inner array = OR, outer = AND).
    #[request(default, error = DeserrJsonError<InvalidDocumentFilter>, example = "popularity > 1000")]
    filter: Option<Value>,
    /// Array of attributes to sort the documents by. Each entry should be in
    /// the format `attribute:direction` where direction is either `asc`
    /// (ascending) or `desc` (descending). Example: `["price:asc",
    /// "rating:desc"]` sorts by price ascending, then by rating descending.
    #[request(default, error = DeserrJsonError<InvalidDocumentSort>, example = json!(["title:asc", "rating:desc"]))]
    sort: Option<Vec<String>>,
    /// When `true`, runs the query on the whole network (all shards covered exactly once).
    ///
    /// When `false`, the query runs locally.
    ///
    /// When omitted or `null`, the default value depends on whether the sharding is enabled for the instance:
    ///
    /// - If the instance has sharding enabled (has a leader), defaults to `true`.
    /// - Otherwise defaults to `false`.
    ///
    /// It also requires the `network` [experimental feature](http://localhost:3000/reference/api/experimental-features/configure-experimental-features).
    ///
    /// Values: `true` = use the whole network; `false` = local, default = see above.
    ///
    /// When using the network, the index must exist with compatible settings on all remotes.
    #[request(default, error = DeserrJsonError<InvalidDocumentUseNetwork>)]
    pub use_network: Option<bool>,
}

impl NetworkableQuery for BrowseQuery {
    fn use_network_field(&mut self) -> &mut Option<bool> {
        &mut self.use_network
    }

    fn has_remote(&self) -> bool {
        false
    }
}

impl ProxyQuery for &BrowseQuery {
    type ProxiedQuery = (String, BrowseQuery);

    fn proxy_with_remote(&self, remote: String) -> Self::ProxiedQuery {
        let mut query = (*self).clone();
        // because we merge the results from multiple sources,
        // we must always start from the first document and retrieve offset+limit documents
        query.limit += self.offset;
        query.offset = 0;
        (remote, query)
    }

    fn filter_field(query: &mut Self::ProxiedQuery) -> &mut Option<IndexFilter> {
        &mut query.1.filter
    }
}

/// Response from a documents retrieval query
#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct DocumentsResult {
    /// Documents for the current page.
    #[schema(value_type = Vec<serde_json::Map<String, serde_json::Value>>)]
    pub results: Vec<Document>,
    /// Number of items skipped.
    pub offset: usize,
    /// Maximum number of items returned.
    pub limit: usize,
    /// Total number of items matching the query.
    pub total: usize,
    /// Errors from remote servers
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_errors: Option<BTreeMap<String, ResponseError>>,
}

/// A single merged document.
#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub struct MergedDocument {
    /// Document fields as stored in the index.
    ///
    /// According to `attributesToRetrieve`.
    #[serde(flatten)]
    #[schema(additional_properties, inline, value_type = HashMap<String, Value>)]
    pub document: Document,

    /// Weighted score values for the document.
    /// This contains each facet value needed to sort the document.
    ///
    /// They are never de/serialized and must be manually
    /// mounted to/unmounted from `_federation.weightedScoreValues` in a federated context.
    ///
    #[serde(default, skip)]
    #[schema(ignore)]
    score: Vec<WeightedScoreValue>,
}

impl TryFrom<Document> for MergedDocument {
    type Error = ResponseError;

    fn try_from(mut document: Document) -> Result<Self, Self::Error> {
        let mut federation = document
            .remove(FEDERATION_HIT)
            .ok_or(ProxySearchError::MissingPathInResponse("._federation").as_response_error())?;

        let federation = match federation.as_object_mut() {
            Some(federation) => federation,
            None => {
                return Err(ProxySearchError::UnexpectedValueInPath {
                    path: "._federation",
                    expected_type: "map",
                    received_value: federation.to_string(),
                }
                .as_response_error());
            }
        };

        let score: Vec<WeightedScoreValue> = serde_json::from_value(
            federation.remove(WEIGHTED_SCORE_VALUES).ok_or(
                ProxySearchError::MissingPathInResponse("._federation.weightedScoreValues")
                    .as_response_error(),
            )?,
        )
        .map_err(ProxySearchError::CouldNotParseWeightedScoreValues)
        .map_err(|err| err.as_response_error())?;

        Ok(MergedDocument { document, score })
    }
}

/// List documents with POST
///
/// Retrieve a set of documents with optional filtering, sorting, and pagination. Use the request
/// body to specify filters, sort order, and which fields to return.
///
/// **Note:** Sending an empty payload (`{}`) returns all documents in the index.
///
/// **Note:** Documents are not returned following the order of their primary keys.
#[routes::path(
    security(("Bearer" = ["documents.get", "documents.*", "*"])),
    params(("index_uid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false)),
    request_body = BrowseQuery,
    responses(
        (status = 200, description = "Documents returned.", body = PaginationView<serde_json::Value>, content_type = "application/json", example = json!(
            {
                "results":[
                    {
                        "title":"The Travels of Ibn Battuta",
                        "genres":[
                            "Travel",
                            "Adventure"
                        ],
                        "language":"English",
                        "rating":4.5
                    },
                    {
                        "title":"Pride and Prejudice",
                        "genres":[
                            "Classics",
                            "Fiction",
                            "Romance",
                            "Literature"
                        ],
                        "language":"English",
                        "rating":4
                    },
                ],
                "offset":0,
                "limit":2,
                "total":5
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
    )
)]
pub async fn documents_by_query_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<BrowseQuery, DeserrJsonError>,
    search_queue: web::Data<crate::search_queue::SearchQueue>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let use_queue = index_scheduler.features().queue_documents_fetch();
    let permit = if use_queue { Some(search_queue.try_get_search_permit().await?) } else { None };

    let body = body.into_inner();
    debug!(parameters = ?body, "Get documents POST");

    // check remote header
    let is_proxy = req
        .headers()
        .get(PROXY_SEARCH_HEADER)
        .is_some_and(|value| value.as_bytes() == PROXY_SEARCH_HEADER_VALUE.as_bytes());

    analytics.publish(
        DocumentsFetchAggregator::<DocumentsPOST> {
            per_filter: body.filter.is_some(),
            with_vector_filter: body
                .filter
                .as_ref()
                .is_some_and(|f| f.to_string().contains("_vectors")),
            sort: body.sort.is_some(),
            retrieve_vectors: body.retrieve_vectors,
            max_limit: body.limit,
            max_offset: body.offset,
            max_document_ids: body.ids.as_ref().map(Vec::len).unwrap_or_default(),
            per_document_id: false,
            marker: PhantomData,
        },
        &req,
    );

    let ret = documents_by_query(index_scheduler.clone(), index_uid, body, is_proxy).await;
    if let Some(permit) = permit {
        permit.drop().await;
    }
    ret
}

/// List documents with GET
///
/// Retrieve documents in batches using query parameters for offset, limit, and optional filtering.
///
/// **Deprecated:** This endpoint will be deprecated in a future release. Use `POST /indexes/{index_uid}/documents/fetch` instead, which supports more parameters and array-based filter expressions.
///
/// **Note:** Documents are not returned following the order of their primary keys.
#[routes::path(
    security(("Bearer" = ["documents.get", "documents.*", "*"])),
    params(
        ("index_uid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false),
        BrowseQueryGet
    ),
    responses(
        (status = 200, description = "The documents are returned.", body = PaginationView<serde_json::Value>, content_type = "application/json", example = json!(
            {
                "results": [
                    {
                        "id": 25684,
                        "title": "American Ninja 5",
                        "poster": "https://image.tmdb.org/t/p/w1280/iuAQVI4mvjI83wnirpD8GVNRVuY.jpg",
                        "overview": "When a scientists daughter is kidnapped, American Ninja, attempts to find her, but this time he teams up with a youngster he has trained in the ways of the ninja.",
                        "release_date": 725846400
                    },
                    {
                        "id": 45881,
                        "title": "The Bridge of San Luis Rey",
                        "poster": "https://image.tmdb.org/t/p/w500/4X7quIcdkc24Cveg5XdpfRqxtYA.jpg",
                        "overview": "The Bridge of San Luis Rey is American author Thornton Wilder's second novel, first published in 1927 to worldwide acclaim. It tells the story of several interrelated people who die in the collapse of an Inca rope-fiber suspension bridge in Peru, and the events that lead up to their being on the bridge.[ A friar who has witnessed the tragic accident then goes about inquiring into the lives of the victims, seeking some sort of cosmic answer to the question of why each had to die. The novel won the Pulitzer Prize in 1928.",
                        "release_date": 1072915200
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total": 2
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn get_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<BrowseQueryGet, DeserrQueryParamError>,
    search_queue: web::Data<crate::search_queue::SearchQueue>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?params, "Get documents GET");

    let use_queue = index_scheduler.features().queue_documents_fetch();
    let permit = if use_queue { Some(search_queue.try_get_search_permit().await?) } else { None };

    let BrowseQueryGet { limit, offset, fields, retrieve_vectors, filter, ids, sort, use_network } =
        params.into_inner();

    let filter = match filter {
        Some(f) => match serde_json::from_str(&f) {
            Ok(v) => Some(v),
            _ => Some(Value::String(f)),
        },
        None => None,
    };

    let query = BrowseQuery {
        offset: offset.0,
        limit: limit.0,
        fields: fields.merge_star_and_none(),
        retrieve_vectors: retrieve_vectors.0,
        filter,
        ids: ids.map(|ids| ids.into_iter().map(Into::into).collect()),
        sort: sort.map(|attr| fix_sort_query_parameters(&attr)),
        use_network,
    };

    analytics.publish(
        DocumentsFetchAggregator::<DocumentsGET> {
            per_filter: query.filter.is_some(),
            with_vector_filter: query
                .filter
                .as_ref()
                .is_some_and(|f| f.to_string().contains("_vectors")),
            sort: query.sort.is_some(),
            retrieve_vectors: query.retrieve_vectors,
            max_limit: query.limit,
            max_offset: query.offset,
            max_document_ids: query.ids.as_ref().map(Vec::len).unwrap_or_default(),
            per_document_id: false,
            marker: PhantomData,
        },
        &req,
    );

    let ret = documents_by_query(index_scheduler.clone(), index_uid, query, false).await;

    if let Some(permit) = permit {
        permit.drop().await;
    }

    ret
}

async fn documents_by_query(
    index_scheduler: Data<IndexScheduler>,
    index_uid: web::Path<String>,
    mut query: BrowseQuery,
    is_proxy: bool,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let features = index_scheduler.features();
    let network = index_scheduler.network();

    let ret = if query.must_use_network(&network, &features)? {
        retrieve_documents_federated(index_scheduler, index_uid, query, network).await
    } else {
        retrieve_documents_local(index_scheduler, index_uid, query, is_proxy).await
    };

    Ok(HttpResponse::Ok().json(ret?))
}

async fn retrieve_documents_federated(
    index_scheduler: Data<IndexScheduler>,
    index_uid: IndexUid,
    query: BrowseQuery,
    network: Network,
) -> Result<DocumentsResult, ResponseError> {
    let params =
        ProxySearchParams::new_with_deadline_from_env(index_scheduler.web_client().clone());
    let remote_availability = index_scheduler.remote_availability();
    let partition = Partition::new(network.clone(), remote_availability);

    let (local_queries, remote_queries): (Vec<_>, Vec<_>) =
        partition.into_partition(&query)?.enumerate().partition(
            // true is left, false is right
            |(_, (remote, _))| Some(remote) == network.local.as_ref(),
        );

    let mut results: Vec<_> = Vec::with_capacity(remote_queries.len() + local_queries.len());
    let mut errors: BTreeMap<String, ResponseError> = BTreeMap::new();

    const MAX_IN_FLIGHT_REQUESTS: usize = 40;
    let mut in_flight_requests = VecDeque::with_capacity(MAX_IN_FLIGHT_REQUESTS);

    for (query_id, (remote_name, query)) in remote_queries {
        let Some(remote) = network.remotes.get(&remote_name) else {
            errors.insert(
                remote_name.clone(),
                ProxySearchError::UnknownRemote { remote: remote_name }.as_response_error(),
            );
            continue;
        };

        let path_and_query =
            match meilisearch_types::network::route::documents_fetch_path(&index_uid) {
                Ok(path_and_query) => path_and_query,
                Err(err) => {
                    errors.insert(
                        remote_name,
                        ProxySearchError::InvalidRemoteUrl { cause: err.to_string() }
                            .as_response_error(),
                    );
                    continue;
                }
            };

        let request = match json_proxy(
            path_and_query,
            http_client::reqwest::Method::POST,
            remote,
            &query,
            &params,
            false, // no metadata on documents-fetch
        ) {
            Ok(request) => request,
            Err(err) => {
                errors.insert(remote_name, err.as_response_error());
                continue;
            }
        };

        if in_flight_requests.len() == MAX_IN_FLIGHT_REQUESTS {
            // unwrap: MAX_IN_FLIGHT_REQUESTS > 0
            let task: tokio::task::JoinHandle<(
                Result<DocumentsResult, ProxySearchError>,
                String,
                usize,
            )> = in_flight_requests.pop_front().unwrap();
            match task.await.unwrap() {
                (Ok(result), _, query_id) => results.push((result, query_id)),
                (Err(err), remote_name, _) => {
                    errors.insert(remote_name, err.as_response_error());
                    continue;
                }
            }
        }
        in_flight_requests
            .push_back(tokio::spawn(async move { (request.await, remote_name, query_id) }));
    }

    // Perform local search
    for (query_id, (_, query)) in local_queries {
        let result =
            retrieve_documents_local(index_scheduler.clone(), index_uid.clone(), query, true)
                .await?;
        results.push((result, query_id));
    }

    // Retrieve remote results
    for task in in_flight_requests {
        match task.await.unwrap() {
            (Ok(result), _, query_id) => results.push((result, query_id)),
            (Err(err), remote_name, _) => {
                errors.insert(remote_name, err.as_response_error());
            }
        }
    }

    // merge metadata
    let (total, mut remote_errors) = merge_metadata(&mut results);
    if !errors.is_empty() {
        remote_errors.get_or_insert_with(BTreeMap::new).extend(errors);
    }

    // Merge results
    let merged_results: Result<_, ResponseError> =
        merge_documents_results(results).skip(query.offset).take(query.limit).collect();

    Ok(DocumentsResult {
        results: merged_results?,
        offset: query.offset,
        limit: query.limit,
        total,
        remote_errors,
    })
}

fn merge_documents_results(
    results: Vec<(DocumentsResult, usize)>,
) -> impl Iterator<Item = Result<Document, ResponseError>> {
    itertools::kmerge_by(
        results.into_iter().map(|(results, query_id)| {
            results
                .results
                .into_iter()
                .map(move |result| (MergedDocument::try_from(result), query_id))
        }),
        |left: &(Result<MergedDocument, ResponseError>, usize),
         right: &(Result<MergedDocument, ResponseError>, usize)| {
            let Ok(left_hit) = left.0.as_ref() else {
                return true;
            };
            let Ok(right_hit) = right.0.as_ref() else {
                return false;
            };
            match compare_documents(left_hit, right_hit) {
                Ordering::Greater => true,
                Ordering::Less => false,
                // break ties using query index
                Ordering::Equal => left.1 < right.1,
            }
        },
    )
    .map(|(merged_document, _query_id)| merged_document.map(|md| md.document))
}

fn compare_documents(left: &MergedDocument, right: &MergedDocument) -> Ordering {
    weighted_scores::compare_partial(left.score.iter().cloned(), right.score.iter().cloned())
        // unwrap: comparison should be always possible because all documents use the same sorting strategy
        .unwrap()
}

fn merge_metadata(
    results: &mut [(DocumentsResult, usize)],
) -> (usize, Option<BTreeMap<String, ResponseError>>) {
    let mut errors = None;
    let mut total = 0;
    for (result, _query_id) in results {
        total += result.total;
        if let Some(remote_errors) = result.remote_errors.take() {
            errors.get_or_insert_with(BTreeMap::new).extend(remote_errors);
        }
    }

    (total, errors)
}

async fn retrieve_documents_local(
    index_scheduler: Data<IndexScheduler>,
    index_uid: IndexUid,
    query: BrowseQuery,
    is_proxy: bool,
) -> Result<DocumentsResult, ResponseError> {
    let BrowseQuery { offset, limit, fields, retrieve_vectors, filter, ids, sort, use_network: _ } =
        query;
    tokio::task::spawn_blocking(move || -> Result<_, ResponseError> {
        let retrieve_vectors = RetrieveVectors::new(retrieve_vectors);
        let ids = if let Some(ids) = ids {
            let mut parsed_ids = Vec::with_capacity(ids.len());
            for (index, id) in ids.into_iter().enumerate() {
                let id = id.try_into().map_err(|error| {
                    let msg = format!("In `.ids[{index}]`: {error}");
                    ResponseError::from_msg(msg, Code::InvalidDocumentIds)
                })?;
                parsed_ids.push(id)
            }
            Some(parsed_ids)
        } else {
            None
        };

        let sort_criteria = if let Some(sort) = &sort {
            let sorts: Vec<_> = match sort.iter().map(|s| milli::AscDesc::from_str(s)).collect() {
                Ok(sorts) => sorts,
                Err(asc_desc_error) => {
                    return Err(milli::SortError::from(asc_desc_error).into_document_error().into())
                }
            };
            Some(sorts)
        } else {
            None
        };

        let index = index_scheduler.user_index(&index_uid)?;
        let rtxn = index.read_txn()?;
        let progress = Progress::default();

        let filter = &filter;
        let filter = if let Some(filter) = filter {
            let filter = parse_filter(
                filter,
                Code::InvalidDocumentFilter,
                index_scheduler.features(),
                None,
            )?;
            filter
                .map(|f| {
                    filter_into_index_filter(
                        f,
                        &index,
                        &rtxn,
                        &index_scheduler,
                        &progress,
                        &index_uid,
                    )
                })
                .transpose()?
        } else {
            None
        };

        let (total, documents) = retrieve_documents(
            &index,
            &rtxn,
            offset,
            limit,
            ids,
            filter,
            fields,
            retrieve_vectors,
            sort_criteria,
            is_proxy,
        )?;

        Ok(DocumentsResult {
            results: documents,
            offset,
            limit,
            total: total as usize,
            remote_errors: None,
        })
    })
    .await
    .unwrap()
}

#[derive(Deserialize, Debug, Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(parameter_in = Query, rename_all = "camelCase")]
pub struct UpdateDocumentsQuery {
    /// The [primary key](https://www.meilisearch.com/docs/learn/getting_started/primary_key) field for uniquely identifying each document.
    /// This parameter is optional and can only be set the first time documents are added to an index.
    /// Subsequent attempts to specify it will be ignored if the primary key has already been set.
    #[param(required = false, example = "id")]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexPrimaryKey>)]
    pub primary_key: Option<String>,
    /// Customize the CSV column delimiter when importing CSV documents. Must be a single ASCII
    /// character. Only valid when the content type is `text/csv`; using this parameter with
    /// `application/json` or `application/x-ndjson` will return an error. Default: `,`.
    #[param(required = false, value_type = char, default = ",", example = ";")]
    #[deserr(default, try_from(char) = from_char_csv_delimiter -> DeserrQueryParamError<InvalidDocumentCsvDelimiter>, error = DeserrQueryParamError<InvalidDocumentCsvDelimiter>)]
    pub csv_delimiter: Option<u8>,

    /// A string that can be used to identify and filter tasks. This metadata
    /// is stored with the task and returned in task responses. Useful for
    /// tracking tasks from external systems or associating tasks with
    /// specific operations in your application.
    #[param(required = false, example = "custom")]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexCustomMetadata>)]
    pub custom_metadata: Option<String>,

    /// When set to `true`, only updates existing documents and skips creating
    /// new ones. Documents that don't already exist in the index will be
    /// ignored. This is useful for partial updates where you only want to
    /// modify existing records without adding new ones.
    #[param(required = false, example = true)]
    #[deserr(default, try_from(&String) = from_string_skip_creation -> DeserrQueryParamError<InvalidSkipCreation>, error = DeserrQueryParamError<InvalidSkipCreation>)]
    pub skip_creation: Option<bool>,
}

#[derive(Deserialize, Debug, Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(parameter_in = Query, rename_all = "camelCase")]
pub struct CustomMetadataQuery {
    /// A string that can be used to identify and filter tasks. This metadata
    /// is stored with the task and returned in task responses. Useful for
    /// tracking tasks from external systems or associating tasks with
    /// specific operations in your application.
    #[param(required = false, example = "custom")]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexCustomMetadata>)]
    pub custom_metadata: Option<String>,
}

fn from_char_csv_delimiter(
    c: char,
) -> Result<Option<u8>, DeserrQueryParamError<InvalidDocumentCsvDelimiter>> {
    if c.is_ascii() {
        Ok(Some(c as u8))
    } else {
        Err(DeserrQueryParamError::new(
            format!("csv delimiter must be an ascii character. Found: `{}`", c),
            Code::InvalidDocumentCsvDelimiter,
        ))
    }
}

fn from_string_skip_creation(
    s: &String,
) -> Result<Option<bool>, DeserrQueryParamError<InvalidSkipCreation>> {
    if s.eq_ignore_ascii_case("true") {
        return Ok(Some(true));
    }

    if s.eq_ignore_ascii_case("false") {
        return Ok(Some(false));
    }

    Err(DeserrQueryParamError::new(
        format!("skipCreation must be either `true` or `false`. Found: `{}`", s),
        Code::InvalidSkipCreation,
    ))
}

aggregate_methods!(
    Replaced => "Documents Added",
    Updated => "Documents Updated",
);

#[derive(Serialize)]
pub struct DocumentsAggregator<T: AggregateMethod> {
    payload_types: HashSet<String>,
    primary_key: HashSet<String>,
    index_creation: bool,
    #[serde(skip)]
    method: PhantomData<T>,
}

impl<Method: AggregateMethod> Aggregate for DocumentsAggregator<Method> {
    fn event_name(&self) -> &'static str {
        Method::event_name()
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            payload_types: self.payload_types.union(&new.payload_types).cloned().collect(),
            primary_key: self.primary_key.union(&new.primary_key).cloned().collect(),
            index_creation: self.index_creation | new.index_creation,
            method: PhantomData,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }
}

/// Add or replace documents
///
/// Add a list of documents or replace them if they already exist.
///
/// If you send an already existing document (same id) the whole existing document will be
/// overwritten by the new document. Fields previously in the document not present in the new
/// document are removed.
///
/// If the provided index does not exist, it will be created.
///
/// **Accepted content types:** `application/json`, `application/x-ndjson`, `text/csv`.
///
/// **Note:** Use the reserved `_geo` object to add geo coordinates: `{"lat": 48.8566, "lng": 2.3522}`.
///
/// For a partial update see [add or update documents route](/docs/reference/api/documents/add-or-update-documents).

#[routes::path(
    security(("Bearer" = ["documents.add", "documents.*", "*"])),
    params(
        ("index_uid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false),
        // Here we can use the post version of the browse query since it contains the exact same parameter
        UpdateDocumentsQuery,
    ),
    request_body = serde_json::Value,
    responses(
        (status = 202, description = "Task successfully enqueued.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentAdditionOrUpdate",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
    )
)]
pub async fn replace_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<UpdateDocumentsQuery, DeserrQueryParamError>,
    body: Payload,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    debug!(parameters = ?params, "Replace documents");
    let params = params.into_inner();

    let mut content_types = HashSet::new();
    let content_type = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|s| s.to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    content_types.insert(content_type);
    let mut primary_keys = HashSet::new();
    if let Some(primary_key) = params.primary_key.clone() {
        primary_keys.insert(primary_key);
    }
    analytics.publish(
        DocumentsAggregator::<Replaced> {
            payload_types: content_types,
            primary_key: primary_keys,
            index_creation: index_scheduler.user_index_exists(&index_uid).map_or(true, |x| !x),
            method: PhantomData,
        },
        &req,
    );

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task = document_addition(
        index_scheduler,
        index_uid,
        params.primary_key,
        params.csv_delimiter,
        body,
        IndexDocumentsMethod::ReplaceDocuments,
        uid,
        params.custom_metadata,
        dry_run,
        allow_index_creation,
        params.skip_creation,
        &req,
    )
    .await?;

    debug!(returns = ?task, "Replace documents");

    Ok(HttpResponse::Accepted().json(task))
}

/// Add or update documents
///
/// Add a list of documents or update them if they already exist.
///
/// If you send an already existing document (same id) the old document will
/// be only partially updated according to the fields of the new document.
/// Thus, any fields not present in the new document are kept and remained
/// unchanged.
///
/// **Important:** Partial updates apply only to top-level fields. Updating an object attribute
/// replaces the entire object, removing any subfields not present in the update. Dot notation
/// in an update request creates a new flat attribute rather than updating an existing nested field.
///
/// If the provided index does not exist, it will be created.
///
/// **Accepted content types:** `application/json`, `application/x-ndjson`, `text/csv`.
///
/// **Note:** Use the reserved `_geo` object to add geo coordinates: `{"lat": 48.8566, "lng": 2.3522}`.
///
/// To completely overwrite a document, see [add or replace documents route](/docs/reference/api/documents/add-or-replace-documents).

#[routes::path(
    security(("Bearer" = ["documents.add", "documents.*", "*"])),
    params(
        ("index_uid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false),
        // Here we can use the post version of the browse query since it contains the exact same parameter
        UpdateDocumentsQuery,
    ),
    request_body = serde_json::Value,
    responses(
        (status = 202, description = "Task successfully enqueued.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentAdditionOrUpdate",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
    )
)]
pub async fn update_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<UpdateDocumentsQuery, DeserrQueryParamError>,
    body: Payload,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let params = params.into_inner();
    debug!(parameters = ?params, "Update documents");

    let mut content_types = HashSet::new();
    let content_type = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|s| s.to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    content_types.insert(content_type);
    let mut primary_keys = HashSet::new();
    if let Some(primary_key) = params.primary_key.clone() {
        primary_keys.insert(primary_key);
    }
    analytics.publish(
        DocumentsAggregator::<Updated> {
            payload_types: content_types,
            primary_key: primary_keys,
            index_creation: index_scheduler.user_index_exists(&index_uid).map_or(true, |x| !x),
            method: PhantomData,
        },
        &req,
    );

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task = document_addition(
        index_scheduler,
        index_uid,
        params.primary_key,
        params.csv_delimiter,
        body,
        IndexDocumentsMethod::UpdateDocuments,
        uid,
        params.custom_metadata,
        dry_run,
        allow_index_creation,
        params.skip_creation,
        &req,
    )
    .await?;
    debug!(returns = ?task, "Update documents");

    Ok(HttpResponse::Accepted().json(task))
}

#[allow(clippy::too_many_arguments)]
async fn document_addition(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: IndexUid,
    primary_key: Option<String>,
    csv_delimiter: Option<u8>,
    body: Payload,
    method: IndexDocumentsMethod,
    task_id: Option<TaskId>,
    custom_metadata: Option<String>,
    dry_run: bool,
    allow_index_creation: bool,
    skip_creation: Option<bool>,
    req: &HttpRequest,
) -> Result<SummarizedTaskView, MeilisearchHttpError> {
    let mime_type = extract_mime_type(req)?;
    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(req, &network)?;

    let format = match (
        mime_type.as_ref().map(|m| (m.type_().as_str(), m.subtype().as_str())),
        csv_delimiter,
    ) {
        (Some(("application", "json")), None) => PayloadType::Json,
        (Some(("application", "x-ndjson")), None) => PayloadType::Ndjson,
        (Some(("text", "csv")), None) => PayloadType::Csv { delimiter: b',' },
        (Some(("text", "csv")), Some(delimiter)) => PayloadType::Csv { delimiter },

        (Some(("application", "json")), Some(_)) => {
            return Err(MeilisearchHttpError::CsvDelimiterWithWrongContentType(String::from(
                "application/json",
            )))
        }
        (Some(("application", "x-ndjson")), Some(_)) => {
            return Err(MeilisearchHttpError::CsvDelimiterWithWrongContentType(String::from(
                "application/x-ndjson",
            )))
        }
        (Some((type_, subtype)), _) => {
            return Err(MeilisearchHttpError::InvalidContentType(
                format!("{}/{}", type_, subtype),
                ACCEPTED_CONTENT_TYPE.clone(),
            ))
        }
        (None, _) => {
            return Err(MeilisearchHttpError::MissingContentType(ACCEPTED_CONTENT_TYPE.clone()))
        }
    };

    let (uuid, mut update_file) = index_scheduler.queue.create_update_file(dry_run)?;
    let res = match format {
        PayloadType::Ndjson => {
            let (path, file) = update_file.into_parts();
            let file = match file {
                Some(file) => {
                    let (file, path) = file.into_parts();
                    let mut file = copy_body_to_file(file, body, format).await?;
                    file.rewind().map_err(|e| {
                        index_scheduler::Error::FileStore(file_store::Error::IoError(e))
                    })?;
                    Some(tempfile::NamedTempFile::from_parts(file, path))
                }
                None => None,
            };

            let res = tokio::task::spawn_blocking(move || {
                let documents_count = file.as_ref().map_or(Ok(0), |ntf| {
                    read_ndjson(ntf.as_file()).map_err(MeilisearchHttpError::DocumentFormat)
                })?;

                let update_file = file_store::File::from_parts(path, file);
                let update_file = update_file.persist()?;

                Ok((documents_count, update_file))
            })
            .await?;

            Ok(res)
        }
        PayloadType::Json | PayloadType::Csv { delimiter: _ } => {
            let temp_file = match tempfile() {
                Ok(file) => file,
                Err(e) => return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e)))),
            };

            let read_file = copy_body_to_file(temp_file, body, format).await?;
            tokio::task::spawn_blocking(move || {
                let documents_count = match format {
                    PayloadType::Json => read_json(&read_file, &mut update_file)?,
                    PayloadType::Csv { delimiter } => {
                        read_csv(&read_file, &mut update_file, delimiter)?
                    }
                    PayloadType::Ndjson => {
                        unreachable!("We already wrote the user content into the update file")
                    }
                };
                // we NEED to persist the file here because we moved the `update_file` in another task.
                let file = update_file.persist()?;
                Ok((documents_count, file))
            })
            .await
        }
    };

    let (documents_count, file) = match res {
        Ok(Ok((documents_count, file))) => (documents_count, file),
        // in this case the file has not possibly be persisted.
        Ok(Err(e)) => return Err(e),
        Err(e) => {
            // Here the file MAY have been persisted or not.
            // We don't know thus we ignore the file not found error.
            match index_scheduler.queue.delete_update_file(uuid) {
                Ok(()) => (),
                Err(index_scheduler::Error::FileStore(file_store::Error::IoError(e)))
                    if e.kind() == ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        index_uuid = %uuid,
                        "Unknown error happened while deleting a malformed update file: {e}"
                    );
                }
            }
            // We still want to return the original error to the end user.
            return Err(e.into());
        }
    };

    let task = KindWithContent::DocumentAdditionOrUpdate {
        method,
        content_file: uuid,
        documents_count,
        primary_key,
        allow_index_creation,
        index_uid: index_uid.to_string(),
        on_missing_document: if matches!(skip_creation, Some(true)) {
            MissingDocumentPolicy::Skip
        } else {
            MissingDocumentPolicy::Create
        },
    };

    // FIXME: not new to #6000, but _any_ error here will cause the payload to unduly persist
    let scheduler = index_scheduler.clone();
    let mut task = match tokio::task::spawn_blocking(move || {
        scheduler.register_with_custom_metadata(
            task,
            task_id,
            custom_metadata,
            dry_run,
            task_network,
        )
    })
    .await?
    {
        Ok(task) => task,
        Err(e) => {
            index_scheduler.queue.delete_update_file(uuid)?;
            return Err(e.into());
        }
    };

    if let Some(task_network) = task.network.take() {
        if let Some(file) = file {
            proxy(
                &index_scheduler,
                Some(&index_uid),
                req,
                task_network,
                network,
                Body::with_ndjson_payload(file),
                &task,
            )
            .await?;
        }
    }

    Ok(task.into())
}

async fn copy_body_to_file(
    output: std::fs::File,
    mut body: Payload,
    format: PayloadType,
) -> Result<std::fs::File, MeilisearchHttpError> {
    let async_file = File::from_std(output);
    let mut buffer = BufWriter::new(async_file);
    let mut buffer_write_size: usize = 0;
    while let Some(result) = body.next().await {
        let byte = result?;

        if byte.is_empty() && buffer_write_size == 0 {
            return Err(MeilisearchHttpError::MissingPayload(format));
        }

        match buffer.write_all(&byte).await {
            Ok(()) => buffer_write_size += 1,
            Err(e) => return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e)))),
        }
    }
    if let Err(e) = buffer.flush().await {
        return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e))));
    }
    if buffer_write_size == 0 {
        return Err(MeilisearchHttpError::MissingPayload(format));
    }
    if let Err(e) = buffer.seek(std::io::SeekFrom::Start(0)).await {
        return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e))));
    }
    let read_file = buffer.into_inner().into_std().await;
    Ok(read_file)
}

/// Delete documents by batch
///
/// Delete multiple documents in one request by providing an array of [primary key](https://www.meilisearch.com/docs/learn/getting_started/primary_key) values.
#[routes::path(
    security(("Bearer" = ["documents.delete", "documents.*", "*"])),
    params(
        ("index_uid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false),
        CustomMetadataQuery,
    ),
    request_body(content = Vec<Value>),
    responses(
        (status = 202, description = "Task successfully enqueued.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentAdditionOrUpdate",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
    )
)]
pub async fn delete_documents_batch(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: web::Json<Vec<Value>>,
    params: AwebQueryParameter<CustomMetadataQuery, DeserrQueryParamError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Delete documents by batch");
    let CustomMetadataQuery { custom_metadata } = params.into_inner();

    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    analytics.publish(
        DocumentsDeletionAggregator {
            per_batch: true,
            per_document_id: false,
            clear_all: false,
            per_filter: false,
        },
        &req,
    );

    let ids = body
        .iter()
        .map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))
        .collect();

    let task =
        KindWithContent::DocumentDeletion { index_uid: index_uid.to_string(), documents_ids: ids };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let mut task = {
        let index_scheduler = index_scheduler.clone();
        tokio::task::spawn_blocking(move || {
            index_scheduler.register_with_custom_metadata(
                task,
                uid,
                custom_metadata,
                dry_run,
                task_network,
            )
        })
        .await??
    };

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

    let task: SummarizedTaskView = task.into();

    debug!(returns = ?task, "Delete documents by batch");
    Ok(HttpResponse::Accepted().json(task))
}

/// Request body for deleting documents by filter
#[routes::request(proxied)]
#[derive(Debug)]
pub struct DocumentDeletionByFilter {
    /// Filter expression to match documents for deletion. Attributes must be in
    /// `filterableAttributes` before they can be used. Accepts a string or an array of arrays.
    /// Sending an empty filter will return a `bad_request` error.
    #[request(required, error = DeserrJsonError<InvalidDocumentFilter>, missing_field_error = DeserrJsonError::missing_document_filter)]
    filter: Value,
}

/// Delete documents by filter
///
/// Delete all documents in the index that match the given filter expression.
#[routes::path(
    security(("Bearer" = ["documents.delete", "documents.*", "*"])),
    params(
        ("index_uid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false),
        CustomMetadataQuery,
    ),
    request_body = DocumentDeletionByFilter,
    responses(
        (status = ACCEPTED, description = "Task successfully enqueued.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentDeletion",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
    )
)]
pub async fn delete_documents_by_filter(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<CustomMetadataQuery, DeserrQueryParamError>,
    body: AwebJson<DocumentDeletionByFilter, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Delete documents by filter");
    let CustomMetadataQuery { custom_metadata } = params.into_inner();

    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let index_uid = index_uid.into_inner();
    let filter = body.into_inner();
    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    analytics.publish(
        DocumentsDeletionAggregator {
            per_filter: true,
            per_document_id: false,
            clear_all: false,
            per_batch: false,
        },
        &req,
    );

    // we ensure the filter is well formed before enqueuing it
    parse_local_index_filter(
        &filter.filter,
        None,
        index_scheduler.features(),
        Code::InvalidDocumentFilter,
    )?
    .ok_or(MeilisearchHttpError::EmptyFilter)?;

    let task = KindWithContent::DocumentDeletionByFilter {
        index_uid: index_uid.clone(),
        filter_expr: filter.filter.clone(),
    };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let mut task = {
        let index_scheduler = index_scheduler.clone();
        tokio::task::spawn_blocking(move || {
            index_scheduler.register_with_custom_metadata(
                task,
                uid,
                custom_metadata,
                dry_run,
                task_network,
            )
        })
        .await??
    };

    if let Some(task_network) = task.network.take() {
        proxy(
            &index_scheduler,
            Some(&index_uid),
            &req,
            task_network,
            network,
            Body::inline(filter),
            &task,
        )
        .await?;
    }

    let task: SummarizedTaskView = task.into();

    debug!(returns = ?task, "Delete documents by filter");
    Ok(HttpResponse::Accepted().json(task))
}

/// Request body for editing documents using a RHAI function
#[routes::request(proxied)]
#[derive(Debug)]
pub struct DocumentEditionByFunction {
    /// Filter expression to select which documents to edit. If omitted, all documents in the
    /// index will be processed. Attributes must be in `filterableAttributes` to be used.
    #[request(default, error = DeserrJsonError<InvalidDocumentFilter>)]
    pub filter: Option<Value>,
    /// Arbitrary data to pass into the function scope. By default the function only has access
    /// to the current document being edited via the `doc` variable.
    #[request(default, error = DeserrJsonError<InvalidDocumentEditionContext>)]
    pub context: Option<Value>,
    /// A [RHAI](https://rhai.rs) function string to apply to each document. The function has
    /// access to a `doc` variable representing the current document. Modify `doc` fields to
    /// update the document. Return `null` or `()` to delete the document.
    /// To enable this feature: `PATCH /experimental-features/ {"editDocumentsByFunction": true}`.
    #[request(required, error = DeserrJsonError<InvalidDocumentEditionFunctionFilter>, missing_field_error = DeserrJsonError::missing_document_edition_function)]
    pub function: String,
}

#[derive(Serialize)]
struct EditDocumentsByFunctionAggregator {
    // Set to true if at least one request was filtered
    filtered: bool,
    // Set to true if at least one request contained a context
    with_context: bool,

    index_creation: bool,
}

impl Aggregate for EditDocumentsByFunctionAggregator {
    fn event_name(&self) -> &'static str {
        "Documents Edited By Function"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            filtered: self.filtered | new.filtered,
            with_context: self.with_context | new.with_context,
            index_creation: self.index_creation | new.index_creation,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Edit documents by function
///
/// Use a [RHAI function](https://rhai.rs/book/engine/hello-world.html) to edit one or more documents directly in Meilisearch. The function receives each document and returns the modified document.
///
/// This feature is experimental and must be enabled through the experimental route.
#[routes::path(
    security(("Bearer" = ["documents.*", "*"])),
    params(
        ("index_uid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false),
        CustomMetadataQuery,
    ),
    request_body = DocumentEditionByFunction,
    responses(
        (status = 202, description = "Task successfully enqueued.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentDeletion",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
    )
)]
pub async fn edit_documents_by_function(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ALL }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<CustomMetadataQuery, DeserrQueryParamError>,
    body: AwebJson<DocumentEditionByFunction, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Edit documents by function");
    let CustomMetadataQuery { custom_metadata } = params.into_inner();

    index_scheduler
        .features()
        .check_edit_documents_by_function("Using the documents edit route")?;

    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let index_uid = index_uid.into_inner();
    let body = body.into_inner();

    analytics.publish(
        EditDocumentsByFunctionAggregator {
            filtered: body.filter.is_some(),
            with_context: body.context.is_some(),
            index_creation: index_scheduler.user_index(&index_uid).is_err(),
        },
        &req,
    );

    let engine = milli::rhai::Engine::new();
    if let Err(e) = engine.compile(&body.function) {
        return Err(ResponseError::from_msg(e.to_string(), Code::BadRequest));
    }

    if let Some(ref filter) = body.filter {
        // we ensure the filter is well formed before enqueuing it
        parse_local_index_filter(
            filter,
            Some(index_uid.as_str()),
            index_scheduler.features(),
            Code::InvalidDocumentFilter,
        )?
        .ok_or(MeilisearchHttpError::EmptyFilter)?;
    }
    let task = KindWithContent::DocumentEdition {
        index_uid: index_uid.clone(),
        filter_expr: body.filter.clone(),
        context: match body.context.clone() {
            Some(Value::Object(m)) => Some(m),
            None => None,
            _ => {
                return Err(ResponseError::from_msg(
                    "The context must be an object".to_string(),
                    Code::InvalidDocumentEditionContext,
                ))
            }
        },
        function: body.function.clone(),
    };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let mut task = {
        let index_scheduler = index_scheduler.clone();
        tokio::task::spawn_blocking(move || {
            index_scheduler.register_with_custom_metadata(
                task,
                uid,
                custom_metadata,
                dry_run,
                task_network,
            )
        })
        .await??
    };

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

    let task: SummarizedTaskView = task.into();

    debug!(returns = ?task, "Edit documents by function");
    Ok(HttpResponse::Accepted().json(task))
}

/// Delete all documents
///
/// Permanently delete all documents in the specified index. Settings and index metadata are preserved.
#[routes::path(
    security(("Bearer" = ["documents.delete", "documents.*", "*"])),
    params(
        ("index_uid" = String, example = "movies", description = "Unique identifier of the index.", nullable = false),
        CustomMetadataQuery,
    ),
    responses(
        (status = 202, description = "Task successfully enqueued.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentDeletion",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "Index not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
    )
)]
pub async fn clear_all_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<CustomMetadataQuery, DeserrQueryParamError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let network = index_scheduler.network();
    let CustomMetadataQuery { custom_metadata } = params.into_inner();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    analytics.publish(
        DocumentsDeletionAggregator {
            clear_all: true,
            per_document_id: false,
            per_batch: false,
            per_filter: false,
        },
        &req,
    );

    let task = KindWithContent::DocumentClear { index_uid: index_uid.to_string() };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;

    let mut task = {
        let index_scheduler = index_scheduler.clone();

        tokio::task::spawn_blocking(move || {
            index_scheduler.register_with_custom_metadata(
                task,
                uid,
                custom_metadata,
                dry_run,
                task_network,
            )
        })
        .await??
    };

    if let Some(task_network) = task.network.take() {
        proxy(&index_scheduler, Some(&index_uid), &req, task_network, network, Body::none(), &task)
            .await?;
    }

    let task: SummarizedTaskView = task.into();

    debug!(returns = ?task, "Delete all documents");
    Ok(HttpResponse::Accepted().json(task))
}

fn some_documents<'a, 't: 'a, 'i, I1, I2, S1, S2>(
    index: &'a Index,
    rtxn: &'t RoTxn,
    doc_ids: impl IntoIterator<Item = DocumentId> + 'a,
    retrieve_vectors: RetrieveVectors,
    attributes_to_retrieve: Option<I1>,
    extra_attributes_to_retrieve: Option<I2>,
) -> Result<impl Iterator<Item = Result<(Document, Document), ResponseError>> + 'a, ResponseError>
where
    I1: IntoIterator<Item = S1> + 'i,
    I2: IntoIterator<Item = S2> + 'i,
    S1: AsRef<str> + 'i,
    S2: AsRef<str> + 'i,
{
    let fields_ids_map = index.fields_ids_map(rtxn)?;

    let attributes_to_retrieve: BTreeSet<_> = match attributes_to_retrieve {
        Some(attributes) => attributes
            .into_iter()
            .filter(|attribute| fields_ids_map.id(attribute.as_ref()).is_some())
            .map(|attribute| attribute.as_ref().to_string())
            .collect(),
        None => fields_ids_map.iter().map(|(_, name)| name.to_string()).collect(),
    };

    let extra_attributes_to_retrieve: Option<Vec<_>> = match extra_attributes_to_retrieve {
        Some(attributes) => Some(
            attributes
                .into_iter()
                .filter(|attribute| {
                    fields_ids_map
                        .id(attribute.as_ref())
                        .is_some()
                        // skip attributes that are already in attributes_to_retrieve
                        && !attributes_to_retrieve.contains(attribute.as_ref())
                })
                .map(|attribute| attribute.as_ref().to_string())
                .collect(),
        )
        .filter(|ids: &Vec<_>| !ids.is_empty()),
        None => None,
    };

    Ok(index.iter_documents(rtxn, doc_ids)?.map(move |ret| {
        ret.map_err(ResponseError::from).and_then(|(key, obkv)| -> Result<_, ResponseError> {
            let mut document = make_document(obkv, &fields_ids_map, &attributes_to_retrieve)?;
            let extra_document = extra_attributes_to_retrieve
                .as_ref()
                .map(|extra_attributes| make_document(obkv, &fields_ids_map, extra_attributes))
                .transpose()?;
            match retrieve_vectors {
                RetrieveVectors::Hide => {
                    document.remove("_vectors");
                }
                RetrieveVectors::Retrieve => {
                    let mut vectors = match document.remove("_vectors") {
                        Some(Value::Object(map)) => map,
                        _ => Default::default(),
                    };
                    for (
                        name,
                        EmbeddingsWithMetadata { embeddings, regenerate, has_fragments: _ },
                    ) in index.embeddings(rtxn, key)?
                    {
                        let embeddings =
                            ExplicitVectors { embeddings: Some(embeddings.into()), regenerate };
                        vectors.insert(
                            name,
                            serde_json::to_value(embeddings).map_err(MeilisearchHttpError::from)?,
                        );
                    }
                    document.insert("_vectors".into(), vectors.into());
                }
            }

            Ok((document, extra_document.unwrap_or_default()))
        })
    }))
}

#[allow(clippy::too_many_arguments)]
fn retrieve_documents<S: AsRef<str>>(
    index: &Index,
    rtxn: &RoTxn,
    offset: usize,
    limit: usize,
    ids: Option<Vec<ExternalDocumentId>>,
    filter: Option<IndexFilter>,
    attributes_to_retrieve: Option<Vec<S>>,
    retrieve_vectors: RetrieveVectors,
    sort_criteria: Option<Vec<AscDesc>>,
    is_proxy: bool,
) -> Result<(u64, Vec<Document>), ResponseError> {
    let mut candidates = if let Some(ids) = ids {
        let external_document_ids = index.external_documents_ids();
        let mut candidates = RoaringBitmap::new();
        for id in ids.iter() {
            let Some(docid) = external_document_ids.get(rtxn, id)? else {
                continue;
            };
            candidates.insert(docid);
        }
        candidates
    } else {
        index.documents_ids(rtxn)?
    };

    if let Some(filter) = filter {
        candidates &= filter.evaluate(rtxn, index).map_err(|err| match err {
            milli::Error::UserError(milli::UserError::InvalidFilter(_)) => {
                ResponseError::from_msg(err.to_string(), Code::InvalidDocumentFilter)
            }
            e => e.into(),
        })?
    }

    let (it, number_of_documents) = if let Some(sort) = sort_criteria.as_ref() {
        let number_of_documents = candidates.len();
        let facet_sort = recursive_sort(index, rtxn, sort, &candidates)?;
        let iter = facet_sort.iter()?;
        let mut documents = Vec::with_capacity(limit);
        for result in iter.skip(offset).take(limit) {
            documents.push(result?);
        }

        // retrieve each facet values for the documents if is_proxy is true
        let extra_attributes_to_retrieve: Option<_> = if is_proxy {
            Some(sort.iter().map(|asc_desc| asc_desc.field().unwrap_or(RESERVED_GEO_FIELD_NAME)))
        } else {
            None
        };

        (
            itertools::Either::Left(some_documents(
                index,
                rtxn,
                documents.into_iter(),
                retrieve_vectors,
                attributes_to_retrieve,
                extra_attributes_to_retrieve,
            )?),
            number_of_documents,
        )
    } else {
        let number_of_documents = candidates.len();
        let extra_attributes_to_retrieve: Option<Vec<String>> = None;
        (
            itertools::Either::Right(some_documents(
                index,
                rtxn,
                candidates.into_iter().skip(offset).take(limit),
                retrieve_vectors,
                attributes_to_retrieve,
                extra_attributes_to_retrieve,
            )?),
            number_of_documents,
        )
    };

    let documents: Vec<_> = it
        .map(|res| {
            let (mut document, extra_document) = res?;

            // retrieve each facet values for the documents if is_proxy is true
            if is_proxy {
                let mut weighted_score_values = Vec::new();
                if let Some(sort) = sort_criteria.as_ref() {
                    for asc_desc in sort {
                        let weighted_score_value =
                            build_weighted_score_value(asc_desc, &(&document, &extra_document));

                        weighted_score_values.push(weighted_score_value);
                    }
                }

                // insert the federation hit
                document.insert(
                    FEDERATION_HIT.to_string(),
                    build_federation_hit(weighted_score_values),
                );
            }
            Ok(document)
        })
        .collect::<Result<_, ResponseError>>()?;

    Ok((number_of_documents, documents))
}

impl VisitFacetValues for (&Document, &Document) {
    fn document(&self) -> &Document {
        self.0
    }

    fn extra_document(&self) -> &Document {
        self.1
    }
}

fn build_weighted_score_value<D: VisitFacetValues>(
    asc_desc: &AscDesc,
    document: &D,
) -> WeightedScoreValue {
    let asc = asc_desc.is_asc();
    let member = asc_desc.member();

    match member {
        Member::Field(field) => {
            let mut best: Option<WeightedScoreValue> = None;
            document.facet_values(field, |facet_value| {
                let weighted_score_value =
                    WeightedScoreValue::Sort { asc, value: facet_value.into_value() };
                best = match best.take() {
                    Some(best) => {
                        // unwrap: comparison should be always possible, we are comparing the same type of scores
                        match best.partial_cmp(&weighted_score_value).unwrap() {
                            Ordering::Greater | Ordering::Equal => Some(best),
                            Ordering::Less => Some(weighted_score_value),
                        }
                    }
                    None => Some(weighted_score_value),
                };
            });

            best.unwrap_or(WeightedScoreValue::Sort { asc, value: Value::Null })
        }
        Member::Geo(target_point) => {
            let mut lat = None;
            document.facet_values(RESERVED_GEO_LAT_FIELD_NAME, |facet_value| {
                lat = facet_value.into_value().as_f64();
            });

            let mut lng = None;
            document.facet_values(RESERVED_GEO_LNG_FIELD_NAME, |facet_value| {
                lng = facet_value.into_value().as_f64();
            });

            let distance = if let (Some(lat), Some(lng)) = (lat, lng) {
                GeoSort { target_point: *target_point, ascending: asc, value: Some([lat, lng]) }
                    .distance()
            } else {
                None
            };

            WeightedScoreValue::GeoSort { asc, distance }
        }
    }
}

// TODO: factorize with build_federation_hit in search/federated/perform.rs by using a serializable struct?
fn build_federation_hit(scores: Vec<WeightedScoreValue>) -> serde_json::Value {
    let mut federation = serde_json::Map::new();

    // insert the weighted score values
    federation.insert(WEIGHTED_SCORE_VALUES.to_string(), serde_json::json!(scores));

    serde_json::Value::Object(federation)
}

fn retrieve_document<S: AsRef<str>>(
    index: &Index,
    doc_id: &str,
    attributes_to_retrieve: Option<&[S]>,
    retrieve_vectors: RetrieveVectors,
) -> Result<Document, ResponseError> {
    let txn = index.read_txn()?;

    let internal_id = index
        .external_documents_ids()
        .get(&txn, doc_id)?
        .ok_or_else(|| MeilisearchHttpError::DocumentNotFound(doc_id.to_string()))?;

    let extra_attributes_to_retrieve: Option<Vec<String>> = None;
    let (document, _extra_document) = some_documents(
        index,
        &txn,
        Some(internal_id),
        retrieve_vectors,
        attributes_to_retrieve,
        extra_attributes_to_retrieve,
    )?
    .next()
    .ok_or_else(|| MeilisearchHttpError::DocumentNotFound(doc_id.to_string()))??;

    Ok(document)
}

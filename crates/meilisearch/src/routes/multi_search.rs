use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::TotalProcessingTimeStep;
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use super::multi_search_analytics::MultiSearchAggregator;
use crate::analytics::Analytics;
use crate::documents_retrieval::{DocumentSearch, DocumentSearchResult};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::personalization::PersonalizationService;
use crate::routes::parse_include_metadata_header;
use crate::search::proxy::{PROXY_SEARCH_HEADER, PROXY_SEARCH_HEADER_VALUE};
use crate::search::{FederatedSearch, FederatedSearchResult, SearchResultWithIndex};
use crate::search_queue::SearchQueue;

#[routes::routes(
    routes(
        "" => post(multi_search_with_post),
    ),
    tag = "Multi-search",
    tags((
        name = "Multi-search",
        description = "The `/multi-search` route allows you to perform multiple search queries on one or more indexes by bundling them into a single HTTP request. Multi-search is also known as federated search.",
    )),
)]
pub struct MultiSearchApi;

/// Response containing results from multiple search queries
#[derive(Serialize, ToSchema)]
pub struct SearchResults {
    /// Array of search results for each query
    results: Vec<SearchResultWithIndex>,
}

/// Perform a multi-search
///
/// Run multiple search queries in a single API request.
///
/// Each query can target a different index, so you can search across several indexes at once and get one combined response.
///
/// **Warning:** If Meilisearch encounters an error processing any query in the request, it immediately
/// stops and returns an error message for the first error encountered. Partial results are not returned.
#[routes::path(
    request_body = FederatedSearch,
    security(("Bearer" = ["search", "*"])),
    responses(
        (status = OK, description = "Non federated multi-search.", body = SearchResults, content_type = "application/json", example = json!(
            {
                "results":[
                    {
                        "indexUid":"movies",
                        "hits":[
                            {
                                "id":13682,
                                "title":"Pooh's Heffalump Movie",
                            },
                        ],
                        "query":"pooh",
                        "processingTimeMs":26,
                        "limit":1,
                        "offset":0,
                        "estimatedTotalHits":22
                    },
                    {
                        "indexUid":"movies",
                        "hits":[
                            {
                                "id":12,
                                "title":"Finding Nemo",
                            },
                        ],
                        "query":"nemo",
                        "processingTimeMs":5,
                        "limit":1,
                        "offset":0,
                        "estimatedTotalHits":11
                    },
                    {
                        "indexUid":"movie_ratings",
                        "hits":[
                            {
                                "id":"Us",
                                "director": "Jordan Peele",
                            }
                        ],
                        "query":"Us",
                        "processingTimeMs":0,
                        "limit":1,
                        "offset":0,
                        "estimatedTotalHits":1
                    }
                ]
            }
        )),
        (status = OK, description = "Federated multi-search.", body = FederatedSearchResult, content_type = "application/json", example = json!(
            {
                "hits": [
                    {
                        "id": 42,
                        "title": "Batman returns",
                        "overview": "The overview of batman returns",
                        "_federation": {
                            "indexUid": "movies",
                            "queriesPosition": 0
                        }
                    },
                    {
                        "comicsId": "batman-killing-joke",
                        "description": "This comic is really awesome",
                        "title": "Batman: the killing joke",
                        "_federation": {
                            "indexUid": "comics",
                            "queriesPosition": 1
                        }
                    },
                ],
                "processingTimeMs": 0,
                "limit": 20,
                "offset": 0,
                "estimatedTotalHits": 2,
                "semanticHitCount": 0
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
pub async fn multi_search_with_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: Data<SearchQueue>,
    personalization_service: Data<PersonalizationService>,
    params: AwebJson<FederatedSearch, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    // Since we don't want to process half of the search requests and then get a permit refused
    // we're going to get one permit for the whole duration of the multi-search request.
    let progress = Progress::default();
    progress.update_progress(TotalProcessingTimeStep::WaitInQueue);
    let permit = search_queue.try_get_search_permit().await?;
    progress.update_progress(TotalProcessingTimeStep::Search);
    let request_uid = Uuid::now_v7();

    let federated_search = params.into_inner();

    let mut multi_aggregate = MultiSearchAggregator::from_federated_search(&federated_search);

    let FederatedSearch { queries, federation } = federated_search;

    // check remote header
    let is_proxy = req
        .headers()
        .get(PROXY_SEARCH_HEADER)
        .is_some_and(|value| value.as_bytes() == PROXY_SEARCH_HEADER_VALUE.as_bytes());

    let include_metadata = parse_include_metadata_header(&req);
    let document_retrieval = DocumentSearch {
        request_uid,
        queries,
        federation,
        is_proxy,
        include_metadata,
        personalization_service: (*personalization_service).clone(),
    };

    let search_results = document_retrieval.execute(index_scheduler, &progress).await;

    if search_results.is_ok() {
        multi_aggregate.succeed();
    }

    permit.drop().await;
    analytics.publish(multi_aggregate, &req);

    let search_results = search_results.map_err(|(mut err, query_index)| {
        // Add the query index that failed as context for the error message.
        // We're doing it only here and not directly in the `WithIndex` trait so that the `with_index` function returns a different type
        // of result and we can benefit from static typing.
        if let Some(query_index) = query_index {
            err.message = format!("Inside `.queries[{query_index}]`: {}", err.message);
        }
        err
    })?;

    match search_results {
        DocumentSearchResult::Federated(search_result) => {
            Ok(HttpResponse::Ok().json(search_result))
        }
        DocumentSearchResult::Multi(search_results) => {
            Ok(HttpResponse::Ok().json(SearchResults { results: search_results }))
        }
    }
}

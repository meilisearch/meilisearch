use actix_http::StatusCode;
use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use serde::Serialize;
use tracing::debug;

use crate::analytics::{Analytics, MultiSearchAggregator};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::indexes::search::search_kind;
use crate::search::{
    add_search_rules, perform_search, SearchQueryWithIndex, SearchResultWithIndex,
};
use crate::search_queue::SearchQueue;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(multi_search_with_post))));
}

#[derive(Serialize)]
struct SearchResults {
    results: Vec<SearchResultWithIndex>,
}

#[derive(Debug, deserr::Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct SearchQueries {
    queries: Vec<SearchQueryWithIndex>,
}

pub async fn multi_search_with_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: Data<SearchQueue>,
    params: AwebJson<SearchQueries, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let queries = params.into_inner().queries;

    let mut multi_aggregate = MultiSearchAggregator::from_queries(&queries, &req);
    let features = index_scheduler.features();

    // Since we don't want to process half of the search requests and then get a permit refused
    // we're going to get one permit for the whole duration of the multi-search request.
    let _permit = search_queue.try_get_search_permit().await?;

    // Explicitly expect a `(ResponseError, usize)` for the error type rather than `ResponseError` only,
    // so that `?` doesn't work if it doesn't use `with_index`, ensuring that it is not forgotten in case of code
    // changes.
    let search_results: Result<_, (ResponseError, usize)> = async {
        let mut search_results = Vec::with_capacity(queries.len());
        for (query_index, (index_uid, mut query)) in
            queries.into_iter().map(SearchQueryWithIndex::into_index_query).enumerate()
        {
            debug!(on_index = query_index, parameters = ?query, "Multi-search");

            // Check index from API key
            if !index_scheduler.filters().is_index_authorized(&index_uid) {
                return Err(AuthenticationError::InvalidToken).with_index(query_index);
            }
            // Apply search rules from tenant token
            if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid)
            {
                add_search_rules(&mut query, search_rules);
            }

            let index = index_scheduler
                .index(&index_uid)
                .map_err(|err| {
                    let mut err = ResponseError::from(err);
                    // Patch the HTTP status code to 400 as it defaults to 404 for `index_not_found`, but
                    // here the resource not found is not part of the URL.
                    err.code = StatusCode::BAD_REQUEST;
                    err
                })
                .with_index(query_index)?;

            let search_kind = search_kind(&query, index_scheduler.get_ref(), &index, features)
                .with_index(query_index)?;

            let search_result =
                tokio::task::spawn_blocking(move || perform_search(&index, query, search_kind))
                    .await
                    .with_index(query_index)?;

            search_results.push(SearchResultWithIndex {
                index_uid: index_uid.into_inner(),
                result: search_result.with_index(query_index)?,
            });
        }
        Ok(search_results)
    }
    .await;

    if search_results.is_ok() {
        multi_aggregate.succeed();
    }
    analytics.post_multi_search(multi_aggregate);

    let search_results = search_results.map_err(|(mut err, query_index)| {
        // Add the query index that failed as context for the error message.
        // We're doing it only here and not directly in the `WithIndex` trait so that the `with_index` function returns a different type
        // of result and we can benefit from static typing.
        err.message = format!("Inside `.queries[{query_index}]`: {}", err.message);
        err
    })?;

    debug!(returns = ?search_results, "Multi-search");

    Ok(HttpResponse::Ok().json(SearchResults { results: search_results }))
}

/// Local `Result` extension trait to avoid `map_err` boilerplate.
trait WithIndex {
    type T;
    /// convert the error type inside of the `Result` to a `ResponseError`, and return a couple of it + the usize.
    fn with_index(self, index: usize) -> Result<Self::T, (ResponseError, usize)>;
}

impl<T, E: Into<ResponseError>> WithIndex for Result<T, E> {
    type T = T;
    fn with_index(self, index: usize) -> Result<T, (ResponseError, usize)> {
        self.map_err(|err| (err.into(), index))
    }
}

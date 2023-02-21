use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use log::debug;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use serde::Serialize;

use crate::analytics::{Analytics, MultiSearchAggregator};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::search::{
    add_search_rules, perform_search, SearchQueryWithIndex, SearchResultWithIndex,
};

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
    params: AwebJson<SearchQueries, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let queries = params.into_inner().queries;

    let mut multi_aggregate = MultiSearchAggregator::from_queries(&queries, &req);

    let search_results: Result<_, ResponseError> = (|| {
        async {
            let mut search_results = Vec::with_capacity(queries.len());
            for (index_uid, mut query) in
                queries.into_iter().map(SearchQueryWithIndex::into_index_query)
            {
                debug!("search called with params: {:?}", query);

                // Check index from API key
                if !index_scheduler.filters().is_index_authorized(&index_uid) {
                    return Err(AuthenticationError::InvalidToken.into());
                }
                // Apply search rules from tenant token
                if let Some(search_rules) =
                    index_scheduler.filters().get_index_search_rules(&index_uid)
                {
                    add_search_rules(&mut query, search_rules);
                }

                let index = index_scheduler.index(&index_uid)?;
                let search_result =
                    tokio::task::spawn_blocking(move || perform_search(&index, query)).await?;

                search_results.push(SearchResultWithIndex {
                    index_uid: index_uid.into_inner(),
                    result: search_result?,
                });
            }
            Ok(search_results)
        }
    })()
    .await;

    if search_results.is_ok() {
        multi_aggregate.succeed();
    }
    analytics.post_multi_search(multi_aggregate);

    let search_results = search_results?;

    debug!("returns: {:?}", search_results);

    Ok(HttpResponse::Ok().json(SearchResults { results: search_results }))
}

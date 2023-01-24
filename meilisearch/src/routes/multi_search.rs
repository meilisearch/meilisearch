use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use log::debug;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;

use crate::analytics::{Analytics, SearchAggregator};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::json::ValidatedJson;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::search::{
    add_search_rules, perform_search, SearchQueryWithIndex, SearchResultWithIndex,
};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(search_with_post))));
}

pub async fn search_with_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    params: ValidatedJson<Vec<SearchQueryWithIndex>, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let queries = params.into_inner();
    let mut search_results = Vec::with_capacity(queries.len());
    for (index_uid, mut query) in queries.into_iter().map(SearchQueryWithIndex::into_index_query) {
        debug!("search called with params: {:?}", query);

        // Tenant token search_rules.
        if let Some(search_rules) =
            index_scheduler.filters().search_rules.get_index_search_rules(&index_uid)
        {
            add_search_rules(&mut query, search_rules);
        }

        let mut aggregate = SearchAggregator::from_query(&query, &req);

        let index = index_scheduler.index(&index_uid)?;
        let search_result =
            tokio::task::spawn_blocking(move || perform_search(&index, query)).await?;
        if let Ok(ref search_result) = search_result {
            aggregate.succeed(search_result);
        }
        analytics.post_search(aggregate);

        search_results.push(SearchResultWithIndex { index_uid, result: search_result? });
    }

    debug!("returns: {:?}", search_results);

    Ok(HttpResponse::Ok().json(search_results))
}

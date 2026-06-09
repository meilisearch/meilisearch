use std::sync::Arc;

use actix_web::web::Data;
use index_scheduler::IndexScheduler;
use meilisearch_types::{error::ResponseError, milli::progress::Progress};
use uuid::Uuid;

use crate::{
    error::MeilisearchHttpError,
    extractors::authentication::{policies::ActionPolicy, AuthenticationError, GuardedData},
    personalization::PersonalizationService,
    search::{
        add_search_rules, perform_federated_search, FederatedSearchResult, Federation,
        SearchQueryWithIndex, SearchResultWithIndex, ShowFederationInfo,
    },
};

pub struct DocumentSearch {
    pub queries: Vec<SearchQueryWithIndex>,
    pub federation: Option<Federation>,
    pub personalization_service: Arc<PersonalizationService>,
    pub is_proxy: bool,
    pub include_metadata: bool,
    pub request_uid: Uuid,
}

impl DocumentSearch {
    pub async fn execute<const P: u8>(
        mut self,
        guarded_index_scheduler: GuardedData<ActionPolicy<P>, Data<IndexScheduler>>,
        progress: &Progress,
    ) -> Result<DocumentSearchResult, (ResponseError, Option<usize>)> {
        // regardless of federation, check authorization and apply search rules
        'check_authorization: {
            let auth_filter = guarded_index_scheduler.filters();
            for (query_index, federated_query) in self.queries.iter_mut().enumerate() {
                let index_uid = federated_query.index_uid.as_str();
                // Check index from API key
                if !auth_filter.is_index_authorized(index_uid) {
                    break 'check_authorization Err(AuthenticationError::InvalidToken)
                        .with_index(query_index);
                }
                // Apply search rules from tenant token
                if let Some(search_rules) = auth_filter.get_index_search_rules(index_uid) {
                    add_search_rules(&mut federated_query.filter, search_rules);
                }
            }
            Ok(())
        }?;

        let index_scheduler = guarded_index_scheduler.clone();
        let features = index_scheduler.features();
        // Federated search
        if let Some(federation) = self.federation.take() {
            let (search_result, _) = perform_federated_search(
                index_scheduler,
                self.queries,
                federation,
                features,
                self.is_proxy,
                self.request_uid,
                self.include_metadata,
                ShowFederationInfo::Always,
                &self.personalization_service,
                progress,
            )
            .await?;

            return Ok(DocumentSearchResult::Federated(Box::new(search_result)));
        }

        // Multi-search
        let search_results: Result<_, (ResponseError, _)> = async {
            let mut search_results = Vec::with_capacity(self.queries.len());
            for (query_index, query) in self.queries.iter().enumerate() {
                if query.federation_options.is_some() {
                    return Err((
                        MeilisearchHttpError::FederationOptionsInNonFederatedRequest.into(),
                        Some(query_index),
                    ));
                }

                let (fixed_query, federation) = fixup_query_federation(query);

                let (search_result, _) = perform_federated_search(
                    index_scheduler.clone(),
                    vec![fixed_query],
                    federation,
                    features,
                    self.is_proxy,
                    self.request_uid,
                    self.include_metadata,
                    ShowFederationInfo::OnNetworkOnly,
                    &self.personalization_service,
                    progress,
                )
                .await
                // Fixup the query index for the error
                .map_err(|(err, _)| (err, Some(query_index)))?;

                search_results.push(SearchResultWithIndex {
                    index_uid: query.index_uid.to_string(),
                    result: search_result.into_search_result(
                        query.q.clone().unwrap_or_default(),
                        query.index_uid.as_str(),
                    ),
                });
            }

            Ok(search_results)
        }
        .await;

        search_results.map(DocumentSearchResult::Multi)
    }
}

fn fixup_query_federation(query: &SearchQueryWithIndex) -> (SearchQueryWithIndex, Federation) {
    let mut query = query.clone();
    // Move query parameters that make sense at the federation level
    // from the `SearchQueryWithIndex` to the `Federation`
    let SearchQueryWithIndex {
        index_uid,
        q: _,
        vector: _,
        media: _,
        hybrid: _,
        offset,
        limit,
        page,
        hits_per_page,
        attributes_to_retrieve: _,
        retrieve_vectors: _,
        attributes_to_crop: _,
        crop_length: _,
        attributes_to_highlight: _,
        show_ranking_score: _,
        show_ranking_score_details: _,
        show_performance_details,
        use_network: _,
        show_matches_position: _,
        filter: _,
        sort: _,
        distinct,
        facets,
        highlight_pre_tag: _,
        highlight_post_tag: _,
        crop_marker: _,
        matching_strategy: _,
        attributes_to_search_on: _,
        ranking_score_threshold: _,
        locales: _,
        personalize,
        federation_options: _,
    } = &mut query;

    let mut federation = Federation::default();
    let Federation {
        limit: federation_limit,
        offset: federation_offset,
        page: federation_page,
        hits_per_page: federation_hits_per_page,
        facets_by_index: _,
        merge_facets: _,
        show_performance_details: federation_show_performance_details,
        distinct: federation_distinct,
        personalize: federation_personalize,
    } = &mut federation;

    if let Some(limit) = limit.take() {
        *federation_limit = limit;
    }
    if let Some(offset) = offset.take() {
        *federation_offset = offset;
    }
    if let Some(page) = page.take() {
        *federation_page = Some(page);
    }
    if let Some(hits_per_page) = hits_per_page.take() {
        *federation_hits_per_page = Some(hits_per_page);
    }
    if let Some(distinct) = distinct.take() {
        *federation_distinct = Some(distinct);
    }

    if let Some(show_performance_details) = show_performance_details.take() {
        *federation_show_performance_details = show_performance_details;
    }

    if let Some(personalize) = personalize.take() {
        *federation_personalize = Some(personalize);
    }

    'facets: {
        if let Some(facets) = facets.take() {
            if facets.is_empty() {
                break 'facets;
            }
            let facets_by_index = federation.facets_by_index.entry(index_uid.clone()).or_default();
            *facets_by_index = Some(facets);
        }
    }

    (query, federation)
}

/// Local `Result` extension trait to avoid `map_err` boilerplate.
pub trait WithIndex {
    type T;
    /// convert the error type inside of the `Result` to a `ResponseError`, and
    /// return a couple of it + the query index.
    fn with_index(self, index: usize) -> Result<Self::T, (ResponseError, Option<usize>)>;

    /// convert the error type inside of the `Result` to a `ResponseError`, and
    /// return a couple of it + empty query index.
    fn without_index(self) -> Result<Self::T, (ResponseError, Option<usize>)>;
}

impl<T, E: Into<ResponseError>> WithIndex for Result<T, E> {
    type T = T;
    fn with_index(self, index: usize) -> Result<T, (ResponseError, Option<usize>)> {
        self.map_err(|err| (err.into(), Some(index)))
    }

    fn without_index(self) -> Result<T, (ResponseError, Option<usize>)> {
        self.map_err(|err| (err.into(), None))
    }
}

pub enum DocumentSearchResult {
    Federated(Box<FederatedSearchResult>),
    Multi(Vec<SearchResultWithIndex>),
}

use std::{rc::Rc, sync::Arc};

use actix_web::web::Data;
use index_scheduler::filter::{
    filters_into_index_filters, parse_filter, retrieve_foreign_keys_settings, SourceIndexUid,
};
use index_scheduler::{IndexScheduler, RoFeatures};
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::FederatingResultsStep;
use uuid::Uuid;

use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::personalization::PersonalizationService;
use crate::search::federated::types::{PreprocessableQuery, PreprocessedQuery};
use crate::search::hydration::HydrationContext;
use crate::search::{
    add_search_rules, perform_federated_search, FederatedSearchResult, Federation,
    SearchQueryWithIndex, SearchResultWithIndex, ShowFederationInfo,
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

        let (hydration_cache, preprocessed_queries) = preprocess_filters(
            index_scheduler.clone(),
            self.queries,
            features,
            self.is_proxy,
            progress,
        )
        .await?;

        // Federated search
        if let Some(federation) = self.federation.take() {
            let (search_result, _) = perform_federated_search(
                index_scheduler,
                preprocessed_queries,
                hydration_cache,
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
            let mut search_results = Vec::with_capacity(preprocessed_queries.len());
            for (query_index, query) in preprocessed_queries.into_iter().enumerate() {
                if query.query.federation_options.is_some() {
                    return Err((
                        MeilisearchHttpError::FederationOptionsInNonFederatedRequest.into(),
                        Some(query_index),
                    ));
                }

                let (q, index_uid, fixed_query, federation) = {
                    let PreprocessedQuery { query, filter } = query;
                    let q = query.q.clone();
                    let index_uid = query.index_uid.to_string();
                    let (fixed_query, federation) = fixup_query_federation(&query);

                    (q, index_uid, PreprocessedQuery { query: fixed_query, filter }, federation)
                };

                let (search_result, _) = perform_federated_search(
                    index_scheduler.clone(),
                    vec![fixed_query],
                    hydration_cache.clone(),
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
                    result: search_result
                        .into_search_result(q.unwrap_or_default(), index_uid.as_str()),
                    index_uid,
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

pub async fn preprocess_filters<Q: PreprocessableQuery>(
    index_scheduler: Data<IndexScheduler>,
    mut queries: Vec<Q>,
    features: RoFeatures,
    is_proxy: bool,
    progress: &Progress,
) -> Result<(Option<HydrationContext>, Vec<PreprocessedQuery<Q>>), (ResponseError, Option<usize>)> {
    progress.update_progress(FederatingResultsStep::PreprocessFilters);

    // Document join: list of indexes in the order of the queries
    // only create the hydration cache if the foreign keys feature is enabled
    let filter_values;
    let (hydration_cache, precomputed_filters) = if features.runtime_features().foreign_keys
        && !is_proxy
    {
        filter_values = queries.iter_mut().map(|q| q.filter_field().take()).collect::<Vec<_>>();
        let index_uids: Vec<_> =
            queries.iter().map(|q| SourceIndexUid(Rc::from(q.index_uid().as_str()))).collect();
        let foreign_keys_settings =
            retrieve_foreign_keys_settings(&index_scheduler, &index_uids).without_index()?;

        // parse each query filter and bind them to their respective index
        let filters = index_uids
            .iter()
            .zip(filter_values.iter())
            .enumerate()
            .map(|(query_index, (index_uid, filter))| match filter {
                Some(filter) => {
                    let filter = parse_filter(filter, Code::InvalidSearchFilter, features, None)
                        .with_index(query_index)?;

                    Ok((index_uid.clone(), filter))
                }
                None => Ok((index_uid.clone(), None)),
            })
            .collect::<Result<_, (ResponseError, Option<usize>)>>()?;

        // convert the filters to index filters by evaluating the foreign filters
        let filters: Vec<_> =
            filters_into_index_filters(filters, &foreign_keys_settings, &index_scheduler, progress)
                .without_index()?;

        let hydration_cache = HydrationContext::new(index_uids, foreign_keys_settings);
        (Some(hydration_cache), filters)
    } else {
        return Ok((None, Vec::new()));
    };

    let preprocessed_queries = queries
        .into_iter()
        .zip(precomputed_filters.into_iter())
        .map(|(query, filter)| PreprocessedQuery { query, filter })
        .collect();
    Ok((hydration_cache, preprocessed_queries))
}

use std::collections::{BTreeMap, VecDeque};
use std::{rc::Rc, sync::Arc};

use actix_web::web::Data;
use index_scheduler::{IndexScheduler, RoFeatures};
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::FederatingResultsStep;
use meilisearch_types::network::Network;
use uuid::Uuid;

use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::personalization::PersonalizationService;
use crate::routes::indexes::documents::{BrowseQueryWithIndex, DocumentsResult};
use crate::search::federated::types::{PreprocessableQuery, PreprocessedQuery};
use crate::search::hydration::HydrationContext;
use crate::search::proxy::{json_proxy, ProxySearchError, ProxySearchParams};
use crate::search::{
    add_search_rules, perform_federated_search, FederatedSearchResult, Federation,
    SearchQueryWithIndex, SearchResultWithIndex, ShowFederationInfo,
};

pub use preprocessing::{
    preprocess_filters, retrieve_foreign_keys_settings, ForeignIndexUid, ForeignKeysPerIndex,
    SourceIndexUid,
};

mod preprocessing;

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

        let network = index_scheduler.network();
        let (hydration_cache, preprocessed_queries) = preprocess_filters(
            index_scheduler.clone(),
            network.clone(),
            self.queries,
            features,
            self.is_proxy,
            progress,
            Code::InvalidSearchFilter,
        )
        .await?;

        // Federated search
        if let Some(federation) = self.federation.take() {
            let (search_result, _) = perform_federated_search(
                index_scheduler,
                network,
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
                    network.clone(),
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

const MAX_IN_FLIGHT_REQUESTS: usize = 40;

pub struct RemoteRetrieveDocuments {
    errors: BTreeMap<String, ResponseError>,
    results: Vec<(DocumentsResult, usize)>,
    in_flight_requests: VecDeque<
        tokio::task::JoinHandle<(Result<DocumentsResult, ProxySearchError>, String, usize)>,
    >,
}

impl RemoteRetrieveDocuments {
    pub async fn start(
        network: Network,
        params: ProxySearchParams,
        remote_queries: Vec<(usize, PreprocessedQuery<BrowseQueryWithIndex>)>,
    ) -> Result<Self, ResponseError> {
        let mut errors = BTreeMap::new();
        let mut results = Vec::with_capacity(remote_queries.len());
        let mut in_flight_requests = VecDeque::with_capacity(MAX_IN_FLIGHT_REQUESTS);

        for (query_id, query) in remote_queries {
            let BrowseQueryWithIndex { query, remote: Some(remote_name), index_uid } =
                query.into_inner_preprocessed()
            else {
                unreachable!("remote query must have a remote name");
            };

            let Some(remote) = network.remotes.get(&remote_name) else {
                errors.insert(
                    remote_name.clone(),
                    ProxySearchError::UnknownRemote { remote: remote_name.clone() }
                        .as_response_error(),
                );
                continue;
            };

            let path_and_query =
                match meilisearch_types::network::route::documents_fetch_path(&index_uid) {
                    Ok(path_and_query) => path_and_query,
                    Err(err) => {
                        errors.insert(
                            remote_name.clone(),
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
                    errors.insert(remote_name.clone(), err.as_response_error());
                    continue;
                }
            };

            if in_flight_requests.len() == in_flight_requests.capacity() {
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
            in_flight_requests.push_back(tokio::spawn(async move {
                (request.await, remote_name.clone(), query_id)
            }));
        }

        Ok(Self { errors, results, in_flight_requests })
    }

    pub async fn wait(self) -> (Vec<(DocumentsResult, usize)>, BTreeMap<String, ResponseError>) {
        let Self { mut results, mut errors, in_flight_requests } = self;
        // Retrieve remote results
        for task in in_flight_requests {
            match task.await.unwrap() {
                (Ok(result), _, query_id) => results.push((result, query_id)),
                (Err(err), remote_name, _) => {
                    errors.insert(remote_name, err.as_response_error());
                }
            }
        }

        (results, errors)
    }
}

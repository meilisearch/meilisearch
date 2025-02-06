use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::iter::Zip;
use std::rc::Rc;
use std::str::FromStr as _;
use std::time::{Duration, Instant};
use std::vec::{IntoIter, Vec};

use actix_http::StatusCode;
use index_scheduler::{IndexScheduler, RoFeatures};
use itertools::Itertools;
use meilisearch_types::error::ResponseError;
use meilisearch_types::features::{Network, Remote};
use meilisearch_types::milli::order_by_map::OrderByMap;
use meilisearch_types::milli::score_details::{ScoreDetails, WeightedScoreValue};
use meilisearch_types::milli::{self, DocumentId, OrderBy, TimeBudget, DEFAULT_VALUES_PER_FACET};
use roaring::RoaringBitmap;
use tokio::task::JoinHandle;

use super::super::ranking_rules::{self, RankingRules};
use super::super::{
    compute_facet_distribution_stats, prepare_search, AttributesFormat, ComputedFacets, HitMaker,
    HitsInfo, RetrieveVectors, SearchHit, SearchKind, SearchQuery, SearchQueryWithIndex,
};
use super::proxy::{proxy_search, ProxySearchError, ProxySearchParams};
use super::types::{
    FederatedFacets, FederatedSearchResult, Federation, FederationOptions, MergeFacets, Weight,
    FEDERATION_HIT, FEDERATION_REMOTE, WEIGHTED_SCORE_VALUES,
};
use super::weighted_scores;
use crate::error::MeilisearchHttpError;
use crate::routes::indexes::search::search_kind;
use crate::search::federated::types::{INDEX_UID, QUERIES_POSITION, WEIGHTED_RANKING_SCORE};

pub async fn perform_federated_search(
    index_scheduler: &IndexScheduler,
    queries: Vec<SearchQueryWithIndex>,
    federation: Federation,
    features: RoFeatures,
    is_proxy: bool,
) -> Result<FederatedSearchResult, ResponseError> {
    if is_proxy {
        features.check_network("Performing a remote federated search")?;
    }
    let before_search = std::time::Instant::now();
    let deadline = before_search + std::time::Duration::from_secs(9);

    let required_hit_count = federation.limit + federation.offset;

    let network = index_scheduler.network();

    // this implementation partition the queries by index to guarantee an important property:
    // - all the queries to a particular index use the same read transaction.
    // This is an important property, otherwise we cannot guarantee the self-consistency of the results.

    // 1. partition queries by host and index
    let mut partitioned_queries = PartitionedQueries::new();
    for (query_index, federated_query) in queries.into_iter().enumerate() {
        partitioned_queries.partition(federated_query, query_index, &network, features)?
    }

    // 2. perform queries, merge and make hits index by index
    // 2.1. start remote queries
    let remote_search =
        RemoteSearch::start(partitioned_queries.remote_queries_by_host, &federation, deadline);

    // 2.2. concurrently execute local queries
    let params = SearchByIndexParams {
        index_scheduler,
        features,
        is_proxy,
        network: &network,
        has_remote: partitioned_queries.has_remote,
        required_hit_count,
    };
    let mut search_by_index = SearchByIndex::new(
        federation,
        partitioned_queries.local_queries_by_index.len(),
        params.has_remote,
    );

    for (index_uid, queries) in partitioned_queries.local_queries_by_index {
        // note: this is the only place we open `index_uid`
        search_by_index.execute(index_uid, queries, &params)?;
    }

    // bonus step, make sure to return an error if an index wants a non-faceted field, even if no query actually uses that index.
    search_by_index.check_unused_facets(index_scheduler)?;

    let SearchByIndex {
        federation,
        mut semantic_hit_count,
        mut results_by_index,
        previous_query_data: _,
        facet_order,
    } = search_by_index;

    let before_waiting_remote_results = std::time::Instant::now();

    // 2.3. Wait for proxy search requests to complete
    let (mut remote_results, remote_errors) = remote_search.finish().await;

    let after_waiting_remote_results = std::time::Instant::now();

    // 3. merge hits and metadata across indexes and hosts
    // 3.1. merge metadata
    let (estimated_total_hits, degraded, used_negative_operator, facets, max_remote_duration) =
        merge_metadata(&mut results_by_index, &remote_results);

    // 3.2. merge hits
    let merged_hits: Vec<_> = merge_index_global_results(results_by_index, &mut remote_results)
        .skip(federation.offset)
        .take(federation.limit)
        .inspect(|hit| {
            if let Some(semantic_hit_count) = &mut semantic_hit_count {
                if hit.to_score().0.any(|score| matches!(&score, WeightedScoreValue::VectorSort(_)))
                {
                    *semantic_hit_count += 1;
                }
            }
        })
        .map(|hit| hit.hit())
        .collect();

    // 3.3. merge facets
    let (facet_distribution, facet_stats, facets_by_index) =
        facet_order.merge(federation.merge_facets, remote_results, facets);

    let after_merge = std::time::Instant::now();

    let local_duration = (before_waiting_remote_results - before_search)
        + (after_merge - after_waiting_remote_results);
    let max_duration = Duration::max(local_duration, max_remote_duration);

    Ok(FederatedSearchResult {
        hits: merged_hits,
        processing_time_ms: max_duration.as_millis(),
        hits_info: HitsInfo::OffsetLimit {
            limit: federation.limit,
            offset: federation.offset,
            estimated_total_hits,
        },
        semantic_hit_count,
        degraded,
        used_negative_operator,
        facet_distribution,
        facet_stats,
        facets_by_index,
        remote_errors: partitioned_queries.has_remote.then_some(remote_errors),
    })
}

struct QueryByIndex {
    query: SearchQuery,
    weight: Weight,
    query_index: usize,
}

struct SearchResultByQuery<'a> {
    documents_ids: Vec<DocumentId>,
    document_scores: Vec<Vec<ScoreDetails>>,
    weight: Weight,
    hit_maker: HitMaker<'a>,
    query_index: usize,
}

struct SearchResultByQueryIter<'a> {
    it: Zip<IntoIter<DocumentId>, IntoIter<Vec<ScoreDetails>>>,
    weight: Weight,
    hit_maker: Rc<HitMaker<'a>>,
    query_index: usize,
}

impl<'a> SearchResultByQueryIter<'a> {
    fn new(
        SearchResultByQuery {
        documents_ids,
        document_scores,
        weight,
        hit_maker,
        query_index,
    }: SearchResultByQuery<'a>,
    ) -> Self {
        let it = documents_ids.into_iter().zip(document_scores);
        Self { it, weight, hit_maker: Rc::new(hit_maker), query_index }
    }
}

struct SearchResultByQueryIterItem<'a> {
    docid: DocumentId,
    score: Vec<ScoreDetails>,
    weight: Weight,
    hit_maker: Rc<HitMaker<'a>>,
    query_index: usize,
}

fn merge_index_local_results(
    results_by_query: Vec<SearchResultByQuery<'_>>,
) -> impl Iterator<Item = SearchResultByQueryIterItem> + '_ {
    itertools::kmerge_by(
        results_by_query.into_iter().map(SearchResultByQueryIter::new),
        |left: &SearchResultByQueryIterItem, right: &SearchResultByQueryIterItem| {
            match weighted_scores::compare(
                ScoreDetails::weighted_score_values(left.score.iter(), *left.weight),
                ScoreDetails::global_score(left.score.iter()) * *left.weight,
                ScoreDetails::weighted_score_values(right.score.iter(), *right.weight),
                ScoreDetails::global_score(right.score.iter()) * *right.weight,
            ) {
                // the biggest score goes first
                Ordering::Greater => true,
                // break ties using query index
                Ordering::Equal => left.query_index < right.query_index,
                Ordering::Less => false,
            }
        },
    )
}

fn merge_index_global_results(
    results_by_index: Vec<SearchResultByIndex>,
    remote_results: &mut [FederatedSearchResult],
) -> impl Iterator<Item = MergedSearchHit> + '_ {
    itertools::kmerge_by(
        // local results
        results_by_index
            .into_iter()
            .map(|result_by_index| {
                either::Either::Left(result_by_index.hits.into_iter().map(MergedSearchHit::Local))
            })
            // remote results
            .chain(remote_results.iter_mut().map(|x| either::Either::Right(iter_remote_hits(x)))),
        |left: &MergedSearchHit, right: &MergedSearchHit| {
            let (left_it, left_weighted_global_score, left_query_index) = left.to_score();
            let (right_it, right_weighted_global_score, right_query_index) = right.to_score();

            match weighted_scores::compare(
                left_it,
                left_weighted_global_score,
                right_it,
                right_weighted_global_score,
            ) {
                // the biggest score goes first
                Ordering::Greater => true,
                // break ties using query index
                Ordering::Equal => left_query_index < right_query_index,
                Ordering::Less => false,
            }
        },
    )
}

enum MergedSearchHit {
    Local(SearchHitByIndex),
    Remote {
        hit: SearchHit,
        score: Vec<WeightedScoreValue>,
        global_weighted_score: f64,
        query_index: usize,
    },
}

impl MergedSearchHit {
    fn remote(mut hit: SearchHit) -> Result<Self, ProxySearchError> {
        let federation = hit
            .document
            .get_mut(FEDERATION_HIT)
            .ok_or(ProxySearchError::MissingPathInResponse("._federation"))?;
        let federation = match federation.as_object_mut() {
            Some(federation) => federation,
            None => {
                return Err(ProxySearchError::UnexpectedValueInPath {
                    path: "._federation",
                    expected_type: "map",
                    received_value: federation.to_string(),
                });
            }
        };

        let global_weighted_score = federation
            .get(WEIGHTED_RANKING_SCORE)
            .ok_or(ProxySearchError::MissingPathInResponse("._federation.weightedRankingScore"))?;
        let global_weighted_score = global_weighted_score.as_f64().ok_or_else(|| {
            ProxySearchError::UnexpectedValueInPath {
                path: "._federation.weightedRankingScore",
                expected_type: "number",
                received_value: global_weighted_score.to_string(),
            }
        })?;

        let score: Vec<WeightedScoreValue> =
            serde_json::from_value(federation.remove(WEIGHTED_SCORE_VALUES).ok_or(
                ProxySearchError::MissingPathInResponse("._federation.weightedScoreValues"),
            )?)
            .map_err(ProxySearchError::CouldNotParseWeightedScoreValues)?;

        let query_index = federation
            .get(QUERIES_POSITION)
            .ok_or(ProxySearchError::MissingPathInResponse("._federation.queriesPosition"))?;
        let query_index =
            query_index.as_u64().ok_or_else(|| ProxySearchError::UnexpectedValueInPath {
                path: "._federation.queriesPosition",
                expected_type: "integer",
                received_value: query_index.to_string(),
            })? as usize;

        Ok(Self::Remote { hit, score, global_weighted_score, query_index })
    }

    fn hit(self) -> SearchHit {
        match self {
            MergedSearchHit::Local(search_hit_by_index) => search_hit_by_index.hit,
            MergedSearchHit::Remote { hit, .. } => hit,
        }
    }

    fn to_score(&self) -> (impl Iterator<Item = WeightedScoreValue> + '_, f64, usize) {
        match self {
            MergedSearchHit::Local(search_hit_by_index) => (
                either::Left(ScoreDetails::weighted_score_values(
                    search_hit_by_index.score.iter(),
                    *search_hit_by_index.weight,
                )),
                ScoreDetails::global_score(search_hit_by_index.score.iter())
                    * *search_hit_by_index.weight,
                search_hit_by_index.query_index,
            ),
            MergedSearchHit::Remote { hit: _, score, global_weighted_score, query_index } => {
                let global_weighted_score = *global_weighted_score;
                let query_index = *query_index;
                (either::Right(score.iter().cloned()), global_weighted_score, query_index)
            }
        }
    }
}

fn iter_remote_hits(
    results_by_host: &mut FederatedSearchResult,
) -> impl Iterator<Item = MergedSearchHit> + '_ {
    // have a per node registry of failed hits
    results_by_host.hits.drain(..).filter_map(|hit| match MergedSearchHit::remote(hit) {
        Ok(hit) => Some(hit),
        Err(err) => {
            tracing::warn!("skipping remote hit due to error: {err}");
            None
        }
    })
}

impl<'a> Iterator for SearchResultByQueryIter<'a> {
    type Item = SearchResultByQueryIterItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let (docid, score) = self.it.next()?;
        Some(SearchResultByQueryIterItem {
            docid,
            score,
            weight: self.weight,
            hit_maker: Rc::clone(&self.hit_maker),
            query_index: self.query_index,
        })
    }
}

struct SearchHitByIndex {
    hit: SearchHit,
    score: Vec<ScoreDetails>,
    weight: Weight,
    query_index: usize,
}

struct SearchResultByIndex {
    index: String,
    hits: Vec<SearchHitByIndex>,
    estimated_total_hits: usize,
    degraded: bool,
    used_negative_operator: bool,
    facets: Option<ComputedFacets>,
}

fn merge_metadata(
    results_by_index: &mut Vec<SearchResultByIndex>,
    remote_results: &Vec<FederatedSearchResult>,
) -> (usize, bool, bool, FederatedFacets, Duration) {
    let mut estimated_total_hits = 0;
    let mut degraded = false;
    let mut used_negative_operator = false;
    let mut facets: FederatedFacets = FederatedFacets::default();
    let mut max_remote_duration = Duration::ZERO;
    for SearchResultByIndex {
        index,
        hits: _,
        estimated_total_hits: estimated_total_hits_by_index,
        facets: facets_by_index,
        degraded: degraded_by_index,
        used_negative_operator: used_negative_operator_by_index,
    } in results_by_index
    {
        estimated_total_hits += *estimated_total_hits_by_index;
        degraded |= *degraded_by_index;
        used_negative_operator |= *used_negative_operator_by_index;

        let facets_by_index = std::mem::take(facets_by_index);
        let index = std::mem::take(index);

        facets.insert(index, facets_by_index);
    }
    for FederatedSearchResult {
        hits: _,
        processing_time_ms,
        hits_info,
        semantic_hit_count: _,
        facet_distribution: _,
        facet_stats: _,
        facets_by_index: _,
        degraded: degraded_for_host,
        used_negative_operator: host_used_negative_operator,
        remote_errors: _,
    } in remote_results
    {
        let this_remote_duration = Duration::from_millis(*processing_time_ms as u64);
        max_remote_duration = Duration::max(this_remote_duration, max_remote_duration);
        estimated_total_hits += match hits_info {
            HitsInfo::Pagination { total_hits: estimated_total_hits, .. }
            | HitsInfo::OffsetLimit { estimated_total_hits, .. } => estimated_total_hits,
        };
        // note that because `degraded` and `used_negative_operator` are #[serde(skip)],
        // `degraded_for_host` and `host_used_negative_operator` will always be false.
        degraded |= degraded_for_host;
        used_negative_operator |= host_used_negative_operator;
    }
    (estimated_total_hits, degraded, used_negative_operator, facets, max_remote_duration)
}

type LocalQueriesByIndex = BTreeMap<String, Vec<QueryByIndex>>;
type RemoteQueriesByHost = BTreeMap<String, (Remote, Vec<SearchQueryWithIndex>)>;

struct PartitionedQueries {
    local_queries_by_index: LocalQueriesByIndex,
    remote_queries_by_host: RemoteQueriesByHost,
    has_remote: bool,
}

impl PartitionedQueries {
    fn new() -> PartitionedQueries {
        PartitionedQueries {
            local_queries_by_index: Default::default(),
            remote_queries_by_host: Default::default(),
            has_remote: false,
        }
    }

    fn partition(
        &mut self,
        federated_query: SearchQueryWithIndex,
        query_index: usize,
        network: &Network,
        features: RoFeatures,
    ) -> Result<(), ResponseError> {
        if let Some(pagination_field) = federated_query.has_pagination() {
            return Err(MeilisearchHttpError::PaginationInFederatedQuery(
                query_index,
                pagination_field,
            )
            .into());
        }

        if let Some(facets) = federated_query.has_facets() {
            let facets = facets.to_owned();
            return Err(MeilisearchHttpError::FacetsInFederatedQuery(
                query_index,
                federated_query.index_uid.into_inner(),
                facets,
            )
            .into());
        }

        let (index_uid, query, federation_options) = federated_query.into_index_query_federation();

        let federation_options = federation_options.unwrap_or_default();

        // local or remote node?
        'local_query: {
            let queries_by_index = match federation_options.remote {
                None => self.local_queries_by_index.entry(index_uid.into_inner()).or_default(),
                Some(remote_name) => {
                    self.has_remote = true;
                    features.check_network("Performing a remote federated search")?;

                    match &network.local {
                        Some(local) if local == &remote_name => {
                            self.local_queries_by_index.entry(index_uid.into_inner()).or_default()
                        }
                        _ => {
                            // node from the network
                            let Some(remote) = network.remotes.get(&remote_name) else {
                                return Err(ResponseError::from_msg(format!("Invalid `queries[{query_index}].federation_options.remote`: remote `{remote_name}` is not registered"),
                           meilisearch_types::error::Code::InvalidMultiSearchRemote));
                            };
                            let query = SearchQueryWithIndex::from_index_query_federation(
                                index_uid,
                                query,
                                Some(FederationOptions {
                                    weight: federation_options.weight,
                                    // do not pass the `remote` to not require the remote instance to have itself has a local node
                                    remote: None,
                                    // pass an explicit query index
                                    query_position: Some(query_index),
                                }),
                            );

                            self.remote_queries_by_host
                                .entry(remote_name)
                                .or_insert_with(|| (remote.clone(), Default::default()))
                                .1
                                .push(query);
                            break 'local_query;
                        }
                    }
                }
            };

            queries_by_index.push(QueryByIndex {
                query,
                weight: federation_options.weight,
                // override query index here with the one in federation.
                // this will fix-up error messages to refer to the global query index of the original request.
                query_index: if let Some(query_index) = federation_options.query_position {
                    features.check_network("Using `federationOptions.queryPosition`")?;
                    query_index
                } else {
                    query_index
                },
            })
        }
        Ok(())
    }
}

struct RemoteSearch {
    in_flight_remote_queries:
        BTreeMap<String, JoinHandle<Result<FederatedSearchResult, ProxySearchError>>>,
}

impl RemoteSearch {
    fn start(queries: RemoteQueriesByHost, federation: &Federation, deadline: Instant) -> Self {
        let mut in_flight_remote_queries = BTreeMap::new();
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap();
        let params =
            ProxySearchParams { deadline: Some(deadline), try_count: 3, client: client.clone() };
        for (node_name, (node, queries)) in queries {
            // spawn one task per host
            in_flight_remote_queries.insert(
                node_name,
                tokio::spawn({
                    let mut proxy_federation = federation.clone();
                    // fixup limit and offset to not apply them twice
                    proxy_federation.limit = federation.limit + federation.offset;
                    proxy_federation.offset = 0;
                    // never merge distant facets
                    proxy_federation.merge_facets = None;
                    let params = params.clone();
                    async move { proxy_search(&node, queries, proxy_federation, &params).await }
                }),
            );
        }
        Self { in_flight_remote_queries }
    }

    async fn finish(self) -> (Vec<FederatedSearchResult>, BTreeMap<String, ResponseError>) {
        let mut remote_results = Vec::with_capacity(self.in_flight_remote_queries.len());
        let mut remote_errors: BTreeMap<String, ResponseError> = BTreeMap::new();
        'remote_queries: for (node_name, handle) in self.in_flight_remote_queries {
            match handle.await {
                Ok(Ok(mut res)) => {
                    for hit in &mut res.hits {
                        let Some(federation) = hit.document.get_mut(FEDERATION_HIT) else {
                            let error = ProxySearchError::MissingPathInResponse("._federation");
                            remote_errors.insert(node_name, error.as_response_error());
                            continue 'remote_queries;
                        };
                        let Some(federation) = federation.as_object_mut() else {
                            let error = ProxySearchError::UnexpectedValueInPath {
                                path: "._federation",
                                expected_type: "map",
                                received_value: federation.to_string(),
                            };
                            remote_errors.insert(node_name, error.as_response_error());
                            continue 'remote_queries;
                        };
                        if !federation.contains_key(WEIGHTED_SCORE_VALUES) {
                            let error = ProxySearchError::MissingPathInResponse(
                                "._federation.weightedScoreValues",
                            );
                            remote_errors.insert(node_name, error.as_response_error());
                            continue 'remote_queries;
                        }

                        if !federation.contains_key(WEIGHTED_RANKING_SCORE) {
                            let error = ProxySearchError::MissingPathInResponse(
                                "._federation.weightedRankingScore",
                            );
                            remote_errors.insert(node_name, error.as_response_error());
                            continue 'remote_queries;
                        }

                        federation.insert(
                            FEDERATION_REMOTE.to_string(),
                            serde_json::Value::String(node_name.clone()),
                        );
                    }

                    remote_results.push(res);
                }
                Ok(Err(error)) => {
                    remote_errors.insert(node_name, error.as_response_error());
                }
                Err(panic) => match panic.try_into_panic() {
                    Ok(panic) => {
                        let msg = match panic.downcast_ref::<&'static str>() {
                            Some(s) => *s,
                            None => match panic.downcast_ref::<String>() {
                                Some(s) => &s[..],
                                None => "Box<dyn Any>",
                            },
                        };
                        remote_errors.insert(
                            node_name,
                            ResponseError::from_msg(
                                msg.to_string(),
                                meilisearch_types::error::Code::Internal,
                            ),
                        );
                    }
                    Err(_) => tracing::error!("proxy search task was unexpectedly cancelled"),
                },
            }
        }
        (remote_results, remote_errors)
    }
}

struct SearchByIndexParams<'a> {
    index_scheduler: &'a IndexScheduler,
    required_hit_count: usize,
    features: RoFeatures,
    is_proxy: bool,
    has_remote: bool,
    network: &'a Network,
}

struct SearchByIndex {
    federation: Federation,
    // During search by index, semantic_hit_count will be set to Some(0) if any search kind uses semantic
    // Then when merging, we'll update its value if there is any semantic hit
    semantic_hit_count: Option<u32>,
    results_by_index: Vec<SearchResultByIndex>,
    previous_query_data: Option<(RankingRules, usize, String)>,
    // remember the order and name of first index for each facet when merging with index settings
    // to detect if the order is inconsistent for a facet.
    facet_order: FacetOrder,
}

impl SearchByIndex {
    fn new(federation: Federation, index_count: usize, has_remote: bool) -> Self {
        SearchByIndex {
            facet_order: match (federation.merge_facets, has_remote) {
                (None, true) => FacetOrder::ByIndex(Default::default()),
                (None, false) => FacetOrder::None,
                (Some(_), _) => FacetOrder::ByFacet(Default::default()),
            },
            federation,
            semantic_hit_count: None,
            results_by_index: Vec::with_capacity(index_count),
            previous_query_data: None,
        }
    }

    fn execute(
        &mut self,
        index_uid: String,
        queries: Vec<QueryByIndex>,
        params: &SearchByIndexParams<'_>,
    ) -> Result<(), ResponseError> {
        let first_query_index = queries.first().map(|query| query.query_index);
        let index = match params.index_scheduler.index(&index_uid) {
            Ok(index) => index,
            Err(err) => {
                let mut err = ResponseError::from(err);
                // Patch the HTTP status code to 400 as it defaults to 404 for `index_not_found`, but
                // here the resource not found is not part of the URL.
                err.code = StatusCode::BAD_REQUEST;
                if let Some(query_index) = first_query_index {
                    err.message = format!("Inside `.queries[{}]`: {}", query_index, err.message);
                }
                return Err(err);
            }
        };
        let rtxn = index.read_txn()?;
        let criteria = index.criteria(&rtxn)?;
        let dictionary = index.dictionary(&rtxn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
        let separators = index.allowed_separators(&rtxn)?;
        let separators: Option<Vec<_>> =
            separators.as_ref().map(|x| x.iter().map(String::as_str).collect());
        let cutoff = index.search_cutoff(&rtxn)?;
        let mut degraded = false;
        let mut used_negative_operator = false;
        let mut candidates = RoaringBitmap::new();
        let facets_by_index = self.federation.facets_by_index.remove(&index_uid).flatten();
        if let Err(mut error) =
            self.facet_order.check_facet_order(&index_uid, &facets_by_index, &index, &rtxn)
        {
            error.message = format!(
                "Inside `.federation.facetsByIndex.{index_uid}`: {error}{}",
                if let Some(query_index) = first_query_index {
                    format!("\n - Note: index `{index_uid}` used in `.queries[{query_index}]`")
                } else {
                    Default::default()
                }
            );
            return Err(error);
        }
        let mut results_by_query = Vec::with_capacity(queries.len());
        for QueryByIndex { query, weight, query_index } in queries {
            // use an immediately invoked lambda to capture the result without returning from the function

            let res: Result<(), ResponseError> = (|| {
                let search_kind =
                    search_kind(&query, params.index_scheduler, index_uid.to_string(), &index)?;

                let canonicalization_kind = match (&search_kind, &query.q) {
                    (SearchKind::SemanticOnly { .. }, _) => {
                        ranking_rules::CanonicalizationKind::Vector
                    }
                    (_, Some(q)) if !q.is_empty() => ranking_rules::CanonicalizationKind::Keyword,
                    _ => ranking_rules::CanonicalizationKind::Placeholder,
                };

                let sort = if let Some(sort) = &query.sort {
                    let sorts: Vec<_> =
                        match sort.iter().map(|s| milli::AscDesc::from_str(s)).collect() {
                            Ok(sorts) => sorts,
                            Err(asc_desc_error) => {
                                return Err(milli::Error::from(milli::SortError::from(
                                    asc_desc_error,
                                ))
                                .into())
                            }
                        };
                    Some(sorts)
                } else {
                    None
                };

                let ranking_rules = ranking_rules::RankingRules::new(
                    criteria.clone(),
                    sort,
                    query.matching_strategy.into(),
                    canonicalization_kind,
                );

                if let Some((previous_ranking_rules, previous_query_index, previous_index_uid)) =
                    self.previous_query_data.take()
                {
                    if let Err(error) = ranking_rules.is_compatible_with(&previous_ranking_rules) {
                        return Err(error.to_response_error(
                            &ranking_rules,
                            &previous_ranking_rules,
                            query_index,
                            previous_query_index,
                            &index_uid,
                            &previous_index_uid,
                        ));
                    }
                    self.previous_query_data = if previous_ranking_rules.constraint_count()
                        > ranking_rules.constraint_count()
                    {
                        Some((previous_ranking_rules, previous_query_index, previous_index_uid))
                    } else {
                        Some((ranking_rules, query_index, index_uid.clone()))
                    };
                } else {
                    self.previous_query_data =
                        Some((ranking_rules, query_index, index_uid.clone()));
                }

                match search_kind {
                    SearchKind::KeywordOnly => {}
                    _ => self.semantic_hit_count = Some(0),
                }

                let retrieve_vectors = RetrieveVectors::new(query.retrieve_vectors);

                let time_budget = match cutoff {
                    Some(cutoff) => TimeBudget::new(Duration::from_millis(cutoff)),
                    None => TimeBudget::default(),
                };

                let (mut search, _is_finite_pagination, _max_total_hits, _offset) = prepare_search(
                    &index,
                    &rtxn,
                    &query,
                    &search_kind,
                    time_budget,
                    params.features,
                )?;

                search.scoring_strategy(milli::score_details::ScoringStrategy::Detailed);
                search.offset(0);
                search.limit(params.required_hit_count);

                let (result, _semantic_hit_count) =
                    super::super::search_from_kind(index_uid.to_string(), search_kind, search)?;
                let format = AttributesFormat {
                    attributes_to_retrieve: query.attributes_to_retrieve,
                    retrieve_vectors,
                    attributes_to_highlight: query.attributes_to_highlight,
                    attributes_to_crop: query.attributes_to_crop,
                    crop_length: query.crop_length,
                    crop_marker: query.crop_marker,
                    highlight_pre_tag: query.highlight_pre_tag,
                    highlight_post_tag: query.highlight_post_tag,
                    show_matches_position: query.show_matches_position,
                    sort: query.sort,
                    show_ranking_score: query.show_ranking_score,
                    show_ranking_score_details: query.show_ranking_score_details,
                    locales: query.locales.map(|l| l.iter().copied().map(Into::into).collect()),
                };

                let milli::SearchResult {
                    matching_words,
                    candidates: query_candidates,
                    documents_ids,
                    document_scores,
                    degraded: query_degraded,
                    used_negative_operator: query_used_negative_operator,
                } = result;

                candidates |= query_candidates;
                degraded |= query_degraded;
                used_negative_operator |= query_used_negative_operator;

                let tokenizer = HitMaker::tokenizer(dictionary.as_deref(), separators.as_deref());

                let formatter_builder = HitMaker::formatter_builder(matching_words, tokenizer);

                let hit_maker =
                    HitMaker::new(&index, &rtxn, format, formatter_builder).map_err(|e| {
                        MeilisearchHttpError::from_milli(e, Some(index_uid.to_string()))
                    })?;

                results_by_query.push(SearchResultByQuery {
                    weight,
                    hit_maker,
                    query_index,
                    documents_ids,
                    document_scores,
                });
                Ok(())
            })();

            if let Err(mut error) = res {
                error.message = format!("Inside `.queries[{query_index}]`: {}", error.message);
                return Err(error);
            }
        }
        let mut documents_seen = RoaringBitmap::new();
        let merged_result: Result<Vec<_>, ResponseError> =
            merge_index_local_results(results_by_query)
                // skip documents we've already seen & mark that we saw the current document
                .filter(|SearchResultByQueryIterItem { docid, .. }| documents_seen.insert(*docid))
                .take(params.required_hit_count)
                // 2.3 make hits
                .map(
                    |SearchResultByQueryIterItem {
                         docid,
                         score,
                         weight,
                         hit_maker,
                         query_index,
                     }| {
                        let mut hit = hit_maker.make_hit(docid, &score)?;
                        let weighted_score = ScoreDetails::global_score(score.iter()) * (*weight);

                        let mut _federation = serde_json::json!(
                            {
                                INDEX_UID: index_uid,
                                QUERIES_POSITION: query_index,
                                WEIGHTED_RANKING_SCORE: weighted_score,
                            }
                        );
                        if params.has_remote && !params.is_proxy {
                            _federation.as_object_mut().unwrap().insert(
                                FEDERATION_REMOTE.to_string(),
                                params.network.local.clone().into(),
                            );
                        }
                        if params.is_proxy {
                            _federation.as_object_mut().unwrap().insert(
                                WEIGHTED_SCORE_VALUES.to_string(),
                                serde_json::json!(ScoreDetails::weighted_score_values(
                                    score.iter(),
                                    *weight
                                )
                                .collect_vec()),
                            );
                        }
                        hit.document.insert(FEDERATION_HIT.to_string(), _federation);
                        Ok(SearchHitByIndex { hit, score, weight, query_index })
                    },
                )
                .collect();
        let merged_result = merged_result?;
        let estimated_total_hits = candidates.len() as usize;
        let facets = facets_by_index
            .map(|facets_by_index| {
                compute_facet_distribution_stats(
                    &facets_by_index,
                    &index,
                    &rtxn,
                    candidates,
                    super::super::Route::MultiSearch,
                )
            })
            .transpose()
            .map_err(|mut error| {
                error.message = format!(
                    "Inside `.federation.facetsByIndex.{index_uid}`: {}{}",
                    error.message,
                    if let Some(query_index) = first_query_index {
                        format!("\n - Note: index `{index_uid}` used in `.queries[{query_index}]`")
                    } else {
                        Default::default()
                    }
                );
                error
            })?;
        self.results_by_index.push(SearchResultByIndex {
            index: index_uid,
            hits: merged_result,
            estimated_total_hits,
            degraded,
            used_negative_operator,
            facets,
        });
        Ok(())
    }

    fn check_unused_facets(
        &mut self,
        index_scheduler: &IndexScheduler,
    ) -> Result<(), ResponseError> {
        for (index_uid, facets) in std::mem::take(&mut self.federation.facets_by_index) {
            let index = match index_scheduler.index(&index_uid) {
                Ok(index) => index,
                Err(err) => {
                    let mut err = ResponseError::from(err);
                    // Patch the HTTP status code to 400 as it defaults to 404 for `index_not_found`, but
                    // here the resource not found is not part of the URL.
                    err.code = StatusCode::BAD_REQUEST;
                    err.message = format!(
                "Inside `.federation.facetsByIndex.{index_uid}`: {}\n - Note: index `{index_uid}` is not used in queries",
                err.message
            );
                    return Err(err);
                }
            };

            // Important: this is the only transaction we'll use for this index during this federated search
            let rtxn = index.read_txn()?;

            if let Err(mut error) =
                self.facet_order.check_facet_order(&index_uid, &facets, &index, &rtxn)
            {
                error.message = format!(
            "Inside `.federation.facetsByIndex.{index_uid}`: {error}\n - Note: index `{index_uid}` is not used in queries",
        );
                return Err(error);
            }

            if let Some(facets) = facets {
                if let Err(mut error) = compute_facet_distribution_stats(
                    &facets,
                    &index,
                    &rtxn,
                    Default::default(),
                    super::super::Route::MultiSearch,
                ) {
                    error.message =
                format!("Inside `.federation.facetsByIndex.{index_uid}`: {}\n - Note: index `{index_uid}` is not used in queries", error.message);
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}

enum FacetOrder {
    /// The order is stored by facet to be able to merge facets regardless of index of origin
    ///
    /// - key: facet name
    /// - value: (first_index_name, first_index_order)
    ///
    /// We store the name of the first index where the facet is present as well as its order,
    /// so that if encountering the same facet in a different index we can compare the order and send
    /// a readable error.
    ByFacet(BTreeMap<String, (String, OrderBy)>),
    /// The order is stored by index to be able to merge facets regardless of the remote of origin.
    ///
    /// This variant is only used when `is_remote = true`, and always used in that case.
    ///
    /// - key: index name
    /// - value: (order_by_map, max_values_per_facet)
    ///
    /// We store a map of the order per facet for that index, as well as the max values per facet.
    /// Both are retrieved from the settings of the local version of the index.
    ///
    /// It is not possible to have an index only existing in the remotes, because as of now all indexes that appear
    /// in `federation.facetsByIndex` must exist on all hosts.
    ByIndex(BTreeMap<String, (OrderByMap, usize)>),
    /// Do not merge facets. Used when `federation.mergeFacets = null` and `!has_remote`
    None,
}

type FacetDistributions = BTreeMap<String, indexmap::IndexMap<String, u64>>;
type FacetStats = BTreeMap<String, crate::search::FacetStats>;

impl FacetOrder {
    fn check_facet_order(
        &mut self,
        current_index: &str,
        facets_by_index: &Option<Vec<String>>,
        index: &milli::Index,
        rtxn: &milli::heed::RoTxn<'_>,
    ) -> Result<(), ResponseError> {
        match self {
            FacetOrder::ByFacet(facet_order) => {
                if let Some(facets_by_index) = facets_by_index {
                    let index_facet_order = index.sort_facet_values_by(rtxn)?;
                    for facet in facets_by_index {
                        let index_facet_order = index_facet_order.get(facet);
                        let (previous_index, previous_facet_order) = facet_order
                            .entry(facet.to_owned())
                            .or_insert_with(|| (current_index.to_owned(), index_facet_order));
                        if previous_facet_order != &index_facet_order {
                            return Err(MeilisearchHttpError::InconsistentFacetOrder {
                                facet: facet.clone(),
                                previous_facet_order: *previous_facet_order,
                                previous_uid: previous_index.clone(),
                                current_uid: current_index.to_owned(),
                                index_facet_order,
                            }
                            .into());
                        }
                    }
                }
            }
            FacetOrder::ByIndex(order_by_index) => {
                let max_values_per_facet = index
                    .max_values_per_facet(rtxn)?
                    .map(|x| x as usize)
                    .unwrap_or(DEFAULT_VALUES_PER_FACET);
                order_by_index.insert(
                    current_index.to_owned(),
                    (index.sort_facet_values_by(rtxn)?, max_values_per_facet),
                );
            }
            FacetOrder::None => {}
        }
        Ok(())
    }

    fn merge(
        self,
        merge_facets: Option<MergeFacets>,
        remote_results: Vec<FederatedSearchResult>,
        mut facets: FederatedFacets,
    ) -> (Option<FacetDistributions>, Option<FacetStats>, FederatedFacets) {
        let (facet_distribution, facet_stats, facets_by_index) = match (self, merge_facets) {
            (FacetOrder::ByFacet(facet_order), Some(merge_facets)) => {
                for remote_facets_by_index in
                    remote_results.into_iter().map(|result| result.facets_by_index)
                {
                    facets.append(remote_facets_by_index);
                }
                let facets = facets.merge(merge_facets, facet_order);

                let (facet_distribution, facet_stats) = facets
                    .map(|ComputedFacets { distribution, stats }| (distribution, stats))
                    .unzip();

                (facet_distribution, facet_stats, FederatedFacets::default())
            }
            (FacetOrder::ByIndex(facet_order), _) => {
                for remote_facets_by_index in
                    remote_results.into_iter().map(|result| result.facets_by_index)
                {
                    facets.append(remote_facets_by_index);
                }
                facets.sort_and_truncate(facet_order);
                (None, None, facets)
            }
            _ => (None, None, facets),
        };
        (facet_distribution, facet_stats, facets_by_index)
    }
}

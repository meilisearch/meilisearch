use std::collections::HashSet;

use serde_json::json;

use crate::analytics::Aggregate;
use crate::search::{FederatedSearch, SearchQueryWithIndex};

#[derive(Default)]
pub struct MultiSearchAggregator {
    // requests
    total_received: usize,
    total_succeeded: usize,

    // sum of the number of distinct indexes in each single request, use with total_received to compute an avg
    total_distinct_index_count: usize,
    // sum of the number of distinct remotes in each single request, use with total_received to compute an avg
    total_distinct_remote_count: usize,
    // number of queries with a single index, use with total_received to compute a proportion
    total_single_index: usize,

    // sum of the number of search queries in the requests, use with total_received to compute an average
    total_search_count: usize,

    // scoring
    show_ranking_score: bool,
    show_ranking_score_details: bool,

    // federation
    use_federation: bool,
}

impl MultiSearchAggregator {
    pub fn from_federated_search(federated_search: &FederatedSearch) -> Self {
        let use_federation = federated_search.federation.is_some();

        let mut distinct_indexes = HashSet::with_capacity(federated_search.queries.len());
        let mut distinct_remotes = HashSet::with_capacity(federated_search.queries.len());

        // make sure we get a compilation error if a field gets added to / removed from SearchQueryWithIndex
        for SearchQueryWithIndex {
            index_uid,
            federation_options,
            q: _,
            vector: _,
            offset: _,
            limit: _,
            page: _,
            hits_per_page: _,
            attributes_to_retrieve: _,
            retrieve_vectors: _,
            attributes_to_crop: _,
            crop_length: _,
            attributes_to_highlight: _,
            show_ranking_score: _,
            show_ranking_score_details: _,
            show_matches_position: _,
            filter: _,
            sort: _,
            distinct: _,
            facets: _,
            highlight_pre_tag: _,
            highlight_post_tag: _,
            crop_marker: _,
            matching_strategy: _,
            attributes_to_search_on: _,
            hybrid: _,
            ranking_score_threshold: _,
            locales: _,
        } in &federated_search.queries
        {
            if let Some(federation_options) = federation_options {
                if let Some(remote) = &federation_options.remote {
                    distinct_remotes.insert(remote.as_str());
                }
            }

            distinct_indexes.insert(index_uid.as_str());
        }

        let show_ranking_score =
            federated_search.queries.iter().any(|query| query.show_ranking_score);
        let show_ranking_score_details =
            federated_search.queries.iter().any(|query| query.show_ranking_score_details);

        Self {
            total_received: 1,
            total_succeeded: 0,
            total_distinct_index_count: distinct_indexes.len(),
            total_distinct_remote_count: distinct_remotes.len(),
            total_single_index: if distinct_indexes.len() == 1 { 1 } else { 0 },
            total_search_count: federated_search.queries.len(),
            show_ranking_score,
            show_ranking_score_details,
            use_federation,
        }
    }

    pub fn succeed(&mut self) {
        self.total_succeeded = self.total_succeeded.saturating_add(1);
    }
}

impl Aggregate for MultiSearchAggregator {
    fn event_name(&self) -> &'static str {
        "Documents Searched by Multi-Search POST"
    }

    /// Aggregate one [MultiSearchAggregator] into another.
    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        // write the aggregate in a way that will cause a compilation error if a field is added.

        // get ownership of self, replacing it by a default value.
        let this = *self;

        let total_received = this.total_received.saturating_add(new.total_received);
        let total_succeeded = this.total_succeeded.saturating_add(new.total_succeeded);
        let total_distinct_index_count =
            this.total_distinct_index_count.saturating_add(new.total_distinct_index_count);
        let total_distinct_remote_count =
            this.total_distinct_remote_count.saturating_add(new.total_distinct_remote_count);
        let total_single_index = this.total_single_index.saturating_add(new.total_single_index);
        let total_search_count = this.total_search_count.saturating_add(new.total_search_count);
        let show_ranking_score = this.show_ranking_score || new.show_ranking_score;
        let show_ranking_score_details =
            this.show_ranking_score_details || new.show_ranking_score_details;
        let use_federation = this.use_federation || new.use_federation;

        Box::new(Self {
            total_received,
            total_succeeded,
            total_distinct_index_count,
            total_distinct_remote_count,
            total_single_index,
            total_search_count,
            show_ranking_score,
            show_ranking_score_details,
            use_federation,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        let Self {
            total_received,
            total_succeeded,
            total_distinct_index_count,
            total_distinct_remote_count,
            total_single_index,
            total_search_count,
            show_ranking_score,
            show_ranking_score_details,
            use_federation,
        } = *self;

        json!({
            "requests": {
                "total_succeeded": total_succeeded,
                "total_failed": total_received.saturating_sub(total_succeeded), // just to be sure we never panics
                "total_received": total_received,
            },
            "indexes": {
                "total_single_index": total_single_index,
                "total_distinct_index_count": total_distinct_index_count,
                "avg_distinct_index_count": (total_distinct_index_count as f64) / (total_received as f64), // not 0 else returned early
            },
            "remotes": {
                "total_distinct_remote_count": total_distinct_remote_count,
                "avg_distinct_remote_count": (total_distinct_remote_count as f64) / (total_received as f64), // not 0 else returned early
            },
            "searches": {
                "total_search_count": total_search_count,
                "avg_search_count": (total_search_count as f64) / (total_received as f64),
            },
            "scoring": {
                "show_ranking_score": show_ranking_score,
                "show_ranking_score_details": show_ranking_score_details,
            },
            "federation": {
                "use_federation": use_federation,
            }
        })
    }
}

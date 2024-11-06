use std::collections::{BTreeSet, BinaryHeap, HashMap};

use meilisearch_types::locales::Locale;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{json, Value};

use crate::aggregate_methods;
use crate::analytics::{Aggregate, AggregateMethod};
use crate::search::{
    SearchQuery, SearchResult, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER,
    DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT,
    DEFAULT_SEMANTIC_RATIO,
};

aggregate_methods!(
    SearchGET => "Documents Searched GET",
    SearchPOST => "Documents Searched POST",
);

#[derive(Default)]
pub struct SearchAggregator<Method: AggregateMethod> {
    // requests
    total_received: usize,
    total_succeeded: usize,
    total_degraded: usize,
    total_used_negative_operator: usize,
    time_spent: BinaryHeap<usize>,

    // sort
    sort_with_geo_point: bool,
    // every time a request has a filter, this field must be incremented by the number of terms it contains
    sort_sum_of_criteria_terms: usize,
    // every time a request has a filter, this field must be incremented by one
    sort_total_number_of_criteria: usize,

    // distinct
    distinct: bool,

    // filter
    filter_with_geo_radius: bool,
    filter_with_geo_bounding_box: bool,
    // every time a request has a filter, this field must be incremented by the number of terms it contains
    filter_sum_of_criteria_terms: usize,
    // every time a request has a filter, this field must be incremented by one
    filter_total_number_of_criteria: usize,
    used_syntax: HashMap<String, usize>,

    // attributes_to_search_on
    // every time a search is done using attributes_to_search_on
    attributes_to_search_on_total_number_of_uses: usize,

    // q
    // The maximum number of terms in a q request
    max_terms_number: usize,

    // vector
    // The maximum number of floats in a vector request
    max_vector_size: usize,
    // Whether the semantic ratio passed to a hybrid search equals the default ratio.
    semantic_ratio: bool,
    hybrid: bool,
    retrieve_vectors: bool,

    // every time a search is done, we increment the counter linked to the used settings
    matching_strategy: HashMap<String, usize>,

    // List of the unique Locales passed as parameter
    locales: BTreeSet<Locale>,

    // pagination
    max_limit: usize,
    max_offset: usize,
    finite_pagination: usize,

    // formatting
    max_attributes_to_retrieve: usize,
    max_attributes_to_highlight: usize,
    highlight_pre_tag: bool,
    highlight_post_tag: bool,
    max_attributes_to_crop: usize,
    crop_marker: bool,
    show_matches_position: bool,
    crop_length: bool,

    // facets
    facets_sum_of_terms: usize,
    facets_total_number_of_facets: usize,

    // scoring
    show_ranking_score: bool,
    show_ranking_score_details: bool,
    ranking_score_threshold: bool,

    marker: std::marker::PhantomData<Method>,
}

impl<Method: AggregateMethod> SearchAggregator<Method> {
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_query(query: &SearchQuery) -> Self {
        let SearchQuery {
            q,
            vector,
            offset,
            limit,
            page,
            hits_per_page,
            attributes_to_retrieve: _,
            retrieve_vectors,
            attributes_to_crop: _,
            crop_length,
            attributes_to_highlight: _,
            show_matches_position,
            show_ranking_score,
            show_ranking_score_details,
            filter,
            sort,
            distinct,
            facets: _,
            highlight_pre_tag,
            highlight_post_tag,
            crop_marker,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
        } = query;

        let mut ret = Self::default();

        ret.total_received = 1;

        if let Some(ref sort) = sort {
            ret.sort_total_number_of_criteria = 1;
            ret.sort_with_geo_point = sort.iter().any(|s| s.contains("_geoPoint("));
            ret.sort_sum_of_criteria_terms = sort.len();
        }

        ret.distinct = distinct.is_some();

        if let Some(ref filter) = filter {
            static RE: Lazy<Regex> = Lazy::new(|| Regex::new("AND | OR").unwrap());
            ret.filter_total_number_of_criteria = 1;

            let syntax = match filter {
                Value::String(_) => "string".to_string(),
                Value::Array(values) => {
                    if values.iter().map(|v| v.to_string()).any(|s| RE.is_match(&s)) {
                        "mixed".to_string()
                    } else {
                        "array".to_string()
                    }
                }
                _ => "none".to_string(),
            };
            // convert the string to a HashMap
            ret.used_syntax.insert(syntax, 1);

            let stringified_filters = filter.to_string();
            ret.filter_with_geo_radius = stringified_filters.contains("_geoRadius(");
            ret.filter_with_geo_bounding_box = stringified_filters.contains("_geoBoundingBox(");
            ret.filter_sum_of_criteria_terms = RE.split(&stringified_filters).count();
        }

        // attributes_to_search_on
        if attributes_to_search_on.is_some() {
            ret.attributes_to_search_on_total_number_of_uses = 1;
        }

        if let Some(ref q) = q {
            ret.max_terms_number = q.split_whitespace().count();
        }

        if let Some(ref vector) = vector {
            ret.max_vector_size = vector.len();
        }
        ret.retrieve_vectors |= retrieve_vectors;

        if query.is_finite_pagination() {
            let limit = hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
            ret.max_limit = limit;
            ret.max_offset = page.unwrap_or(1).saturating_sub(1) * limit;
            ret.finite_pagination = 1;
        } else {
            ret.max_limit = *limit;
            ret.max_offset = *offset;
            ret.finite_pagination = 0;
        }

        ret.matching_strategy.insert(format!("{:?}", matching_strategy), 1);

        if let Some(locales) = locales {
            ret.locales = locales.iter().copied().collect();
        }

        ret.highlight_pre_tag = *highlight_pre_tag != DEFAULT_HIGHLIGHT_PRE_TAG();
        ret.highlight_post_tag = *highlight_post_tag != DEFAULT_HIGHLIGHT_POST_TAG();
        ret.crop_marker = *crop_marker != DEFAULT_CROP_MARKER();
        ret.crop_length = *crop_length != DEFAULT_CROP_LENGTH();
        ret.show_matches_position = *show_matches_position;

        ret.show_ranking_score = *show_ranking_score;
        ret.show_ranking_score_details = *show_ranking_score_details;
        ret.ranking_score_threshold = ranking_score_threshold.is_some();

        if let Some(hybrid) = hybrid {
            ret.semantic_ratio = hybrid.semantic_ratio != DEFAULT_SEMANTIC_RATIO();
            ret.hybrid = true;
        }

        ret
    }

    pub fn succeed(&mut self, result: &SearchResult) {
        let SearchResult {
            hits: _,
            query: _,
            processing_time_ms,
            hits_info: _,
            semantic_hit_count: _,
            facet_distribution: _,
            facet_stats: _,
            degraded,
            used_negative_operator,
        } = result;

        self.total_succeeded = self.total_succeeded.saturating_add(1);
        if *degraded {
            self.total_degraded = self.total_degraded.saturating_add(1);
        }
        if *used_negative_operator {
            self.total_used_negative_operator = self.total_used_negative_operator.saturating_add(1);
        }
        self.time_spent.push(*processing_time_ms as usize);
    }
}

impl<Method: AggregateMethod> Aggregate for SearchAggregator<Method> {
    fn event_name(&self) -> &'static str {
        Method::event_name()
    }

    fn aggregate(mut self: Box<Self>, new: Box<Self>) -> Box<Self> {
        let Self {
            total_received,
            total_succeeded,
            mut time_spent,
            sort_with_geo_point,
            sort_sum_of_criteria_terms,
            sort_total_number_of_criteria,
            distinct,
            filter_with_geo_radius,
            filter_with_geo_bounding_box,
            filter_sum_of_criteria_terms,
            filter_total_number_of_criteria,
            used_syntax,
            attributes_to_search_on_total_number_of_uses,
            max_terms_number,
            max_vector_size,
            retrieve_vectors,
            matching_strategy,
            max_limit,
            max_offset,
            finite_pagination,
            max_attributes_to_retrieve,
            max_attributes_to_highlight,
            highlight_pre_tag,
            highlight_post_tag,
            max_attributes_to_crop,
            crop_marker,
            show_matches_position,
            crop_length,
            facets_sum_of_terms,
            facets_total_number_of_facets,
            show_ranking_score,
            show_ranking_score_details,
            semantic_ratio,
            hybrid,
            total_degraded,
            total_used_negative_operator,
            ranking_score_threshold,
            mut locales,
            marker: _,
        } = *new;

        // request
        self.total_received = self.total_received.saturating_add(total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(total_succeeded);
        self.total_degraded = self.total_degraded.saturating_add(total_degraded);
        self.total_used_negative_operator =
            self.total_used_negative_operator.saturating_add(total_used_negative_operator);
        self.time_spent.append(&mut time_spent);

        // sort
        self.sort_with_geo_point |= sort_with_geo_point;
        self.sort_sum_of_criteria_terms =
            self.sort_sum_of_criteria_terms.saturating_add(sort_sum_of_criteria_terms);
        self.sort_total_number_of_criteria =
            self.sort_total_number_of_criteria.saturating_add(sort_total_number_of_criteria);

        // distinct
        self.distinct |= distinct;

        // filter
        self.filter_with_geo_radius |= filter_with_geo_radius;
        self.filter_with_geo_bounding_box |= filter_with_geo_bounding_box;
        self.filter_sum_of_criteria_terms =
            self.filter_sum_of_criteria_terms.saturating_add(filter_sum_of_criteria_terms);
        self.filter_total_number_of_criteria =
            self.filter_total_number_of_criteria.saturating_add(filter_total_number_of_criteria);
        for (key, value) in used_syntax.into_iter() {
            let used_syntax = self.used_syntax.entry(key).or_insert(0);
            *used_syntax = used_syntax.saturating_add(value);
        }

        // attributes_to_search_on
        self.attributes_to_search_on_total_number_of_uses = self
            .attributes_to_search_on_total_number_of_uses
            .saturating_add(attributes_to_search_on_total_number_of_uses);

        // q
        self.max_terms_number = self.max_terms_number.max(max_terms_number);

        // vector
        self.max_vector_size = self.max_vector_size.max(max_vector_size);
        self.retrieve_vectors |= retrieve_vectors;
        self.semantic_ratio |= semantic_ratio;
        self.hybrid |= hybrid;

        // pagination
        self.max_limit = self.max_limit.max(max_limit);
        self.max_offset = self.max_offset.max(max_offset);
        self.finite_pagination += finite_pagination;

        // formatting
        self.max_attributes_to_retrieve =
            self.max_attributes_to_retrieve.max(max_attributes_to_retrieve);
        self.max_attributes_to_highlight =
            self.max_attributes_to_highlight.max(max_attributes_to_highlight);
        self.highlight_pre_tag |= highlight_pre_tag;
        self.highlight_post_tag |= highlight_post_tag;
        self.max_attributes_to_crop = self.max_attributes_to_crop.max(max_attributes_to_crop);
        self.crop_marker |= crop_marker;
        self.show_matches_position |= show_matches_position;
        self.crop_length |= crop_length;

        // facets
        self.facets_sum_of_terms = self.facets_sum_of_terms.saturating_add(facets_sum_of_terms);
        self.facets_total_number_of_facets =
            self.facets_total_number_of_facets.saturating_add(facets_total_number_of_facets);

        // matching strategy
        for (key, value) in matching_strategy.into_iter() {
            let matching_strategy = self.matching_strategy.entry(key).or_insert(0);
            *matching_strategy = matching_strategy.saturating_add(value);
        }

        // scoring
        self.show_ranking_score |= show_ranking_score;
        self.show_ranking_score_details |= show_ranking_score_details;
        self.ranking_score_threshold |= ranking_score_threshold;

        // locales
        self.locales.append(&mut locales);

        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        let Self {
            total_received,
            total_succeeded,
            time_spent,
            sort_with_geo_point,
            sort_sum_of_criteria_terms,
            sort_total_number_of_criteria,
            distinct,
            filter_with_geo_radius,
            filter_with_geo_bounding_box,
            filter_sum_of_criteria_terms,
            filter_total_number_of_criteria,
            used_syntax,
            attributes_to_search_on_total_number_of_uses,
            max_terms_number,
            max_vector_size,
            retrieve_vectors,
            matching_strategy,
            max_limit,
            max_offset,
            finite_pagination,
            max_attributes_to_retrieve,
            max_attributes_to_highlight,
            highlight_pre_tag,
            highlight_post_tag,
            max_attributes_to_crop,
            crop_marker,
            show_matches_position,
            crop_length,
            facets_sum_of_terms,
            facets_total_number_of_facets,
            show_ranking_score,
            show_ranking_score_details,
            semantic_ratio,
            hybrid,
            total_degraded,
            total_used_negative_operator,
            ranking_score_threshold,
            locales,
            marker: _,
        } = *self;

        // we get all the values in a sorted manner
        let time_spent = time_spent.into_sorted_vec();
        // the index of the 99th percentage of value
        let percentile_99th = time_spent.len() * 99 / 100;
        // We are only interested by the slowest value of the 99th fastest results
        let time_spent = time_spent.get(percentile_99th);

        json!({
            "requests": {
                "99th_response_time": time_spent.map(|t| format!("{:.2}", t)),
                "total_succeeded": total_succeeded,
                "total_failed": total_received.saturating_sub(total_succeeded), // just to be sure we never panics
                "total_received": total_received,
                "total_degraded": total_degraded,
                "total_used_negative_operator": total_used_negative_operator,
            },
            "sort": {
                "with_geoPoint": sort_with_geo_point,
                "avg_criteria_number": format!("{:.2}", sort_sum_of_criteria_terms as f64 / sort_total_number_of_criteria as f64),
            },
            "distinct": distinct,
            "filter": {
               "with_geoRadius": filter_with_geo_radius,
               "with_geoBoundingBox": filter_with_geo_bounding_box,
               "avg_criteria_number": format!("{:.2}", filter_sum_of_criteria_terms as f64 / filter_total_number_of_criteria as f64),
               "most_used_syntax": used_syntax.iter().max_by_key(|(_, v)| *v).map(|(k, _)| json!(k)).unwrap_or_else(|| json!(null)),
            },
            "attributes_to_search_on": {
               "total_number_of_uses": attributes_to_search_on_total_number_of_uses,
            },
            "q": {
               "max_terms_number": max_terms_number,
            },
            "vector": {
                "max_vector_size": max_vector_size,
                "retrieve_vectors": retrieve_vectors,
            },
            "hybrid": {
                "enabled": hybrid,
                "semantic_ratio": semantic_ratio,
            },
            "pagination": {
               "max_limit": max_limit,
               "max_offset": max_offset,
               "most_used_navigation": if finite_pagination > (total_received / 2) { "exhaustive" } else { "estimated" },
            },
            "formatting": {
                "max_attributes_to_retrieve": max_attributes_to_retrieve,
                "max_attributes_to_highlight": max_attributes_to_highlight,
                "highlight_pre_tag": highlight_pre_tag,
                "highlight_post_tag": highlight_post_tag,
                "max_attributes_to_crop": max_attributes_to_crop,
                "crop_marker": crop_marker,
                "show_matches_position": show_matches_position,
                "crop_length": crop_length,
            },
            "facets": {
                "avg_facets_number": format!("{:.2}", facets_sum_of_terms as f64 / facets_total_number_of_facets as f64),
            },
            "matching_strategy": {
                "most_used_strategy": matching_strategy.iter().max_by_key(|(_, v)| *v).map(|(k, _)| json!(k)).unwrap_or_else(|| json!(null)),
            },
            "locales": locales,
            "scoring": {
                "show_ranking_score": show_ranking_score,
                "show_ranking_score_details": show_ranking_score_details,
                "ranking_score_threshold": ranking_score_threshold,
            },
        })
    }
}

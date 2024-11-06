use std::collections::{BinaryHeap, HashMap};

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{json, Value};

use crate::aggregate_methods;
use crate::analytics::{Aggregate, AggregateMethod};
use crate::search::{SimilarQuery, SimilarResult};

aggregate_methods!(
    SimilarPOST => "Similar POST",
    SimilarGET => "Similar GET",
);

#[derive(Default)]
pub struct SimilarAggregator<Method: AggregateMethod> {
    // requests
    total_received: usize,
    total_succeeded: usize,
    time_spent: BinaryHeap<usize>,

    // filter
    filter_with_geo_radius: bool,
    filter_with_geo_bounding_box: bool,
    // every time a request has a filter, this field must be incremented by the number of terms it contains
    filter_sum_of_criteria_terms: usize,
    // every time a request has a filter, this field must be incremented by one
    filter_total_number_of_criteria: usize,
    used_syntax: HashMap<String, usize>,

    // Whether a non-default embedder was specified
    retrieve_vectors: bool,

    // pagination
    max_limit: usize,
    max_offset: usize,

    // formatting
    max_attributes_to_retrieve: usize,

    // scoring
    show_ranking_score: bool,
    show_ranking_score_details: bool,
    ranking_score_threshold: bool,

    marker: std::marker::PhantomData<Method>,
}

impl<Method: AggregateMethod> SimilarAggregator<Method> {
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_query(query: &SimilarQuery) -> Self {
        let SimilarQuery {
            id: _,
            embedder: _,
            offset,
            limit,
            attributes_to_retrieve: _,
            retrieve_vectors,
            show_ranking_score,
            show_ranking_score_details,
            filter,
            ranking_score_threshold,
        } = query;

        let mut ret = Self::default();

        ret.total_received = 1;

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

        ret.max_limit = *limit;
        ret.max_offset = *offset;

        ret.show_ranking_score = *show_ranking_score;
        ret.show_ranking_score_details = *show_ranking_score_details;
        ret.ranking_score_threshold = ranking_score_threshold.is_some();

        ret.retrieve_vectors = *retrieve_vectors;

        ret
    }

    pub fn succeed(&mut self, result: &SimilarResult) {
        let SimilarResult { id: _, hits: _, processing_time_ms, hits_info: _ } = result;

        self.total_succeeded = self.total_succeeded.saturating_add(1);

        self.time_spent.push(*processing_time_ms as usize);
    }
}

impl<Method: AggregateMethod> Aggregate for SimilarAggregator<Method> {
    fn event_name(&self) -> &'static str {
        Method::event_name()
    }

    /// Aggregate one [SimilarAggregator] into another.
    fn aggregate(mut self: Box<Self>, new: Box<Self>) -> Box<Self> {
        let Self {
            total_received,
            total_succeeded,
            mut time_spent,
            filter_with_geo_radius,
            filter_with_geo_bounding_box,
            filter_sum_of_criteria_terms,
            filter_total_number_of_criteria,
            used_syntax,
            max_limit,
            max_offset,
            max_attributes_to_retrieve,
            show_ranking_score,
            show_ranking_score_details,
            ranking_score_threshold,
            retrieve_vectors,
            marker: _,
        } = *new;

        // request
        self.total_received = self.total_received.saturating_add(total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(total_succeeded);
        self.time_spent.append(&mut time_spent);

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

        self.retrieve_vectors |= retrieve_vectors;

        // pagination
        self.max_limit = self.max_limit.max(max_limit);
        self.max_offset = self.max_offset.max(max_offset);

        // formatting
        self.max_attributes_to_retrieve =
            self.max_attributes_to_retrieve.max(max_attributes_to_retrieve);

        // scoring
        self.show_ranking_score |= show_ranking_score;
        self.show_ranking_score_details |= show_ranking_score_details;
        self.ranking_score_threshold |= ranking_score_threshold;

        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        let Self {
            total_received,
            total_succeeded,
            time_spent,
            filter_with_geo_radius,
            filter_with_geo_bounding_box,
            filter_sum_of_criteria_terms,
            filter_total_number_of_criteria,
            used_syntax,
            max_limit,
            max_offset,
            max_attributes_to_retrieve,
            show_ranking_score,
            show_ranking_score_details,
            ranking_score_threshold,
            retrieve_vectors,
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
            },
            "filter": {
               "with_geoRadius": filter_with_geo_radius,
               "with_geoBoundingBox": filter_with_geo_bounding_box,
               "avg_criteria_number": format!("{:.2}", filter_sum_of_criteria_terms as f64 / filter_total_number_of_criteria as f64),
               "most_used_syntax": used_syntax.iter().max_by_key(|(_, v)| *v).map(|(k, _)| json!(k)).unwrap_or_else(|| json!(null)),
            },
            "vector": {
                "retrieve_vectors": retrieve_vectors,
            },
            "pagination": {
               "max_limit": max_limit,
               "max_offset": max_offset,
            },
            "formatting": {
                "max_attributes_to_retrieve": max_attributes_to_retrieve,
            },
            "scoring": {
                "show_ranking_score": show_ranking_score,
                "show_ranking_score_details": show_ranking_score_details,
                "ranking_score_threshold": ranking_score_threshold,
            }
        })
    }
}

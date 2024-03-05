use std::cmp::Ordering;

use itertools::Itertools;
use roaring::RoaringBitmap;

use crate::score_details::{ScoreDetails, ScoreValue, ScoringStrategy};
use crate::{MatchingWords, Result, Search, SearchResult};

struct ScoreWithRatioResult {
    matching_words: MatchingWords,
    candidates: RoaringBitmap,
    document_scores: Vec<(u32, ScoreWithRatio)>,
}

type ScoreWithRatio = (Vec<ScoreDetails>, f32);

fn compare_scores(
    &(ref left_scores, left_ratio): &ScoreWithRatio,
    &(ref right_scores, right_ratio): &ScoreWithRatio,
) -> Ordering {
    let mut left_it = ScoreDetails::score_values(left_scores.iter());
    let mut right_it = ScoreDetails::score_values(right_scores.iter());

    loop {
        let left = left_it.next();
        let right = right_it.next();

        match (left, right) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(ScoreValue::Score(left)), Some(ScoreValue::Score(right))) => {
                let left = left * left_ratio as f64;
                let right = right * right_ratio as f64;
                if (left - right).abs() <= f64::EPSILON {
                    continue;
                }
                return left.partial_cmp(&right).unwrap();
            }
            (Some(ScoreValue::Sort(left)), Some(ScoreValue::Sort(right))) => {
                match left.partial_cmp(right).unwrap() {
                    Ordering::Equal => continue,
                    order => return order,
                }
            }
            (Some(ScoreValue::GeoSort(left)), Some(ScoreValue::GeoSort(right))) => {
                match left.partial_cmp(right).unwrap() {
                    Ordering::Equal => continue,
                    order => return order,
                }
            }
            (Some(ScoreValue::Score(_)), Some(_)) => return Ordering::Greater,
            (Some(_), Some(ScoreValue::Score(_))) => return Ordering::Less,
            // if we have this, we're bad
            (Some(ScoreValue::GeoSort(_)), Some(ScoreValue::Sort(_)))
            | (Some(ScoreValue::Sort(_)), Some(ScoreValue::GeoSort(_))) => {
                unreachable!("Unexpected geo and sort comparison")
            }
        }
    }
}

impl ScoreWithRatioResult {
    fn new(results: SearchResult, ratio: f32) -> Self {
        let document_scores = results
            .documents_ids
            .into_iter()
            .zip(results.document_scores.into_iter().map(|scores| (scores, ratio)))
            .collect();

        Self {
            matching_words: results.matching_words,
            candidates: results.candidates,
            document_scores,
        }
    }

    fn merge(left: Self, right: Self, from: usize, length: usize) -> SearchResult {
        let mut documents_ids =
            Vec::with_capacity(left.document_scores.len() + right.document_scores.len());
        let mut document_scores =
            Vec::with_capacity(left.document_scores.len() + right.document_scores.len());

        let mut documents_seen = RoaringBitmap::new();
        for (docid, (main_score, _sub_score)) in left
            .document_scores
            .into_iter()
            .merge_by(right.document_scores.into_iter(), |(_, left), (_, right)| {
                // the first value is the one with the greatest score
                compare_scores(left, right).is_ge()
            })
            // remove documents we already saw
            .filter(|(docid, _)| documents_seen.insert(*docid))
            // start skipping **after** the filter
            .skip(from)
            // take **after** skipping
            .take(length)
        {
            documents_ids.push(docid);
            // TODO: pass both scores to documents_score in some way?
            document_scores.push(main_score);
        }

        SearchResult {
            matching_words: right.matching_words,
            candidates: left.candidates | right.candidates,
            documents_ids,
            document_scores,
            degraded: false,
        }
    }
}

impl<'a> Search<'a> {
    pub fn execute_hybrid(&self, semantic_ratio: f32) -> Result<SearchResult> {
        // TODO: find classier way to achieve that than to reset vector and query params
        // create separate keyword and semantic searches
        let mut search = Search {
            query: self.query.clone(),
            vector: self.vector.clone(),
            filter: self.filter.clone(),
            offset: 0,
            limit: self.limit + self.offset,
            sort_criteria: self.sort_criteria.clone(),
            searchable_attributes: self.searchable_attributes,
            geo_strategy: self.geo_strategy,
            terms_matching_strategy: self.terms_matching_strategy,
            scoring_strategy: ScoringStrategy::Detailed,
            words_limit: self.words_limit,
            exhaustive_number_hits: self.exhaustive_number_hits,
            rtxn: self.rtxn,
            index: self.index,
            distribution_shift: self.distribution_shift,
            embedder_name: self.embedder_name.clone(),
            time_budget: self.time_budget,
        };

        let vector_query = search.vector.take();
        let keyword_results = search.execute()?;

        // skip semantic search if we don't have a vector query (placeholder search)
        let Some(vector_query) = vector_query else {
            return Ok(keyword_results);
        };

        // completely skip semantic search if the results of the keyword search are good enough
        if self.results_good_enough(&keyword_results, semantic_ratio) {
            return Ok(keyword_results);
        }

        search.vector = Some(vector_query);
        search.query = None;

        // TODO: would be better to have two distinct functions at this point
        let vector_results = search.execute()?;

        let keyword_results = ScoreWithRatioResult::new(keyword_results, 1.0 - semantic_ratio);
        let vector_results = ScoreWithRatioResult::new(vector_results, semantic_ratio);

        let merge_results =
            ScoreWithRatioResult::merge(vector_results, keyword_results, self.offset, self.limit);
        assert!(merge_results.documents_ids.len() <= self.limit);
        Ok(merge_results)
    }

    fn results_good_enough(&self, keyword_results: &SearchResult, semantic_ratio: f32) -> bool {
        // A result is good enough if its keyword score is > 0.9 with a semantic ratio of 0.5 => 0.9 * 0.5
        const GOOD_ENOUGH_SCORE: f64 = 0.45;

        // 1. we check that we got a sufficient number of results
        if keyword_results.document_scores.len() < self.limit + self.offset {
            return false;
        }

        // 2. and that all results have a good enough score.
        // we need to check all results because due to sort like rules, they're not necessarily in relevancy order
        for score in &keyword_results.document_scores {
            let score = ScoreDetails::global_score(score.iter());
            if score * ((1.0 - semantic_ratio) as f64) < GOOD_ENOUGH_SCORE {
                return false;
            }
        }
        true
    }
}

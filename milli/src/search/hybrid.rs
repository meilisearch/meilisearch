use std::cmp::Ordering;

use itertools::Itertools;
use roaring::RoaringBitmap;

use crate::score_details::{ScoreDetails, ScoreValue, ScoringStrategy};
use crate::search::SemanticSearch;
use crate::{MatchingWords, Result, Search, SearchResult};

struct ScoreWithRatioResult {
    matching_words: MatchingWords,
    candidates: RoaringBitmap,
    document_scores: Vec<(u32, ScoreWithRatio)>,
    degraded: bool,
    used_negative_operator: bool,
}

type ScoreWithRatio = (Vec<ScoreDetails>, f32);

#[tracing::instrument(level = "trace", skip_all, target = "search::hybrid")]
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
            (Some(ScoreValue::Score(x)), Some(_)) => {
                return if x == 0. { Ordering::Less } else { Ordering::Greater }
            }
            (Some(_), Some(ScoreValue::Score(x))) => {
                return if x == 0. { Ordering::Greater } else { Ordering::Less }
            }
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
            degraded: results.degraded,
            used_negative_operator: results.used_negative_operator,
        }
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::hybrid")]
    fn merge(
        vector_results: Self,
        keyword_results: Self,
        from: usize,
        length: usize,
    ) -> (SearchResult, u32) {
        #[derive(Clone, Copy)]
        enum ResultSource {
            Semantic,
            Keyword,
        }
        let mut semantic_hit_count = 0;

        let mut documents_ids = Vec::with_capacity(
            vector_results.document_scores.len() + keyword_results.document_scores.len(),
        );
        let mut document_scores = Vec::with_capacity(
            vector_results.document_scores.len() + keyword_results.document_scores.len(),
        );

        let mut documents_seen = RoaringBitmap::new();
        for ((docid, (main_score, _sub_score)), source) in vector_results
            .document_scores
            .into_iter()
            .zip(std::iter::repeat(ResultSource::Semantic))
            .merge_by(
                keyword_results
                    .document_scores
                    .into_iter()
                    .zip(std::iter::repeat(ResultSource::Keyword)),
                |((_, left), _), ((_, right), _)| {
                    // the first value is the one with the greatest score
                    compare_scores(left, right).is_ge()
                },
            )
            // remove documents we already saw
            .filter(|((docid, _), _)| documents_seen.insert(*docid))
            // start skipping **after** the filter
            .skip(from)
            // take **after** skipping
            .take(length)
        {
            if let ResultSource::Semantic = source {
                semantic_hit_count += 1;
            }
            documents_ids.push(docid);
            // TODO: pass both scores to documents_score in some way?
            document_scores.push(main_score);
        }

        (
            SearchResult {
                matching_words: keyword_results.matching_words,
                candidates: vector_results.candidates | keyword_results.candidates,
                documents_ids,
                document_scores,
                degraded: vector_results.degraded | keyword_results.degraded,
                used_negative_operator: vector_results.used_negative_operator
                    | keyword_results.used_negative_operator,
            },
            semantic_hit_count,
        )
    }
}

impl<'a> Search<'a> {
    #[tracing::instrument(level = "trace", skip_all, target = "search::hybrid")]
    pub fn execute_hybrid(&self, semantic_ratio: f32) -> Result<(SearchResult, Option<u32>)> {
        // TODO: find classier way to achieve that than to reset vector and query params
        // create separate keyword and semantic searches
        let mut search = Search {
            query: self.query.clone(),
            filter: self.filter.clone(),
            offset: 0,
            limit: self.limit + self.offset,
            sort_criteria: self.sort_criteria.clone(),
            distinct: self.distinct.clone(),
            searchable_attributes: self.searchable_attributes,
            geo_strategy: self.geo_strategy,
            terms_matching_strategy: self.terms_matching_strategy,
            scoring_strategy: ScoringStrategy::Detailed,
            words_limit: self.words_limit,
            exhaustive_number_hits: self.exhaustive_number_hits,
            rtxn: self.rtxn,
            index: self.index,
            semantic: self.semantic.clone(),
            time_budget: self.time_budget.clone(),
            ranking_score_threshold: self.ranking_score_threshold,
            locales: self.locales.clone(),
        };

        let semantic = search.semantic.take();
        let keyword_results = search.execute()?;

        // completely skip semantic search if the results of the keyword search are good enough
        if self.results_good_enough(&keyword_results, semantic_ratio) {
            return Ok(return_keyword_results(self.limit, self.offset, keyword_results));
        }

        // no vector search against placeholder search
        let Some(query) = search.query.take() else {
            return Ok(return_keyword_results(self.limit, self.offset, keyword_results));
        };
        // no embedder, no semantic search
        let Some(SemanticSearch { vector, embedder_name, embedder }) = semantic else {
            return Ok(return_keyword_results(self.limit, self.offset, keyword_results));
        };

        let vector_query = match vector {
            Some(vector_query) => vector_query,
            None => {
                // attempt to embed the vector
                let span = tracing::trace_span!(target: "search::hybrid", "embed_one");
                let _entered = span.enter();

                match embedder.embed_one(query) {
                    Ok(embedding) => embedding,
                    Err(error) => {
                        tracing::error!(error=%error, "Embedding failed");
                        return Ok((keyword_results, Some(0)));
                    }
                }
            }
        };

        search.semantic =
            Some(SemanticSearch { vector: Some(vector_query), embedder_name, embedder });

        // TODO: would be better to have two distinct functions at this point
        let vector_results = search.execute()?;

        let keyword_results = ScoreWithRatioResult::new(keyword_results, 1.0 - semantic_ratio);
        let vector_results = ScoreWithRatioResult::new(vector_results, semantic_ratio);

        let (merge_results, semantic_hit_count) =
            ScoreWithRatioResult::merge(vector_results, keyword_results, self.offset, self.limit);
        assert!(merge_results.documents_ids.len() <= self.limit);
        Ok((merge_results, Some(semantic_hit_count)))
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

fn return_keyword_results(
    limit: usize,
    offset: usize,
    SearchResult {
        matching_words,
        candidates,
        mut documents_ids,
        mut document_scores,
        degraded,
        used_negative_operator,
    }: SearchResult,
) -> (SearchResult, Option<u32>) {
    let (documents_ids, document_scores) = if offset >= documents_ids.len() ||
    // technically redudant because documents_ids.len() == document_scores.len(),
    // defensive programming
    offset >= document_scores.len()
    {
        (vec![], vec![])
    } else {
        // PANICS: offset < len
        documents_ids.rotate_left(offset);
        documents_ids.truncate(limit);

        // PANICS: offset < len
        document_scores.rotate_left(offset);
        document_scores.truncate(limit);
        (documents_ids, document_scores)
    };
    (
        SearchResult {
            matching_words,
            candidates,
            documents_ids,
            document_scores,
            degraded,
            used_negative_operator,
        },
        Some(0),
    )
}

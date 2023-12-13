use std::cmp::Ordering;
use std::collections::HashMap;

use itertools::Itertools;
use roaring::RoaringBitmap;

use super::new::{execute_vector_search, PartialSearchResult};
use crate::score_details::{ScoreDetails, ScoreValue, ScoringStrategy};
use crate::{
    execute_search, DefaultSearchLogger, MatchingWords, Result, Search, SearchContext, SearchResult,
};

struct CombinedSearchResult {
    matching_words: MatchingWords,
    candidates: RoaringBitmap,
    document_scores: Vec<(u32, CombinedScore)>,
}

type CombinedScore = (Vec<ScoreDetails>, Option<Vec<ScoreDetails>>);

fn compare_scores(left: &CombinedScore, right: &CombinedScore) -> Ordering {
    let mut left_main_it = ScoreDetails::score_values(left.0.iter());
    let mut left_sub_it =
        ScoreDetails::score_values(left.1.as_ref().map(|x| x.iter()).into_iter().flatten());

    let mut right_main_it = ScoreDetails::score_values(right.0.iter());
    let mut right_sub_it =
        ScoreDetails::score_values(right.1.as_ref().map(|x| x.iter()).into_iter().flatten());

    let mut left_main = left_main_it.next();
    let mut left_sub = left_sub_it.next();
    let mut right_main = right_main_it.next();
    let mut right_sub = right_sub_it.next();

    loop {
        let left =
            take_best_score(&mut left_main, &mut left_sub, &mut left_main_it, &mut left_sub_it);

        let right =
            take_best_score(&mut right_main, &mut right_sub, &mut right_main_it, &mut right_sub_it);

        match (left, right) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(ScoreValue::Score(left)), Some(ScoreValue::Score(right))) => {
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

fn take_best_score<'a>(
    main_score: &mut Option<ScoreValue<'a>>,
    sub_score: &mut Option<ScoreValue<'a>>,
    main_it: &mut impl Iterator<Item = ScoreValue<'a>>,
    sub_it: &mut impl Iterator<Item = ScoreValue<'a>>,
) -> Option<ScoreValue<'a>> {
    match (*main_score, *sub_score) {
        (Some(main), None) => {
            *main_score = main_it.next();
            Some(main)
        }
        (None, Some(sub)) => {
            *sub_score = sub_it.next();
            Some(sub)
        }
        (main @ Some(ScoreValue::Score(main_f)), sub @ Some(ScoreValue::Score(sub_v))) => {
            // take max, both advance
            *main_score = main_it.next();
            *sub_score = sub_it.next();
            if main_f >= sub_v {
                main
            } else {
                sub
            }
        }
        (main @ Some(ScoreValue::Score(_)), _) => {
            *main_score = main_it.next();
            main
        }
        (_, sub @ Some(ScoreValue::Score(_))) => {
            *sub_score = sub_it.next();
            sub
        }
        (main @ Some(ScoreValue::GeoSort(main_geo)), sub @ Some(ScoreValue::GeoSort(sub_geo))) => {
            // take best advance both
            *main_score = main_it.next();
            *sub_score = sub_it.next();
            if main_geo >= sub_geo {
                main
            } else {
                sub
            }
        }
        (main @ Some(ScoreValue::Sort(main_sort)), sub @ Some(ScoreValue::Sort(sub_sort))) => {
            // take best advance both
            *main_score = main_it.next();
            *sub_score = sub_it.next();
            if main_sort >= sub_sort {
                main
            } else {
                sub
            }
        }
        (
            Some(ScoreValue::GeoSort(_) | ScoreValue::Sort(_)),
            Some(ScoreValue::GeoSort(_) | ScoreValue::Sort(_)),
        ) => None,

        (None, None) => None,
    }
}

impl CombinedSearchResult {
    fn new(main_results: SearchResult, ancillary_results: PartialSearchResult) -> Self {
        let mut docid_scores = HashMap::new();
        for (docid, score) in
            main_results.documents_ids.iter().zip(main_results.document_scores.into_iter())
        {
            docid_scores.insert(*docid, (score, None));
        }

        for (docid, score) in ancillary_results
            .documents_ids
            .iter()
            .zip(ancillary_results.document_scores.into_iter())
        {
            docid_scores
                .entry(*docid)
                .and_modify(|(_main_score, ancillary_score)| *ancillary_score = Some(score));
        }

        let mut document_scores: Vec<_> = docid_scores.into_iter().collect();

        document_scores.sort_by(|(_, left), (_, right)| compare_scores(left, right).reverse());

        Self {
            matching_words: main_results.matching_words,
            candidates: main_results.candidates,
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
            matching_words: left.matching_words,
            candidates: left.candidates | right.candidates,
            documents_ids,
            document_scores,
        }
    }
}

impl<'a> Search<'a> {
    pub fn execute_hybrid(&self) -> Result<SearchResult> {
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
        };

        let vector_query = search.vector.take();
        let keyword_query = self.query.as_deref();

        let keyword_results = search.execute()?;

        // skip semantic search if we don't have a vector query (placeholder search)
        let Some(vector_query) = vector_query else {
            return Ok(keyword_results);
        };

        // completely skip semantic search if the results of the keyword search are good enough
        if self.results_good_enough(&keyword_results) {
            return Ok(keyword_results);
        }

        search.vector = Some(vector_query);
        search.query = None;

        // TODO: would be better to have two distinct functions at this point
        let vector_results = search.execute()?;

        // Compute keyword scores for vector_results
        let keyword_results_for_vector =
            self.keyword_results_for_vector(keyword_query, &vector_results)?;

        // compute vector scores for keyword_results
        let vector_results_for_keyword =
            // can unwrap because we returned already if there was no vector query
            self.vector_results_for_keyword(search.vector.as_ref().unwrap(), &keyword_results)?;

        let keyword_results =
            CombinedSearchResult::new(keyword_results, vector_results_for_keyword);
        let vector_results = CombinedSearchResult::new(vector_results, keyword_results_for_vector);

        let merge_results =
            CombinedSearchResult::merge(vector_results, keyword_results, self.offset, self.limit);
        assert!(merge_results.documents_ids.len() <= self.limit);
        Ok(merge_results)
    }

    fn vector_results_for_keyword(
        &self,
        vector: &[f32],
        keyword_results: &SearchResult,
    ) -> Result<PartialSearchResult> {
        let embedder_name;
        let embedder_name = match &self.embedder_name {
            Some(embedder_name) => embedder_name,
            None => {
                embedder_name = self.index.default_embedding_name(self.rtxn)?;
                &embedder_name
            }
        };

        let mut ctx = SearchContext::new(self.index, self.rtxn);

        if let Some(searchable_attributes) = self.searchable_attributes {
            ctx.searchable_attributes(searchable_attributes)?;
        }

        let universe = keyword_results.documents_ids.iter().collect();

        execute_vector_search(
            &mut ctx,
            vector,
            ScoringStrategy::Detailed,
            universe,
            &self.sort_criteria,
            self.geo_strategy,
            0,
            self.limit + self.offset,
            self.distribution_shift,
            embedder_name,
        )
    }

    fn keyword_results_for_vector(
        &self,
        query: Option<&str>,
        vector_results: &SearchResult,
    ) -> Result<PartialSearchResult> {
        let mut ctx = SearchContext::new(self.index, self.rtxn);

        if let Some(searchable_attributes) = self.searchable_attributes {
            ctx.searchable_attributes(searchable_attributes)?;
        }

        let universe = vector_results.documents_ids.iter().collect();

        execute_search(
            &mut ctx,
            query,
            self.terms_matching_strategy,
            ScoringStrategy::Detailed,
            self.exhaustive_number_hits,
            universe,
            &self.sort_criteria,
            self.geo_strategy,
            0,
            self.limit + self.offset,
            Some(self.words_limit),
            &mut DefaultSearchLogger,
            &mut DefaultSearchLogger,
        )
    }

    fn results_good_enough(&self, keyword_results: &SearchResult) -> bool {
        const GOOD_ENOUGH_SCORE: f64 = 0.9;

        // 1. we check that we got a sufficient number of results
        if keyword_results.document_scores.len() < self.limit + self.offset {
            return false;
        }

        // 2. and that all results have a good enough score.
        // we need to check all results because due to sort like rules, they're not necessarily in relevancy order
        for score in &keyword_results.document_scores {
            let score = ScoreDetails::global_score(score.iter());
            if score < GOOD_ENOUGH_SCORE {
                return false;
            }
        }
        true
    }
}

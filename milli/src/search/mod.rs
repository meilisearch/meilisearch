use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::time::Instant;

use fst::{IntoStreamer, Streamer, Set};
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use log::debug;
use meilisearch_tokenizer::{AnalyzerConfig, Analyzer};
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;

use crate::search::criteria::{Criterion, CriterionResult};
use crate::search::criteria::{typo::Typo, words::Words};
use crate::{Index, FieldId, DocumentId};

pub use self::facet::{FacetCondition, FacetDistribution, FacetNumberOperator, FacetStringOperator};
pub use self::facet::{FacetIter};
use self::query_tree::QueryTreeBuilder;

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

mod facet;
mod query_tree;
mod criteria;

pub struct Search<'a> {
    query: Option<String>,
    facet_condition: Option<FacetCondition>,
    offset: usize,
    limit: usize,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> Search<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> Search<'a> {
        Search { query: None, facet_condition: None, offset: 0, limit: 20, rtxn, index }
    }

    pub fn query(&mut self, query: impl Into<String>) -> &mut Search<'a> {
        self.query = Some(query.into());
        self
    }

    pub fn offset(&mut self, offset: usize) -> &mut Search<'a> {
        self.offset = offset;
        self
    }

    pub fn limit(&mut self, limit: usize) -> &mut Search<'a> {
        self.limit = limit;
        self
    }

    pub fn facet_condition(&mut self, condition: FacetCondition) -> &mut Search<'a> {
        self.facet_condition = Some(condition);
        self
    }

    pub fn execute(&self) -> anyhow::Result<SearchResult> {
        // We create the query tree by spliting the query into tokens.
        let before = Instant::now();
        let query_tree = match self.query.as_ref() {
            Some(query) => {
                let mut builder = QueryTreeBuilder::new(self.rtxn, self.index);
                let stop_words = &Set::default();
                let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
                let result = analyzer.analyze(query);
                let tokens = result.tokens();
                builder.build(tokens)
            },
            None => None,
        };

        debug!("query tree: {:?} took {:.02?}", query_tree, before.elapsed());

        // We create the original candidates with the facet conditions results.
        let before = Instant::now();
        let facet_candidates = match &self.facet_condition {
            Some(condition) => Some(condition.evaluate(self.rtxn, self.index)?),
            None => None,
        };

        debug!("facet candidates: {:?} took {:.02?}", facet_candidates, before.elapsed());

        // We aretesting the typo criteria but there will be more of them soon.
        let criteria_ctx = criteria::HeedContext::new(self.rtxn, self.index)?;
        let typo_criterion = Typo::initial(&criteria_ctx, query_tree, facet_candidates)?;
        let mut criteria = Words::new(&criteria_ctx, Box::new(typo_criterion))?;

        let mut offset = self.offset;
        let mut limit = self.limit;
        let mut documents_ids = Vec::new();
        let mut initial_candidates = RoaringBitmap::new();
        while let Some(CriterionResult { candidates, bucket_candidates, .. }) = criteria.next()? {

            let mut len = candidates.len() as usize;
            let mut candidates = candidates.into_iter();

            initial_candidates.union_with(&bucket_candidates);

            if offset != 0 {
                candidates.by_ref().skip(offset).for_each(drop);
                offset = offset.saturating_sub(len.min(offset));
                len = len.saturating_sub(len.min(offset));
            }

            if len != 0 {
                documents_ids.extend(candidates.take(limit));
                limit = limit.saturating_sub(len.min(limit));
            }

            if limit == 0 { break }
        }

        let found_words = HashSet::new();
        Ok(SearchResult { found_words, candidates: initial_candidates, documents_ids })
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Search { query, facet_condition, offset, limit, rtxn: _, index: _ } = self;
        f.debug_struct("Search")
            .field("query", query)
            .field("facet_condition", facet_condition)
            .field("offset", offset)
            .field("limit", limit)
            .finish()
    }
}

#[derive(Default)]
pub struct SearchResult {
    pub found_words: HashSet<String>,
    pub candidates: RoaringBitmap,
    // TODO those documents ids should be associated with their criteria scores.
    pub documents_ids: Vec<DocumentId>,
}

pub fn word_derivations(word: &str, is_prefix: bool, max_typo: u8, fst: &fst::Set<Cow<[u8]>>) -> anyhow::Result<Vec<(String, u8)>> {
    let lev = match max_typo {
        0 => &LEVDIST0,
        1 => &LEVDIST1,
        _ => &LEVDIST2,
    };

    let dfa = if is_prefix {
        lev.build_prefix_dfa(&word)
    } else {
        lev.build_dfa(&word)
    };

    let mut derived_words = Vec::new();
    let mut stream = fst.search_with_state(&dfa).into_stream();

    while let Some((word, state)) = stream.next() {
        let word = std::str::from_utf8(word)?;
        let distance = dfa.distance(state);
        derived_words.push((word.to_string(), distance.to_u8()));
    }

    Ok(derived_words)
}

use std::borrow::Cow;
use std::collections::hash_map::{Entry, HashMap};
use std::fmt;
use std::mem::take;
use std::result::Result as StdResult;
use std::str::Utf8Error;
use std::time::Instant;

use distinct::{Distinct, DocIter, FacetDistinct, NoopDistinct};
use fst::{IntoStreamer, Streamer};
use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};
use log::debug;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;

pub(crate) use self::facet::ParserRule;
pub use self::facet::{FacetDistribution, FacetNumberIter, FilterCondition, Operator};
pub use self::matching_words::MatchingWords;
use self::query_tree::QueryTreeBuilder;
use crate::error::FieldIdMapMissingEntry;
use crate::search::criteria::r#final::{Final, FinalResult};
use crate::{DocumentId, Index, Result};

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

mod criteria;
mod distinct;
mod facet;
mod matching_words;
mod query_tree;

pub struct Search<'a> {
    query: Option<String>,
    filter: Option<FilterCondition>,
    offset: usize,
    limit: usize,
    optional_words: bool,
    authorize_typos: bool,
    words_limit: usize,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> Search<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> Search<'a> {
        Search {
            query: None,
            filter: None,
            offset: 0,
            limit: 20,
            optional_words: true,
            authorize_typos: true,
            words_limit: 10,
            rtxn,
            index,
        }
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

    pub fn optional_words(&mut self, value: bool) -> &mut Search<'a> {
        self.optional_words = value;
        self
    }

    pub fn authorize_typos(&mut self, value: bool) -> &mut Search<'a> {
        self.authorize_typos = value;
        self
    }

    pub fn words_limit(&mut self, value: usize) -> &mut Search<'a> {
        self.words_limit = value;
        self
    }

    pub fn filter(&mut self, condition: FilterCondition) -> &mut Search<'a> {
        self.filter = Some(condition);
        self
    }

    pub fn execute(&self) -> Result<SearchResult> {
        // We create the query tree by spliting the query into tokens.
        let before = Instant::now();
        let (query_tree, primitive_query) = match self.query.as_ref() {
            Some(query) => {
                let mut builder = QueryTreeBuilder::new(self.rtxn, self.index);
                builder.optional_words(self.optional_words);
                builder.authorize_typos(self.authorize_typos);
                builder.words_limit(self.words_limit);
                // We make sure that the analyzer is aware of the stop words
                // this ensures that the query builder is able to properly remove them.
                let mut config = AnalyzerConfig::default();
                let stop_words = self.index.stop_words(self.rtxn)?;
                if let Some(ref stop_words) = stop_words {
                    config.stop_words(stop_words);
                }
                let analyzer = Analyzer::new(config);
                let result = analyzer.analyze(query);
                let tokens = result.tokens();
                builder.build(tokens)?.map_or((None, None), |(qt, pq)| (Some(qt), Some(pq)))
            }
            None => (None, None),
        };

        debug!("query tree: {:?} took {:.02?}", query_tree, before.elapsed());

        // We create the original candidates with the facet conditions results.
        let before = Instant::now();
        let filtered_candidates = match &self.filter {
            Some(condition) => Some(condition.evaluate(self.rtxn, self.index)?),
            None => None,
        };

        debug!("facet candidates: {:?} took {:.02?}", filtered_candidates, before.elapsed());

        let matching_words = match query_tree.as_ref() {
            Some(query_tree) => MatchingWords::from_query_tree(&query_tree),
            None => MatchingWords::default(),
        };

        let criteria_builder = criteria::CriteriaBuilder::new(self.rtxn, self.index)?;
        let criteria = criteria_builder.build(query_tree, primitive_query, filtered_candidates)?;

        match self.index.distinct_field(self.rtxn)? {
            None => self.perform_sort(NoopDistinct, matching_words, criteria),
            Some(name) => {
                let field_ids_map = self.index.fields_ids_map(self.rtxn)?;
                let id =
                    field_ids_map.id(name).ok_or_else(|| FieldIdMapMissingEntry::FieldName {
                        field_name: name.to_string(),
                        process: "distinct attribute",
                    })?;
                let distinct = FacetDistinct::new(id, self.index, self.rtxn);
                self.perform_sort(distinct, matching_words, criteria)
            }
        }
    }

    fn perform_sort<D: Distinct>(
        &self,
        mut distinct: D,
        matching_words: MatchingWords,
        mut criteria: Final,
    ) -> Result<SearchResult> {
        let mut offset = self.offset;
        let mut initial_candidates = RoaringBitmap::new();
        let mut excluded_candidates = RoaringBitmap::new();
        let mut documents_ids = Vec::new();

        while let Some(FinalResult { candidates, bucket_candidates, .. }) =
            criteria.next(&excluded_candidates)?
        {
            debug!("Number of candidates found {}", candidates.len());

            let excluded = take(&mut excluded_candidates);

            let mut candidates = distinct.distinct(candidates, excluded);

            initial_candidates |= bucket_candidates;

            if offset != 0 {
                let discarded = candidates.by_ref().take(offset).count();
                offset = offset.saturating_sub(discarded);
            }

            for candidate in candidates.by_ref().take(self.limit - documents_ids.len()) {
                documents_ids.push(candidate?);
            }
            if documents_ids.len() == self.limit {
                break;
            }
            excluded_candidates = candidates.into_excluded();
        }

        Ok(SearchResult { matching_words, candidates: initial_candidates, documents_ids })
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Search {
            query,
            filter,
            offset,
            limit,
            optional_words,
            authorize_typos,
            words_limit,
            rtxn: _,
            index: _,
        } = self;
        f.debug_struct("Search")
            .field("query", query)
            .field("filter", filter)
            .field("offset", offset)
            .field("limit", limit)
            .field("optional_words", optional_words)
            .field("authorize_typos", authorize_typos)
            .field("words_limit", words_limit)
            .finish()
    }
}

#[derive(Default)]
pub struct SearchResult {
    pub matching_words: MatchingWords,
    pub candidates: RoaringBitmap,
    // TODO those documents ids should be associated with their criteria scores.
    pub documents_ids: Vec<DocumentId>,
}

pub type WordDerivationsCache = HashMap<(String, bool, u8), Vec<(String, u8)>>;

pub fn word_derivations<'c>(
    word: &str,
    is_prefix: bool,
    max_typo: u8,
    fst: &fst::Set<Cow<[u8]>>,
    cache: &'c mut WordDerivationsCache,
) -> StdResult<&'c [(String, u8)], Utf8Error> {
    match cache.entry((word.to_string(), is_prefix, max_typo)) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => {
            let mut derived_words = Vec::new();
            let dfa = build_dfa(word, max_typo, is_prefix);
            let mut stream = fst.search_with_state(&dfa).into_stream();

            while let Some((word, state)) = stream.next() {
                let word = std::str::from_utf8(word)?;
                let distance = dfa.distance(state);
                derived_words.push((word.to_string(), distance.to_u8()));
            }

            Ok(entry.insert(derived_words))
        }
    }
}

pub fn build_dfa(word: &str, typos: u8, is_prefix: bool) -> DFA {
    let lev = match typos {
        0 => &LEVDIST0,
        1 => &LEVDIST1,
        _ => &LEVDIST2,
    };

    if is_prefix {
        lev.build_prefix_dfa(word)
    } else {
        lev.build_dfa(word)
    }
}

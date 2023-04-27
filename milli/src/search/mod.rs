use std::fmt;

use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};
use log::{debug, error};
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;

pub use self::facet::{FacetDistribution, Filter, DEFAULT_VALUES_PER_FACET};
pub use self::new::matches::{FormatOptions, MatchBounds, Matcher, MatcherBuilder, MatchingWords};
use self::new::PartialSearchResult;
use crate::error::UserError;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::{
    execute_search, AscDesc, DefaultSearchLogger, DocumentId, FieldIdMapMissingEntry, Index,
    Result, SearchContext, BEU16,
};
use fst::automaton::{Complement, Intersection, StartsWith, Str, Union};
use fst::Streamer;

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

/// The maximum number of facets returned by the facet search route.
const MAX_NUMBER_OF_FACETS: usize = 100;

pub mod facet;
mod fst_utils;
pub mod new;

pub struct Search<'a> {
    query: Option<String>,
    // this should be linked to the String in the query
    filter: Option<Filter<'a>>,
    offset: usize,
    limit: usize,
    sort_criteria: Option<Vec<AscDesc>>,
    geo_strategy: new::GeoSortStrategy,
    terms_matching_strategy: TermsMatchingStrategy,
    words_limit: usize,
    exhaustive_number_hits: bool,
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
            sort_criteria: None,
            geo_strategy: new::GeoSortStrategy::default(),
            terms_matching_strategy: TermsMatchingStrategy::default(),
            exhaustive_number_hits: false,
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

    pub fn sort_criteria(&mut self, criteria: Vec<AscDesc>) -> &mut Search<'a> {
        self.sort_criteria = Some(criteria);
        self
    }

    pub fn terms_matching_strategy(&mut self, value: TermsMatchingStrategy) -> &mut Search<'a> {
        self.terms_matching_strategy = value;
        self
    }

    pub fn words_limit(&mut self, value: usize) -> &mut Search<'a> {
        self.words_limit = value;
        self
    }

    pub fn filter(&mut self, condition: Filter<'a>) -> &mut Search<'a> {
        self.filter = Some(condition);
        self
    }

    #[cfg(test)]
    pub fn geo_sort_strategy(&mut self, strategy: new::GeoSortStrategy) -> &mut Search<'a> {
        self.geo_strategy = strategy;
        self
    }

    /// Force the search to exhastivelly compute the number of candidates,
    /// this will increase the search time but allows finite pagination.
    pub fn exhaustive_number_hits(&mut self, exhaustive_number_hits: bool) -> &mut Search<'a> {
        self.exhaustive_number_hits = exhaustive_number_hits;
        self
    }

    pub fn execute(&self) -> Result<SearchResult> {
        let mut ctx = SearchContext::new(self.index, self.rtxn);
        let PartialSearchResult { located_query_terms, candidates, documents_ids } =
            execute_search(
                &mut ctx,
                &self.query,
                self.terms_matching_strategy,
                self.exhaustive_number_hits,
                &self.filter,
                &self.sort_criteria,
                self.geo_strategy,
                self.offset,
                self.limit,
                Some(self.words_limit),
                &mut DefaultSearchLogger,
                &mut DefaultSearchLogger,
            )?;

        // consume context and located_query_terms to build MatchingWords.
        let matching_words = match located_query_terms {
            Some(located_query_terms) => MatchingWords::new(ctx, located_query_terms),
            None => MatchingWords::default(),
        };

        Ok(SearchResult { matching_words, candidates, documents_ids })
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Search {
            query,
            filter,
            offset,
            limit,
            sort_criteria,
            geo_strategy: _,
            terms_matching_strategy,
            words_limit,
            exhaustive_number_hits,
            rtxn: _,
            index: _,
        } = self;
        f.debug_struct("Search")
            .field("query", query)
            .field("filter", filter)
            .field("offset", offset)
            .field("limit", limit)
            .field("sort_criteria", sort_criteria)
            .field("terms_matching_strategy", terms_matching_strategy)
            .field("exhaustive_number_hits", exhaustive_number_hits)
            .field("words_limit", words_limit)
            .finish()
    }
}

#[derive(Default, Debug)]
pub struct SearchResult {
    pub matching_words: MatchingWords,
    pub candidates: RoaringBitmap,
    // TODO those documents ids should be associated with their criteria scores.
    pub documents_ids: Vec<DocumentId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermsMatchingStrategy {
    // remove last word first
    Last,
    // all words are mandatory
    All,
}

impl Default for TermsMatchingStrategy {
    fn default() -> Self {
        Self::Last
    }
}

fn get_first(s: &str) -> &str {
    match s.chars().next() {
        Some(c) => &s[..c.len_utf8()],
        None => panic!("unexpected empty query"),
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

pub struct SearchForFacetValues<'a> {
    query: Option<String>,
    facet: String,
    search_query: Search<'a>,
}

impl<'a> SearchForFacetValues<'a> {
    pub fn new(facet: String, search_query: Search<'a>) -> SearchForFacetValues<'a> {
        SearchForFacetValues { query: None, facet, search_query }
    }

    pub fn query(&mut self, query: impl Into<String>) -> &mut Self {
        self.query = Some(query.into());
        self
    }

    pub fn execute(&self) -> Result<Vec<FacetValueHit>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;

        let filterable_fields = index.filterable_fields(rtxn)?;
        if !filterable_fields.contains(&self.facet) {
            return Err(UserError::InvalidSearchFacet {
                field: self.facet.clone(),
                valid_fields: filterable_fields.into_iter().collect(),
            }
            .into());
        }

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let fid = match fields_ids_map.id(&self.facet) {
            Some(fid) => fid,
            None => {
                return Err(FieldIdMapMissingEntry::FieldName {
                    field_name: self.facet.clone(),
                    process: "search for facet values",
                }
                .into());
            }
        };
        let fst = match self.search_query.index.facet_id_string_fst.get(rtxn, &BEU16::new(fid))? {
            Some(fst) => fst,
            None => return Ok(vec![]),
        };

        let search_candidates = self.search_query.execute()?.candidates;

        match self.query.as_ref() {
            Some(query) => {
                let is_prefix = true;
                let starts = StartsWith(Str::new(get_first(query)));
                let first = Intersection(build_dfa(query, 1, is_prefix), Complement(&starts));
                let second_dfa = build_dfa(query, 2, is_prefix);
                let second = Intersection(&second_dfa, &starts);
                let automaton = Union(first, &second);

                let mut stream = fst.search(automaton).into_stream();
                let mut result = vec![];
                let mut length = 0;
                while let Some(facet_value) = stream.next() {
                    let value = std::str::from_utf8(facet_value)?;
                    let key = FacetGroupKey { field_id: fid, level: 0, left_bound: value };
                    let docids = match index.facet_id_string_docids.get(rtxn, &key)? {
                        Some(FacetGroupValue { bitmap, .. }) => bitmap,
                        None => {
                            error!("the facet value is missing from the facet database: {key:?}");
                            continue;
                        }
                    };
                    let count = search_candidates.intersection_len(&docids);
                    if count != 0 {
                        result.push(FacetValueHit { value: value.to_string(), count });
                        length += 1;
                    }
                    if length >= MAX_NUMBER_OF_FACETS {
                        break;
                    }
                }

                Ok(result)
            }
            None => {
                let mut stream = fst.stream();
                let mut result = vec![];
                let mut length = 0;
                while let Some(facet_value) = stream.next() {
                    let value = std::str::from_utf8(facet_value)?;
                    let key = FacetGroupKey { field_id: fid, level: 0, left_bound: value };
                    let docids = match index.facet_id_string_docids.get(rtxn, &key)? {
                        Some(FacetGroupValue { bitmap, .. }) => bitmap,
                        None => {
                            error!("the facet value is missing from the facet database: {key:?}");
                            continue;
                        }
                    };
                    let count = search_candidates.intersection_len(&docids);
                    if count != 0 {
                        result.push(FacetValueHit { value: value.to_string(), count });
                        length += 1;
                    }
                    if length >= MAX_NUMBER_OF_FACETS {
                        break;
                    }
                }

                Ok(result)
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct FacetValueHit {
    /// The original facet value
    pub value: String,
    /// The number of documents associated to this facet
    pub count: u64,
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;

    #[cfg(feature = "japanese")]
    #[test]
    fn test_kanji_language_detection() {
        use crate::index::tests::TempIndex;

        let index = TempIndex::new();

        index
            .add_documents(documents!([
                { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
                { "id": 1, "title": "東京のお寿司。" },
                { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
            ]))
            .unwrap();

        let txn = index.write_txn().unwrap();
        let mut search = Search::new(&txn, &index);

        search.query("東京");
        let SearchResult { documents_ids, .. } = search.execute().unwrap();

        assert_eq!(documents_ids, vec![1]);
    }
}

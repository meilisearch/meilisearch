use std::fmt;
use std::ops::ControlFlow;

use charabia::normalizer::NormalizerOption;
use charabia::Normalize;
use fst::automaton::{Automaton, Str};
use fst::{IntoStreamer, Streamer};
use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};
use log::error;
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;

pub use self::facet::{FacetDistribution, Filter, OrderBy, DEFAULT_VALUES_PER_FACET};
pub use self::new::matches::{FormatOptions, MatchBounds, Matcher, MatcherBuilder, MatchingWords};
use self::new::PartialSearchResult;
use crate::error::UserError;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::score_details::{ScoreDetails, ScoringStrategy};
use crate::{
    execute_search, AscDesc, DefaultSearchLogger, DocumentId, FieldId, Index, Result,
    SearchContext, BEU16,
};

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
    vector: Option<Vec<f32>>,
    // this should be linked to the String in the query
    filter: Option<Filter<'a>>,
    offset: usize,
    limit: usize,
    sort_criteria: Option<Vec<AscDesc>>,
    searchable_attributes: Option<&'a [String]>,
    geo_strategy: new::GeoSortStrategy,
    terms_matching_strategy: TermsMatchingStrategy,
    scoring_strategy: ScoringStrategy,
    words_limit: usize,
    exhaustive_number_hits: bool,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> Search<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> Search<'a> {
        Search {
            query: None,
            vector: None,
            filter: None,
            offset: 0,
            limit: 20,
            sort_criteria: None,
            searchable_attributes: None,
            geo_strategy: new::GeoSortStrategy::default(),
            terms_matching_strategy: TermsMatchingStrategy::default(),
            scoring_strategy: Default::default(),
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

    pub fn vector(&mut self, vector: impl Into<Vec<f32>>) -> &mut Search<'a> {
        self.vector = Some(vector.into());
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

    pub fn searchable_attributes(&mut self, searchable: &'a [String]) -> &mut Search<'a> {
        self.searchable_attributes = Some(searchable);
        self
    }

    pub fn terms_matching_strategy(&mut self, value: TermsMatchingStrategy) -> &mut Search<'a> {
        self.terms_matching_strategy = value;
        self
    }

    pub fn scoring_strategy(&mut self, value: ScoringStrategy) -> &mut Search<'a> {
        self.scoring_strategy = value;
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

    /// Forces the search to exhaustively compute the number of candidates,
    /// this will increase the search time but allows finite pagination.
    pub fn exhaustive_number_hits(&mut self, exhaustive_number_hits: bool) -> &mut Search<'a> {
        self.exhaustive_number_hits = exhaustive_number_hits;
        self
    }

    pub fn execute(&self) -> Result<SearchResult> {
        let mut ctx = SearchContext::new(self.index, self.rtxn);

        if let Some(searchable_attributes) = self.searchable_attributes {
            ctx.searchable_attributes(searchable_attributes)?;
        }

        let PartialSearchResult { located_query_terms, candidates, documents_ids, document_scores } =
            execute_search(
                &mut ctx,
                &self.query,
                &self.vector,
                self.terms_matching_strategy,
                self.scoring_strategy,
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

        Ok(SearchResult { matching_words, candidates, document_scores, documents_ids })
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Search {
            query,
            vector: _,
            filter,
            offset,
            limit,
            sort_criteria,
            searchable_attributes,
            geo_strategy: _,
            terms_matching_strategy,
            scoring_strategy,
            words_limit,
            exhaustive_number_hits,
            rtxn: _,
            index: _,
        } = self;
        f.debug_struct("Search")
            .field("query", query)
            .field("vector", &"[...]")
            .field("filter", filter)
            .field("offset", offset)
            .field("limit", limit)
            .field("sort_criteria", sort_criteria)
            .field("searchable_attributes", searchable_attributes)
            .field("terms_matching_strategy", terms_matching_strategy)
            .field("scoring_strategy", scoring_strategy)
            .field("exhaustive_number_hits", exhaustive_number_hits)
            .field("words_limit", words_limit)
            .finish()
    }
}

#[derive(Default, Debug)]
pub struct SearchResult {
    pub matching_words: MatchingWords,
    pub candidates: RoaringBitmap,
    pub documents_ids: Vec<DocumentId>,
    pub document_scores: Vec<Vec<ScoreDetails>>,
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

    fn one_original_value_of(
        &self,
        field_id: FieldId,
        facet_str: &str,
        any_docid: DocumentId,
    ) -> Result<Option<String>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;
        let key: (FieldId, _, &str) = (field_id, any_docid, facet_str);
        Ok(index.field_id_docid_facet_strings.get(rtxn, &key)?.map(|v| v.to_owned()))
    }

    pub fn execute(&self) -> Result<Vec<FacetValueHit>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;

        let filterable_fields = index.filterable_fields(rtxn)?;
        if !filterable_fields.contains(&self.facet) {
            let (valid_fields, hidden_fields) =
                index.remove_hidden_fields(rtxn, filterable_fields)?;

            return Err(UserError::InvalidFacetSearchFacetName {
                field: self.facet.clone(),
                valid_fields,
                hidden_fields,
            }
            .into());
        }

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let fid = match fields_ids_map.id(&self.facet) {
            Some(fid) => fid,
            // we return an empty list of results when the attribute has been
            // set as filterable but no document contains this field (yet).
            None => return Ok(Vec::new()),
        };

        let fst = match self.search_query.index.facet_id_string_fst.get(rtxn, &BEU16::new(fid))? {
            Some(fst) => fst,
            None => return Ok(vec![]),
        };

        let search_candidates = self.search_query.execute()?.candidates;

        match self.query.as_ref() {
            Some(query) => {
                let options = NormalizerOption { lossy: true, ..Default::default() };
                let query = query.normalize(&options);
                let query = query.as_ref();

                let authorize_typos = self.search_query.index.authorize_typos(rtxn)?;
                let field_authorizes_typos =
                    !self.search_query.index.exact_attributes_ids(rtxn)?.contains(&fid);

                if authorize_typos && field_authorizes_typos {
                    let exact_words_fst = self.search_query.index.exact_words(rtxn)?;
                    if exact_words_fst.map_or(false, |fst| fst.contains(query)) {
                        let mut results = vec![];
                        if fst.contains(query) {
                            self.fetch_original_facets_using_normalized(
                                fid,
                                query,
                                query,
                                &search_candidates,
                                &mut results,
                            )?;
                        }
                        Ok(results)
                    } else {
                        let one_typo = self.search_query.index.min_word_len_one_typo(rtxn)?;
                        let two_typos = self.search_query.index.min_word_len_two_typos(rtxn)?;

                        let is_prefix = true;
                        let automaton = if query.len() < one_typo as usize {
                            build_dfa(query, 0, is_prefix)
                        } else if query.len() < two_typos as usize {
                            build_dfa(query, 1, is_prefix)
                        } else {
                            build_dfa(query, 2, is_prefix)
                        };

                        let mut stream = fst.search(automaton).into_stream();
                        let mut results = vec![];
                        while let Some(facet_value) = stream.next() {
                            let value = std::str::from_utf8(facet_value)?;
                            if self
                                .fetch_original_facets_using_normalized(
                                    fid,
                                    value,
                                    query,
                                    &search_candidates,
                                    &mut results,
                                )?
                                .is_break()
                            {
                                break;
                            }
                        }

                        Ok(results)
                    }
                } else {
                    let automaton = Str::new(query).starts_with();
                    let mut stream = fst.search(automaton).into_stream();
                    let mut results = vec![];
                    while let Some(facet_value) = stream.next() {
                        let value = std::str::from_utf8(facet_value)?;
                        if self
                            .fetch_original_facets_using_normalized(
                                fid,
                                value,
                                query,
                                &search_candidates,
                                &mut results,
                            )?
                            .is_break()
                        {
                            break;
                        }
                    }

                    Ok(results)
                }
            }
            None => {
                let mut results = vec![];
                let prefix = FacetGroupKey { field_id: fid, level: 0, left_bound: "" };
                for result in index.facet_id_string_docids.prefix_iter(rtxn, &prefix)? {
                    let (FacetGroupKey { left_bound, .. }, FacetGroupValue { bitmap, .. }) =
                        result?;
                    let count = search_candidates.intersection_len(&bitmap);
                    if count != 0 {
                        let value = self
                            .one_original_value_of(fid, left_bound, bitmap.min().unwrap())?
                            .unwrap_or_else(|| left_bound.to_string());
                        results.push(FacetValueHit { value, count });
                    }
                    if results.len() >= MAX_NUMBER_OF_FACETS {
                        break;
                    }
                }
                Ok(results)
            }
        }
    }

    fn fetch_original_facets_using_normalized(
        &self,
        fid: FieldId,
        value: &str,
        query: &str,
        search_candidates: &RoaringBitmap,
        results: &mut Vec<FacetValueHit>,
    ) -> Result<ControlFlow<()>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;

        let database = index.facet_id_normalized_string_strings;
        let key = (fid, value);
        let original_strings = match database.get(rtxn, &key)? {
            Some(original_strings) => original_strings,
            None => {
                error!("the facet value is missing from the facet database: {key:?}");
                return Ok(ControlFlow::Continue(()));
            }
        };
        for original in original_strings {
            let key = FacetGroupKey { field_id: fid, level: 0, left_bound: original.as_str() };
            let docids = match index.facet_id_string_docids.get(rtxn, &key)? {
                Some(FacetGroupValue { bitmap, .. }) => bitmap,
                None => {
                    error!("the facet value is missing from the facet database: {key:?}");
                    return Ok(ControlFlow::Continue(()));
                }
            };
            let count = search_candidates.intersection_len(&docids);
            if count != 0 {
                let value = self
                    .one_original_value_of(fid, &original, docids.min().unwrap())?
                    .unwrap_or_else(|| query.to_string());
                results.push(FacetValueHit { value, count });
            }
            if results.len() >= MAX_NUMBER_OF_FACETS {
                return Ok(ControlFlow::Break(()));
            }
        }

        Ok(ControlFlow::Continue(()))
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

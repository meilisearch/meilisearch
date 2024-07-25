use std::fmt;
use std::sync::Arc;

use charabia::Language;
use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;

pub use self::facet::{FacetDistribution, Filter, OrderBy, DEFAULT_VALUES_PER_FACET};
pub use self::new::matches::{FormatOptions, MatchBounds, MatcherBuilder, MatchingWords};
use self::new::{execute_vector_search, PartialSearchResult};
use crate::score_details::{ScoreDetails, ScoringStrategy};
use crate::vector::Embedder;
use crate::{
    execute_search, filtered_universe, AscDesc, DefaultSearchLogger, DocumentId, Error, Index,
    Result, SearchContext, TimeBudget, UserError,
};

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

pub mod facet;
mod fst_utils;
pub mod hybrid;
pub mod new;
pub mod similar;

#[derive(Debug, Clone)]
pub struct SemanticSearch {
    vector: Option<Vec<f32>>,
    embedder_name: String,
    embedder: Arc<Embedder>,
}

pub struct Search<'a> {
    query: Option<String>,
    // this should be linked to the String in the query
    filter: Option<Filter<'a>>,
    offset: usize,
    limit: usize,
    sort_criteria: Option<Vec<AscDesc>>,
    distinct: Option<String>,
    searchable_attributes: Option<&'a [String]>,
    geo_strategy: new::GeoSortStrategy,
    terms_matching_strategy: TermsMatchingStrategy,
    scoring_strategy: ScoringStrategy,
    words_limit: usize,
    exhaustive_number_hits: bool,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
    semantic: Option<SemanticSearch>,
    time_budget: TimeBudget,
    ranking_score_threshold: Option<f64>,
    locales: Option<Vec<Language>>,
}

impl<'a> Search<'a> {
    pub fn new(rtxn: &'a heed::RoTxn<'a>, index: &'a Index) -> Search<'a> {
        Search {
            query: None,
            filter: None,
            offset: 0,
            limit: 20,
            sort_criteria: None,
            distinct: None,
            searchable_attributes: None,
            geo_strategy: new::GeoSortStrategy::default(),
            terms_matching_strategy: TermsMatchingStrategy::default(),
            scoring_strategy: Default::default(),
            exhaustive_number_hits: false,
            words_limit: 10,
            rtxn,
            index,
            semantic: None,
            locales: None,
            time_budget: TimeBudget::max(),
            ranking_score_threshold: None,
        }
    }

    pub fn query(&mut self, query: impl Into<String>) -> &mut Search<'a> {
        self.query = Some(query.into());
        self
    }

    pub fn semantic(
        &mut self,
        embedder_name: String,
        embedder: Arc<Embedder>,
        vector: Option<Vec<f32>>,
    ) -> &mut Search<'a> {
        self.semantic = Some(SemanticSearch { embedder_name, embedder, vector });
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

    pub fn distinct(&mut self, distinct: String) -> &mut Search<'a> {
        self.distinct = Some(distinct);
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

    pub fn time_budget(&mut self, time_budget: TimeBudget) -> &mut Search<'a> {
        self.time_budget = time_budget;
        self
    }

    pub fn ranking_score_threshold(&mut self, ranking_score_threshold: f64) -> &mut Search<'a> {
        self.ranking_score_threshold = Some(ranking_score_threshold);
        self
    }

    pub fn locales(&mut self, locales: Vec<Language>) -> &mut Search<'a> {
        self.locales = Some(locales);
        self
    }

    pub fn execute_for_candidates(&self, has_vector_search: bool) -> Result<RoaringBitmap> {
        if has_vector_search {
            let ctx = SearchContext::new(self.index, self.rtxn)?;
            filtered_universe(ctx.index, ctx.txn, &self.filter)
        } else {
            Ok(self.execute()?.candidates)
        }
    }

    pub fn execute(&self) -> Result<SearchResult> {
        let mut ctx = SearchContext::new(self.index, self.rtxn)?;

        if let Some(searchable_attributes) = self.searchable_attributes {
            ctx.attributes_to_search_on(searchable_attributes)?;
        }

        if let Some(distinct) = &self.distinct {
            let filterable_fields = ctx.index.filterable_fields(ctx.txn)?;
            if !crate::is_faceted(distinct, &filterable_fields) {
                let (valid_fields, hidden_fields) =
                    ctx.index.remove_hidden_fields(ctx.txn, filterable_fields)?;
                return Err(Error::UserError(UserError::InvalidDistinctAttribute {
                    field: distinct.clone(),
                    valid_fields,
                    hidden_fields,
                }));
            }
        }

        let universe = filtered_universe(ctx.index, ctx.txn, &self.filter)?;
        let PartialSearchResult {
            located_query_terms,
            candidates,
            documents_ids,
            document_scores,
            degraded,
            used_negative_operator,
        } = match self.semantic.as_ref() {
            Some(SemanticSearch { vector: Some(vector), embedder_name, embedder }) => {
                execute_vector_search(
                    &mut ctx,
                    vector,
                    self.scoring_strategy,
                    universe,
                    &self.sort_criteria,
                    &self.distinct,
                    self.geo_strategy,
                    self.offset,
                    self.limit,
                    embedder_name,
                    embedder,
                    self.time_budget.clone(),
                    self.ranking_score_threshold,
                )?
            }
            _ => execute_search(
                &mut ctx,
                self.query.as_deref(),
                self.terms_matching_strategy,
                self.scoring_strategy,
                self.exhaustive_number_hits,
                universe,
                &self.sort_criteria,
                &self.distinct,
                self.geo_strategy,
                self.offset,
                self.limit,
                Some(self.words_limit),
                &mut DefaultSearchLogger,
                &mut DefaultSearchLogger,
                self.time_budget.clone(),
                self.ranking_score_threshold,
                self.locales.as_ref(),
            )?,
        };

        // consume context and located_query_terms to build MatchingWords.
        let matching_words = match located_query_terms {
            Some(located_query_terms) => MatchingWords::new(ctx, located_query_terms),
            None => MatchingWords::default(),
        };

        Ok(SearchResult {
            matching_words,
            candidates,
            document_scores,
            documents_ids,
            degraded,
            used_negative_operator,
        })
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Search {
            query,
            filter,
            offset,
            limit,
            sort_criteria,
            distinct,
            searchable_attributes,
            geo_strategy: _,
            terms_matching_strategy,
            scoring_strategy,
            words_limit,
            exhaustive_number_hits,
            rtxn: _,
            index: _,
            semantic,
            time_budget,
            ranking_score_threshold,
            locales,
        } = self;
        f.debug_struct("Search")
            .field("query", query)
            .field("vector", &"[...]")
            .field("filter", filter)
            .field("offset", offset)
            .field("limit", limit)
            .field("sort_criteria", sort_criteria)
            .field("distinct", distinct)
            .field("searchable_attributes", searchable_attributes)
            .field("terms_matching_strategy", terms_matching_strategy)
            .field("scoring_strategy", scoring_strategy)
            .field("exhaustive_number_hits", exhaustive_number_hits)
            .field("words_limit", words_limit)
            .field(
                "semantic.embedder_name",
                &semantic.as_ref().map(|semantic| &semantic.embedder_name),
            )
            .field("time_budget", time_budget)
            .field("ranking_score_threshold", ranking_score_threshold)
            .field("locales", locales)
            .finish()
    }
}

#[derive(Default, Debug)]
pub struct SearchResult {
    pub matching_words: MatchingWords,
    pub candidates: RoaringBitmap,
    pub documents_ids: Vec<DocumentId>,
    pub document_scores: Vec<Vec<ScoreDetails>>,
    pub degraded: bool,
    pub used_negative_operator: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermsMatchingStrategy {
    // remove last word first
    Last,
    // all words are mandatory
    All,
    // remove more frequent word first
    Frequency,
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

    #[cfg(feature = "korean")]
    #[test]
    fn test_hangul_language_detection() {
        use crate::index::tests::TempIndex;

        let index = TempIndex::new();

        index
            .add_documents(documents!([
                { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
                { "id": 1, "title": "김밥먹을래。" },
                { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
            ]))
            .unwrap();

        let txn = index.write_txn().unwrap();
        let mut search = Search::new(&txn, &index);

        search.query("김밥");
        let SearchResult { documents_ids, .. } = search.execute().unwrap();

        assert_eq!(documents_ids, vec![1]);
    }
}

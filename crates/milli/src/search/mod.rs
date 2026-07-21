use std::fmt;
use std::sync::Arc;

use charabia::Language;
use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;
use time::OffsetDateTime;

pub use self::facet::{
    serialize_index_filter_to_filter_string, FacetDistribution, Filter, IndexFilter, OrderBy,
    DEFAULT_VALUES_PER_FACET,
};
pub use self::new::matches::{FormatOptions, MatchBounds, MatcherBuilder, MatchingWords};
use self::new::{execute_vector_search, PartialSearchResult, VectorStoreStats};
use crate::documents::GeoSortParameter;
use crate::dynamic_search_rules::{DsrFuel, DynamicSearchRules};
use crate::filterable_attributes_rules::{filtered_matching_patterns, matching_features};
use crate::index::MatchingStrategy;
use crate::progress::Progress;
use crate::score_details::{ScoreDetails, ScoringStrategy};
use crate::search::new::{
    extract_tokens, resolve_negative_phrases, resolve_negative_words, ExtractedTokens, QueryGraph,
};
use crate::vector::{Embedder, Embedding};
use crate::{
    execute_search, filtered_universe, AscDesc, Deadline, DefaultSearchLogger, DocumentId, Error,
    FieldsIdsMap, Index, Position, Result, SearchContext, SearchStep, UserError,
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
pub mod steps;

#[derive(Debug, Clone)]
pub struct SemanticSearch {
    vector: Option<Vec<f32>>,
    media: Option<serde_json::Value>,
    embedder_name: String,
    embedder: Arc<Embedder>,
    quantized: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinDoc {
    pub pos: Position,
    pub doc_id: DocumentId,
}

pub struct Search<'a> {
    query: Option<String>,
    // this should be linked to the String in the query
    filter: Option<IndexFilter>,
    offset: usize,
    limit: usize,
    sort_criteria: Option<Vec<AscDesc>>,
    distinct: Option<String>,
    searchable_attributes: Option<&'a [String]>,
    geo_param: GeoSortParameter,
    terms_matching_strategy: TermsMatchingStrategy,
    scoring_strategy: ScoringStrategy,
    words_limit: usize,
    retrieve_vectors: bool,
    exhaustive_number_hits: bool,
    max_total_hits: Option<usize>,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
    fields_ids_map: &'a FieldsIdsMap,
    index_uid: &'a str,
    before_search: OffsetDateTime,
    semantic: Option<SemanticSearch>,
    deadline: Deadline,
    ranking_score_threshold: Option<f64>,
    locales: Option<Vec<Language>>,
    progress: &'a Progress,
    dynamic_search_rules: Option<(&'a DynamicSearchRules, DsrFuel)>,
    candidates: Option<&'a RoaringBitmap>,
}

impl<'a> Search<'a> {
    pub fn new(
        rtxn: &'a heed::RoTxn<'a>,
        index: &'a Index,
        fields_ids_map: &'a FieldsIdsMap,
        index_uid: &'a str,
        before_search: OffsetDateTime,
        progress: &'a Progress,
    ) -> Search<'a> {
        Search {
            query: None,
            filter: None,
            offset: 0,
            limit: 20,
            sort_criteria: None,
            distinct: None,
            searchable_attributes: None,
            geo_param: GeoSortParameter::default(),
            terms_matching_strategy: TermsMatchingStrategy::default(),
            scoring_strategy: Default::default(),
            retrieve_vectors: false,
            exhaustive_number_hits: false,
            max_total_hits: None,
            words_limit: 10,
            rtxn,
            index,
            fields_ids_map,
            index_uid,
            before_search,
            semantic: None,
            locales: None,
            deadline: Deadline::never(),
            ranking_score_threshold: None,
            progress,
            dynamic_search_rules: None,
            candidates: None,
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
        quantized: bool,
        vector: Option<Embedding>,
        media: Option<serde_json::Value>,
    ) -> &mut Search<'a> {
        self.semantic = Some(SemanticSearch { embedder_name, embedder, quantized, vector, media });
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

    pub fn filter(&mut self, condition: Option<IndexFilter>) -> &mut Search<'a> {
        self.filter = condition;
        self
    }

    #[cfg(test)]
    pub fn geo_sort_strategy(&mut self, strategy: crate::GeoSortStrategy) -> &mut Search<'a> {
        self.geo_param.strategy = strategy;
        self
    }

    #[cfg(test)]
    pub fn geo_max_bucket_size(&mut self, max_size: u64) -> &mut Search<'a> {
        self.geo_param.max_bucket_size = max_size;
        self
    }

    pub fn retrieve_vectors(&mut self, retrieve_vectors: bool) -> &mut Search<'a> {
        self.retrieve_vectors = retrieve_vectors;
        self
    }

    /// Forces the search to exhaustively compute the number of candidates,
    /// this will increase the search time but allows finite pagination.
    pub fn exhaustive_number_hits(&mut self, exhaustive_number_hits: bool) -> &mut Search<'a> {
        self.exhaustive_number_hits = exhaustive_number_hits;
        self
    }

    pub fn max_total_hits(&mut self, max_total_hits: Option<usize>) -> &mut Search<'a> {
        self.max_total_hits = max_total_hits;
        self
    }

    pub fn deadline(&mut self, deadline: Deadline) -> &mut Search<'a> {
        self.deadline = deadline;
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

    pub fn dynamic_search_rules(
        &mut self,
        dynamic_search_rules: &'a DynamicSearchRules,
        fuel: DsrFuel,
    ) -> &mut Search<'a> {
        self.dynamic_search_rules = Some((dynamic_search_rules, fuel));
        self
    }

    /// Limit the results to **at most** candidates.
    ///
    /// If there is a specified filter, it is applied on top of the candidates.
    pub fn candidates(&mut self, candidates: &'a RoaringBitmap) -> &mut Search<'a> {
        self.candidates = Some(candidates);
        self
    }

    pub fn index_uid(&self) -> &'a str {
        self.index_uid
    }

    pub fn execute_for_candidates(&self, is_hybrid_kind: bool) -> Result<RoaringBitmap> {
        let has_vector = is_hybrid_kind || {
            self.semantic.as_ref().and_then(|semantic| semantic.vector.as_ref()).is_some()
        };

        if has_vector {
            let ctx = SearchContext::new(
                self.index,
                self.rtxn,
                &self.fields_ids_map,
                self.index_uid,
                self.before_search,
            )?;
            filtered_universe(ctx.index, ctx.txn, &self.filter, self.candidates, self.progress)
        } else {
            Ok(self.execute()?.candidates)
        }
    }

    pub fn execute(&self) -> Result<SearchResult> {
        let mut ctx = SearchContext::new(
            self.index,
            self.rtxn,
            &self.fields_ids_map,
            self.index_uid,
            self.before_search,
        )?;

        if let Some(searchable_attributes) = self.searchable_attributes {
            ctx.attributes_to_search_on(searchable_attributes)?;
        }

        if let Some(distinct) = &self.distinct {
            let filterable_fields = ctx.index.filterable_attributes_rules(ctx.txn)?;
            // check if the distinct field is in the filterable fields
            let matched_rule = matching_features(distinct, &filterable_fields);
            let is_filterable = matched_rule.is_some_and(|(_, features)| features.is_filterable());

            if !is_filterable {
                // if not, remove the hidden fields from the filterable fields to generate the error message
                let matching_patterns =
                    filtered_matching_patterns(&filterable_fields, &|features| {
                        features.is_filterable()
                    });
                let (valid_patterns, hidden_fields) =
                    ctx.index.remove_hidden_fields(ctx.txn, matching_patterns)?;

                // Get the matching rule index if any rule matched the attribute
                let matching_rule_index = matched_rule.map(|(rule_index, _)| rule_index);

                // and return the error
                return Err(Error::UserError(UserError::InvalidDistinctAttribute {
                    field: distinct.clone(),
                    valid_patterns,
                    hidden_fields,
                    matching_rule_index,
                }));
            }
        }

        let mut universe =
            filtered_universe(ctx.index, ctx.txn, &self.filter, self.candidates, self.progress)?;

        let (query_terms, pins, used_negative_operator) =
            self.build_located_query_terms(&mut ctx, &mut universe)?;

        let mut query_vector = None;
        let PartialSearchResult {
            located_query_terms,
            candidates,
            documents_ids,
            document_scores,
            degraded,
        } = match self.semantic.as_ref() {
            Some(SemanticSearch {
                vector: Some(vector),
                embedder_name,
                embedder,
                quantized,
                media: _,
            }) => {
                if self.retrieve_vectors {
                    query_vector = Some(vector.clone());
                }
                execute_vector_search(
                    &mut ctx,
                    vector,
                    self.scoring_strategy,
                    self.exhaustive_number_hits,
                    self.max_total_hits,
                    universe,
                    &self.sort_criteria,
                    &self.distinct,
                    self.geo_param,
                    self.offset,
                    self.limit,
                    embedder_name,
                    embedder,
                    *quantized,
                    self.deadline.clone(),
                    self.ranking_score_threshold,
                    self.progress,
                    pins,
                )?
            }
            _ => execute_search(
                &mut ctx,
                query_terms,
                self.terms_matching_strategy,
                self.scoring_strategy,
                self.exhaustive_number_hits,
                self.max_total_hits,
                universe,
                &self.sort_criteria,
                &self.distinct,
                self.geo_param,
                self.offset,
                self.limit,
                &mut DefaultSearchLogger,
                &mut DefaultSearchLogger,
                self.deadline.clone(),
                self.ranking_score_threshold,
                self.progress,
                pins,
            )?,
        };

        if let Some(VectorStoreStats { total_time, total_queries, total_results }) =
            ctx.vector_store_stats
        {
            tracing::debug!("Vector store stats: total_time={total_time:.02?}, total_queries={total_queries}, total_results={total_results}");
        }

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
            query_vector,
        })
    }

    pub fn build_located_query_terms(
        &self,
        ctx: &mut SearchContext<'_>,
        universe: &mut RoaringBitmap,
    ) -> Result<(Option<(QueryGraph, Vec<new::LocatedQueryTerm>)>, Vec<PinDoc>, bool), Error> {
        let mut used_negative_operator = false;

        let mut ignored = RoaringBitmap::new();

        let query_graph_terms =
            if let Some(query) = self.query.as_deref().filter(|q| !q.trim().is_empty()) {
                let _step = self.progress.update_progress_scoped(SearchStep::TokenizeQuery);

                let ExtractedTokens { query_terms, graph, negative_words, negative_phrases } =
                    extract_tokens(ctx, query, Some(self.words_limit), self.locales.as_ref())?;

                used_negative_operator = !negative_words.is_empty() || !negative_phrases.is_empty();

                ignored |= resolve_negative_words(ctx, Some(&*universe), &negative_words)?;
                ignored |= resolve_negative_phrases(ctx, &negative_phrases)?;

                if query_terms.is_empty() {
                    // Do a placeholder search instead
                    None
                } else {
                    Some((graph, query_terms))
                }
            } else {
                None
            };

        let pins = self
            .dynamic_search_rules
            .map(|(dsrs, fuel)| {
                dsrs.resolve_pins(
                    query_graph_terms.as_ref().map(|(_, terms)| terms.as_slice()).unwrap_or(&[]),
                    universe,
                    ctx,
                    fuel,
                )
            })
            .transpose()?
            .unwrap_or_default();

        *universe -= ignored;

        Ok((query_graph_terms, pins, used_negative_operator))
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
            geo_param: _,
            terms_matching_strategy,
            scoring_strategy,
            words_limit,
            retrieve_vectors,
            exhaustive_number_hits,
            max_total_hits,
            rtxn: _,
            index: _,
            fields_ids_map: _,
            index_uid: _,
            before_search: _,
            semantic,
            deadline,
            ranking_score_threshold,
            locales,
            candidates,
            progress: _,
            dynamic_search_rules: _,
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
            .field("retrieve_vectors", retrieve_vectors)
            .field("exhaustive_number_hits", exhaustive_number_hits)
            .field("max_total_hits", max_total_hits)
            .field("words_limit", words_limit)
            .field(
                "semantic.embedder_name",
                &semantic.as_ref().map(|semantic| &semantic.embedder_name),
            )
            .field("deadline", deadline)
            .field("ranking_score_threshold", ranking_score_threshold)
            .field("locales", locales)
            .field("candidates", candidates)
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
    pub query_vector: Option<Embedding>,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermsMatchingStrategy {
    // remove last word first
    #[default]
    Last,
    // all words are mandatory
    All,
    // remove more frequent word first
    Frequency,
}

impl From<MatchingStrategy> for TermsMatchingStrategy {
    fn from(other: MatchingStrategy) -> Self {
        match other {
            MatchingStrategy::Last => Self::Last,
            MatchingStrategy::All => Self::All,
            MatchingStrategy::Frequency => Self::Frequency,
        }
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

pub fn merge_positioned_hits_into_page<P, T, FPos, FMap>(
    pins: Vec<P>,
    skip: usize,
    take: usize,
    organic_hits: Vec<T>,
    pin_position: FPos,
    mut pin_into_hit: FMap,
) -> Vec<T>
where
    FPos: Fn(&P) -> Position,
    FMap: FnMut(P) -> T,
{
    if pins.is_empty() {
        return organic_hits;
    }

    let page_end = skip.saturating_add(take);
    let capacity = take.min(organic_hits.len().saturating_add(pins.len()));
    let mut merged_hits = Vec::with_capacity(capacity);
    let mut organic_hits = organic_hits.into_iter();
    let mut pins = pins.into_iter().peekable();
    let mut combined_index = 0usize;

    while combined_index < page_end {
        let next_hit = if let Some(pin) = pins.peek() {
            if (pin_position(pin) as usize) <= combined_index {
                Some(pin_into_hit(pins.next().expect("peeked pin must exist")))
            } else if let Some(hit) = organic_hits.next() {
                Some(hit)
            } else {
                Some(pin_into_hit(pins.next().expect("peeked pin must exist")))
            }
        } else {
            organic_hits.next()
        };

        let Some(hit) = next_hit else { break };

        if combined_index >= skip {
            merged_hits.push(hit);
        }

        combined_index += 1;
    }

    merged_hits
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;

    #[cfg(feature = "japanese")]
    #[cfg(not(feature = "chinese-pinyin"))]
    #[test]
    fn test_kanji_language_detection() {
        use crate::index::tests::TempIndex;
        let progress = Progress::default();

        let index = TempIndex::new();

        index
            .add_documents(documents!([
                { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
                { "id": 1, "title": "東京のお寿司。" },
                { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
            ]))
            .unwrap();

        let txn = index.write_txn().unwrap();
        let mut search = Search::new(&txn, &index, "test", OffsetDateTime::now_utc(), &progress);

        search.query("東京");
        let SearchResult { documents_ids, .. } = search.execute().unwrap();

        assert_eq!(documents_ids, vec![1]);
    }

    #[cfg(feature = "korean")]
    #[test]
    fn test_hangul_language_detection() {
        use crate::index::tests::TempIndex;
        let progress = Progress::default();

        let index = TempIndex::new();

        index
            .add_documents(documents!([
                { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
                { "id": 1, "title": "김밥먹을래。" },
                { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
            ]))
            .unwrap();

        let txn = index.write_txn().unwrap();
        let mut search = Search::new(&txn, &index, "test", OffsetDateTime::now_utc(), &progress);

        search.query("김밥");
        let SearchResult { documents_ids, .. } = search.execute().unwrap();

        assert_eq!(documents_ids, vec![1]);
    }
}

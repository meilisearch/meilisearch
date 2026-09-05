use std::cell::Cell;
use std::fmt;
use std::sync::Arc;

use charabia::Language;
use itertools::Itertools;
use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};
use once_cell::sync::Lazy;
use ordered_float::OrderedFloat;
use roaring::bitmap::RoaringBitmap;
use roaring::MultiOps;
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
use crate::score_details::{ScoreDetails, ScoringStrategy, WeightedScoreValue};
use crate::search::new::{
    distinct_fid, distinct_single_docid, extract_tokens, resolve_negative_phrases,
    resolve_negative_words, ExtractedTokens, QueryGraph,
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
    pub position: Position,
    pub precedence: Precedence,
    pub id: DocumentId,
}

impl Pin for PinDoc {
    type Id = DocumentId;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn position(&self) -> u32 {
        self.position
    }

    fn precedence(&self) -> Precedence {
        self.precedence
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScaleDocs {
    pub docs: RoaringBitmap,
    pub weight: f64,
}

pub trait Pin {
    type Id;
    fn id(&self) -> Self::Id;

    fn position(&self) -> u32;
    fn precedence(&self) -> Precedence;

    fn sort(pins: &mut Vec<Self>)
    where
        Self: Sized,
    {
        pins.sort_unstable_by_key(|item| (item.position(), item.precedence()));
    }

    fn dedup(pins: &mut Vec<Self>)
    where
        Self: Sized,
        Self::Id: PartialEq + PartialOrd + Ord,
    {
        pins.sort_unstable_by_key(|item| (item.id(), item.precedence(), item.position()));
        pins.dedup_by_key(|item| item.id());
    }

    fn dedup_and_sort(pins: &mut Vec<Self>)
    where
        Self: Sized,
        Self::Id: PartialEq + PartialOrd + Ord,
    {
        Self::dedup(pins);
        Self::sort(pins);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Precedence(pub Option<u64>);

impl PartialOrd for Precedence {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Overrides the natural order of Option such that None is considered greater than Some rather than Less
///
/// When both options are Some, use the regular order.
impl Ord for Precedence {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        match (self.0, other.0) {
            (None, None) => Equal,
            (None, Some(_)) => Greater,
            (Some(_), None) => Less,
            (Some(left), Some(right)) => left.cmp(&right),
        }
    }
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
                self.fields_ids_map,
                self.index_uid,
                self.before_search,
            )?;
            filtered_universe(
                ctx.index,
                ctx.txn,
                self.fields_ids_map,
                &self.filter,
                self.candidates,
                self.progress,
            )
        } else {
            Ok(self.execute()?.candidates)
        }
    }

    pub fn execute(&self) -> Result<SearchResult> {
        let mut ctx = SearchContext::new(
            self.index,
            self.rtxn,
            self.fields_ids_map,
            self.index_uid,
            self.before_search,
        )?;

        if let Some(searchable_attributes) = self.searchable_attributes {
            ctx.attributes_to_search_on(searchable_attributes)?;
        }

        let query_vector =
            self.semantic.as_ref().and_then(|semantic| semantic.vector.as_ref()).cloned();

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

        let mut universe = filtered_universe(
            ctx.index,
            ctx.txn,
            self.fields_ids_map,
            &self.filter,
            self.candidates,
            self.progress,
        )?;

        let ResolvedQuery { query_graph_terms, pins, mut scales, used_negative_operator } =
            self.resolve_query(&mut ctx, self.filter.as_ref(), &mut universe)?;

        let (query_graph, located_query_terms) = query_graph_terms.unzip();

        // remove 0-weight scale operations (hide operations), removing corresponding documents from the universe
        scales.retain(|scale| {
            if scale.weight != 0.0 {
                true
            } else {
                universe -= &scale.docs;
                false
            }
        });

        // whether we can use the original (limit, offset) or if we need to turn it into (limit+offset, 0).
        // whenever there are pins or scales, we'll need to merge multiple lists of results
        // and we don't know the actual position of hits before merging, so we cannot skip `offset`.

        let can_skip_hits_internally = pins.is_empty() && scales.is_empty();

        let PartialSearchResult { candidates, documents_ids, document_scores, degraded } =
            if can_skip_hits_internally {
                // fixme: repeated in `else` case because we don't have if let chains before edition 2024
                self.execute_single_search(
                    &mut ctx,
                    universe,
                    query_graph.as_ref(),
                    can_skip_hits_internally,
                )?
            } else if let Some(mut fuel) = self.dynamic_search_rules.as_ref().map(|(_, fuel)| *fuel)
            {
                let mut partial_results = Vec::new();
                'scales: for k in (1..=(scales.len())).rev() {
                    for combination in scales.iter().combinations(k) {
                        let scale_universe =
                            MultiOps::intersection(combination.iter().map(|c| &c.docs)) & &universe;

                        let OrderedFloat::<f64>(weight) = combination
                            .iter()
                            .map(|scale| ordered_float::OrderedFloat(scale.weight))
                            .product();

                        let partial_result: PartialSearchResult = self.execute_single_search(
                            &mut ctx,
                            scale_universe,
                            query_graph.as_ref(),
                            can_skip_hits_internally,
                        )?;
                        universe -= &partial_result.candidates;
                        let degraded = partial_result.degraded;
                        partial_results.push((partial_result, weight));
                        if fuel.consume_scale_combination().is_break() {
                            break 'scales;
                        }
                        if degraded || universe.is_empty() {
                            break 'scales;
                        }
                    }
                }

                if !universe.is_empty() {
                    let partial_result = self.execute_single_search(
                        &mut ctx,
                        universe,
                        query_graph.as_ref(),
                        can_skip_hits_internally,
                    )?;

                    partial_results.push((partial_result, 1.0));
                }

                self.merge_partial_results(&ctx, partial_results, pins)?
            } else {
                // fixme: repeated from first `if` case because we don't have if let chains before edition 2024
                self.execute_single_search(
                    &mut ctx,
                    universe,
                    query_graph.as_ref(),
                    can_skip_hits_internally,
                )?
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

    fn execute_single_search(
        &self,
        ctx: &mut SearchContext<'_>,
        universe: RoaringBitmap,
        query_graph: Option<&QueryGraph>,
        can_skip: bool,
    ) -> Result<PartialSearchResult, Error> {
        let limit = if can_skip { self.limit } else { self.limit + self.offset };
        let offset = if can_skip { self.offset } else { 0 };

        match self.semantic.as_ref() {
            Some(SemanticSearch {
                vector: Some(vector),
                embedder_name,
                embedder,
                quantized,
                media: _,
            }) => execute_vector_search(
                ctx,
                vector,
                self.scoring_strategy,
                self.exhaustive_number_hits,
                self.max_total_hits,
                universe,
                &self.sort_criteria,
                &self.distinct,
                self.geo_param,
                offset,
                limit,
                embedder_name,
                embedder,
                *quantized,
                self.deadline.clone(),
                self.ranking_score_threshold,
                self.progress,
            ),
            _ => execute_search(
                ctx,
                query_graph,
                self.terms_matching_strategy,
                self.scoring_strategy,
                self.exhaustive_number_hits,
                self.max_total_hits,
                universe,
                &self.sort_criteria,
                &self.distinct,
                self.geo_param,
                offset,
                limit,
                &mut DefaultSearchLogger,
                &mut DefaultSearchLogger,
                self.deadline.clone(),
                self.ranking_score_threshold,
                self.progress,
            ),
        }
    }

    pub fn resolve_query(
        &self,
        ctx: &mut SearchContext<'_>,
        filter: Option<&IndexFilter>,
        universe: &mut RoaringBitmap,
    ) -> Result<ResolvedQuery, Error> {
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

        let (pins, scales) = self
            .dynamic_search_rules
            .map(|(dsrs, fuel)| {
                dsrs.resolve_actions(
                    query_graph_terms.as_ref().map(|(_, terms)| terms.as_slice()).unwrap_or(&[]),
                    filter,
                    universe,
                    ctx,
                    fuel,
                )
            })
            .transpose()?
            .unwrap_or_default();

        *universe -= ignored;

        Ok(ResolvedQuery { query_graph_terms, pins, scales, used_negative_operator })
    }

    /// Merge the hits in `partial_results` depending on their weight and score details, inject pins, and produce
    /// a `PartialSearchResult` with the merged hits and merged metadata
    ///
    /// also reapplies distinct because any sharded application must happen here again
    fn merge_partial_results(
        &self,
        ctx: &SearchContext<'_>,
        partial_results: Vec<(PartialSearchResult, f64)>,
        pins: Vec<PinDoc>,
    ) -> Result<PartialSearchResult> {
        let distinct_fid =
            distinct_fid(self.distinct.as_deref(), ctx.index, ctx.txn, ctx.fields_ids_map)?;

        let mut candidates =
            MultiOps::union(partial_results.iter().map(|(result, _)| &result.candidates));

        let degraded = partial_results.iter().any(|(result, _)| result.degraded);

        let mut indistinct = RoaringBitmap::new();
        let organic_it = itertools::kmerge_by(
            partial_results.into_iter().map(|(result, weight)| {
                result
                    .documents_ids
                    .into_iter()
                    .zip(result.document_scores)
                    .zip(std::iter::repeat(weight))
            }),
            |left: &((u32, Vec<ScoreDetails>), f64), right: &((u32, Vec<ScoreDetails>), f64)| {
                let ((_, left_scores), left_weight) = left;
                let ((_, right_scores), right_weight) = right;

                let left = ScoreDetails::weighted_score_values(left_scores.iter(), *left_weight);
                let right = ScoreDetails::weighted_score_values(right_scores.iter(), *right_weight);

                // is_ge because the greater score goes first
                WeightedScoreValue::compare_partial(left, right).is_some_and(|i| i.is_ge())
            },
        )
        .filter_map(|((id, mut scores), weight)| {
            // rejects indistinct docs
            if indistinct.contains(id) {
                return None;
            }

            // populate indistinct docs
            if let Some(distinct_fid) = &distinct_fid {
                if let Err(err) =
                    distinct_single_docid(ctx.index, ctx.txn, *distinct_fid, id, &mut indistinct)
                {
                    return Some(Err(err));
                }
            }

            // insert the scale detail: this will be used by federated search to take the weight into account when comparing scores
            scores.insert(0, ScoreDetails::Scale { weight });
            Some(Ok((id, scores)))
        });

        // any successful addition to the list of documents will increment the rank
        // we use the rank to decide whether we should take a pin or an organic result.
        let rank = Cell::new(0u32);

        let doc_it = itertools::merge_join_by(organic_it, pins.iter(), |_, right| {
            right.position > rank.get()
        })
        .map(|doc| {
            rank.update(|rank| rank + 1);

            match doc {
                either::Either::Left(result) => result,
                either::Either::Right(PinDoc {
                    position,
                    precedence: Precedence(precedence),
                    id,
                }) => Ok((
                    *id,
                    vec![ScoreDetails::Pin { position: *position, precedence: *precedence }],
                )),
            }
        });

        let res: Result<(Vec<_>, Vec<_>), _> = doc_it.skip(self.offset).take(self.limit).collect();

        // remove excluded candidates from universe
        // this must be done after `organic_it` is consumed to prevent the double borrow of `indistinct` (and make sure it has its last value)
        candidates -= &indistinct;

        // pinned docs are always in candidates
        //
        // order matters: pinned docs could be indistinct docs, we decide to always pin them anyway
        // should we decide differently, we should reject indistinct docs in `doc_it` to also reject pins,
        // and add the pins to candidates before removing indistinct docs.
        for PinDoc { id, position: _, precedence: _ } in &pins {
            candidates.insert(*id);
        }

        let (documents_ids, document_scores) = res?;

        Ok(PartialSearchResult { candidates, documents_ids, document_scores, degraded })
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

pub struct ResolvedQuery {
    pub query_graph_terms: Option<(QueryGraph, Vec<new::LocatedQueryTerm>)>,
    pub pins: Vec<PinDoc>,
    pub scales: Vec<ScaleDocs>,
    pub used_negative_operator: bool,
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
    pin_count: usize,
    pins: impl IntoIterator<Item = P>,
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
    if pin_count == 0 {
        return organic_hits;
    }

    let page_end = skip.saturating_add(take);
    let capacity = take.min(organic_hits.len().saturating_add(pin_count));
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
        let fields_ids_map = index.fields_ids_map(&txn).unwrap();
        let mut search = Search::new(
            &txn,
            &index,
            &fields_ids_map,
            "test",
            OffsetDateTime::now_utc(),
            &progress,
        );

        search.query("김밥");
        let SearchResult { documents_ids, .. } = search.execute().unwrap();

        assert_eq!(documents_ids, vec![1]);
    }
}

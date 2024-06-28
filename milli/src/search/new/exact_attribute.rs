use heed::types::Bytes;
use roaring::{MultiOps, RoaringBitmap};

use super::query_graph::QueryGraph;
use super::ranking_rules::{RankingRule, RankingRuleOutput};
use crate::score_details::{self, ScoreDetails};
use crate::search::new::query_graph::QueryNodeData;
use crate::search::new::query_term::ExactTerm;
use crate::{CboRoaringBitmapCodec, Result, SearchContext, SearchLogger};

/// A ranking rule that produces 3 disjoint buckets:
///
/// 1. Documents from the universe whose value is exactly the query.
/// 2. Documents from the universe not in (1) whose value starts with the query.
/// 3. Documents from the universe not in (1) or (2).
pub struct ExactAttribute {
    state: State,
}

impl ExactAttribute {
    pub fn new() -> Self {
        Self { state: Default::default() }
    }
}

impl<'ctx> RankingRule<'ctx, QueryGraph> for ExactAttribute {
    fn id(&self) -> String {
        "exact_attribute".to_owned()
    }

    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &roaring::RoaringBitmap,
        query: &QueryGraph,
    ) -> Result<()> {
        self.state = State::start_iteration(ctx, universe, query)?;
        Ok(())
    }

    fn next_bucket(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &roaring::RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<QueryGraph>>> {
        let state = std::mem::take(&mut self.state);
        let (state, output) = State::next(state, universe);
        self.state = state;

        Ok(output)
    }

    fn end_iteration(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        self.state = Default::default();
    }
}

/// Inner state of the ranking rule.
#[derive(Default)]
enum State {
    /// State between two iterations
    #[default]
    Uninitialized,
    /// The next call to `next` will output the documents in the universe that have an attribute that is the exact query
    ExactAttribute(QueryGraph, Vec<FieldCandidates>),
    /// The next call to `next` will output the documents in the universe that have an attribute that starts with the exact query,
    /// but isn't the exact query.
    AttributeStarts(QueryGraph, Vec<FieldCandidates>),
    /// The next calls to `next` will output the input universe.
    Empty(QueryGraph),
}

/// The candidates sorted by attributes
///
/// Each of the bitmap in a single `FieldCandidates` struct applies to the same field.
struct FieldCandidates {
    /// The candidates that start with all the words of the query in the field
    start_with_exact: RoaringBitmap,
    /// The candidates that have the same number of words as the query in the field
    exact_word_count: RoaringBitmap,
}

impl State {
    fn start_iteration(
        ctx: &mut SearchContext<'_>,
        universe: &RoaringBitmap,
        query_graph: &QueryGraph,
    ) -> Result<Self> {
        struct ExactTermInfo {
            exact_term: ExactTerm,
            start_position: u16,
            start_term_id: u8,
            position_count: usize,
        }

        let mut exact_terms: Vec<ExactTermInfo> =
            Vec::with_capacity(query_graph.nodes.len() as usize);
        for (_, node) in query_graph.nodes.iter() {
            match &node.data {
                QueryNodeData::Term(term) => {
                    let exact_term = if let Some(exact_term) = term.term_subset.exact_term(ctx) {
                        exact_term
                    } else {
                        continue;
                    };
                    exact_terms.push(ExactTermInfo {
                        exact_term,
                        start_position: *term.positions.start(),
                        start_term_id: *term.term_ids.start(),
                        position_count: term.positions.len(),
                    });
                }
                QueryNodeData::Deleted | QueryNodeData::Start | QueryNodeData::End => continue,
            }
        }

        exact_terms.sort_by_key(|x| x.start_term_id);
        exact_terms.dedup_by_key(|x| x.start_term_id);
        let count_all_positions = exact_terms.iter().fold(0, |acc, x| acc + x.position_count);

        // bail if there is a "hole" (missing word) in remaining query graph
        if let Some(e) = exact_terms.first() {
            if e.start_term_id != 0 {
                return Ok(State::Empty(query_graph.clone()));
            }
        } else {
            return Ok(State::Empty(query_graph.clone()));
        }
        let mut previous_id = 0;
        for e in exact_terms.iter() {
            if e.start_term_id < previous_id || e.start_term_id - previous_id > 1 {
                return Ok(State::Empty(query_graph.clone()));
            } else {
                previous_id = e.start_term_id;
            }
        }

        // sample query: "sunflower are pretty"
        // sunflower at pos 0 in attr A
        // are at pos 1 in attr B
        // pretty at pos 2 in attr C
        // We want to eliminate such document

        // first check that for each term, there exists some attribute that has this term at the correct position
        //"word-position-docids";
        let mut candidates = universe.clone();
        let words_positions: Vec<(Vec<_>, _)> = exact_terms
            .iter()
            .map(|e| (e.exact_term.interned_words(ctx).collect(), e.start_position))
            .collect();
        for (words, position) in &words_positions {
            if candidates.is_empty() {
                return Ok(State::Empty(query_graph.clone()));
            }

            'words: for (offset, word) in words.iter().enumerate() {
                let offset = offset as u16;
                let word = if let Some(word) = word {
                    word
                } else {
                    continue 'words;
                };
                // Note: Since the position is stored bucketed in word_position_docids, for queries with a lot of
                // longer phrases we'll be losing on precision here.
                let bucketed_position = crate::bucketed_position(position + offset);
                let word_position_docids = ctx
                    .get_db_word_position_docids(Some(universe), *word, bucketed_position)?
                    .unwrap_or_default();
                candidates &= word_position_docids;
                if candidates.is_empty() {
                    return Ok(State::Empty(query_graph.clone()));
                }
            }
        }

        let candidates = candidates;

        if candidates.is_empty() {
            return Ok(State::Empty(query_graph.clone()));
        }

        let searchable_fields_ids = ctx.index.searchable_fields_ids(ctx.txn)?;

        let mut candidates_per_attribute = Vec::with_capacity(searchable_fields_ids.len());
        // then check that there exists at least one attribute that has all of the terms
        for fid in searchable_fields_ids {
            let mut intersection = MultiOps::intersection(
                words_positions
                    .iter()
                    .flat_map(|(words, ..)| words.iter())
                    // ignore stop words words in phrases
                    .flatten()
                    .map(|word| -> Result<_> {
                        Ok(ctx
                            .get_db_word_fid_docids(Some(universe), *word, fid)?
                            .unwrap_or_default())
                    }),
            )?;
            // TODO Why not doing this intersection in the MultiOps above?
            intersection &= &candidates;
            if !intersection.is_empty() {
                // Although not really worth it in terms of performance,
                // if would be good to put this in cache for the sake of consistency
                let candidates_with_exact_word_count = if count_all_positions < u8::MAX as usize {
                    let bitmap_bytes = ctx
                        .index
                        .field_id_word_count_docids
                        .remap_data_type::<Bytes>()
                        .get(ctx.txn, &(fid, count_all_positions as u8))?;

                    match bitmap_bytes {
                        Some(bytes) => {
                            CboRoaringBitmapCodec::intersection_with_serialized(bytes, universe)?
                        }
                        None => RoaringBitmap::default(),
                    }
                } else {
                    RoaringBitmap::default()
                };
                candidates_per_attribute.push(FieldCandidates {
                    start_with_exact: intersection,
                    exact_word_count: candidates_with_exact_word_count,
                });
            }
        }
        // note we could have "false positives" where there both exist different attributes that collectively
        // have the terms in the correct order and a single attribute that have all the terms, but in the incorrect order.

        Ok(State::ExactAttribute(query_graph.clone(), candidates_per_attribute))
    }

    fn next(
        state: State,
        universe: &RoaringBitmap,
    ) -> (State, Option<RankingRuleOutput<QueryGraph>>) {
        let (state, output) = match state {
            State::Uninitialized => (state, None),
            State::ExactAttribute(query_graph, candidates_per_attribute) => {
                // TODO it can be much faster to do the intersections before the unions...
                //      or maybe the candidates_per_attribute are not containing anything outside universe
                let mut candidates = MultiOps::union(candidates_per_attribute.iter().map(
                    |FieldCandidates { start_with_exact, exact_word_count }| {
                        start_with_exact & exact_word_count
                    },
                ));
                candidates &= universe;
                (
                    State::AttributeStarts(query_graph.clone(), candidates_per_attribute),
                    Some(RankingRuleOutput {
                        query: query_graph,
                        candidates,
                        score: ScoreDetails::ExactAttribute(
                            score_details::ExactAttribute::ExactMatch,
                        ),
                    }),
                )
            }
            State::AttributeStarts(query_graph, candidates_per_attribute) => {
                // TODO it can be much faster to do the intersections before the unions...
                //      or maybe the candidates_per_attribute are not containing anything outside universe
                let mut candidates = MultiOps::union(candidates_per_attribute.into_iter().map(
                    |FieldCandidates { mut start_with_exact, exact_word_count }| {
                        start_with_exact -= exact_word_count;
                        start_with_exact
                    },
                ));
                candidates &= universe;
                (
                    State::Empty(query_graph.clone()),
                    Some(RankingRuleOutput {
                        query: query_graph,
                        candidates,
                        score: ScoreDetails::ExactAttribute(
                            score_details::ExactAttribute::MatchesStart,
                        ),
                    }),
                )
            }
            State::Empty(query_graph) => (
                State::Empty(query_graph.clone()),
                Some(RankingRuleOutput {
                    query: query_graph,
                    candidates: universe.clone(),
                    score: ScoreDetails::ExactAttribute(
                        score_details::ExactAttribute::NoExactMatch,
                    ),
                }),
            ),
        };
        (state, output)
    }
}

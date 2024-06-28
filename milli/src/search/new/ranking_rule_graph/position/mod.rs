use fxhash::{FxHashMap, FxHashSet};
use roaring::RoaringBitmap;

use super::{ComputedCondition, RankingRuleGraphTrait};
use crate::score_details::{Rank, ScoreDetails};
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids_within_position;
use crate::search::new::SearchContext;
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PositionCondition {
    term: LocatedQueryTermSubset,
    positions: Vec<u16>,
}

pub enum PositionGraph {}

impl RankingRuleGraphTrait for PositionGraph {
    type Condition = PositionCondition;

    fn resolve_condition(
        ctx: &mut SearchContext,
        condition: &Self::Condition,
        universe: &RoaringBitmap,
    ) -> Result<ComputedCondition> {
        let PositionCondition { term, positions } = condition;
        let mut docids = RoaringBitmap::new();
        // TODO use MultiOps to do the big union
        for position in positions {
            // maybe compute_query_term_subset_docids_within_position should accept a universe as argument
            docids |= compute_query_term_subset_docids_within_position(
                ctx,
                Some(universe),
                &term.term_subset,
                *position,
            )?;
        }
        Ok(ComputedCondition {
            docids,
            universe_len: universe.len(),
            start_term_subset: None,
            end_term_subset: term.clone(),
        })
    }

    fn build_edges(
        ctx: &mut SearchContext,
        conditions_interner: &mut DedupInterner<Self::Condition>,
        _from: Option<&LocatedQueryTermSubset>,
        to_term: &LocatedQueryTermSubset,
    ) -> Result<Vec<(u32, Interned<Self::Condition>)>> {
        let term = to_term;

        let mut all_positions = FxHashSet::default();
        for word in term.term_subset.all_single_words_except_prefix_db(ctx)? {
            let positions = ctx.get_db_word_positions(word.interned())?;
            all_positions.extend(positions);
        }

        for phrase in term.term_subset.all_phrases(ctx)? {
            // Only check the position of the first word in the phrase
            // this is not correct, but it is the best we can do, since
            // it is difficult/impossible to know the expected position
            // of a word in a phrase.
            // There is probably a more correct way to do it though.
            if let Some(word) = phrase.words(ctx).iter().flatten().next() {
                let positions = ctx.get_db_word_positions(*word)?;
                all_positions.extend(positions);
            }
        }

        if let Some(word_prefix) = term.term_subset.use_prefix_db(ctx) {
            let positions = ctx.get_db_word_prefix_positions(word_prefix.interned())?;
            all_positions.extend(positions);
        }

        let mut positions_for_costs = FxHashMap::<u32, Vec<u16>>::default();

        for position in all_positions {
            // FIXME: bucketed position???
            let distance = position.abs_diff(*term.positions.start());
            let cost = {
                let mut cost = 0;
                for i in 0..term.term_ids.len() {
                    // This is actually not fully correct and slightly penalises ngrams unfairly.
                    // Because if two words are in the same bucketed position (e.g. 32) and consecutive,
                    // then their position cost will be 32+32=64, but an ngram of these two words at the
                    // same position will have a cost of 32+32+1=65
                    cost += cost_from_distance(distance as u32 + i as u32);
                }
                cost
            };
            positions_for_costs.entry(cost).or_default().push(position);
        }

        let max_cost = term.term_ids.len() as u32 * 10;
        let max_cost_exists = positions_for_costs.contains_key(&max_cost);

        let mut edges = vec![];
        for (cost, positions) in positions_for_costs {
            edges.push((
                cost,
                conditions_interner.insert(PositionCondition { term: term.clone(), positions }),
            ));
        }

        if !max_cost_exists {
            // artificial empty condition for computing max cost
            edges.push((
                max_cost,
                conditions_interner
                    .insert(PositionCondition { term: term.clone(), positions: Vec::default() }),
            ));
        }

        Ok(edges)
    }

    fn rank_to_score(rank: Rank) -> ScoreDetails {
        ScoreDetails::Position(rank)
    }
}

fn cost_from_distance(distance: u32) -> u32 {
    match distance {
        0 => 0,
        1 => 1,
        2..=4 => 2,
        5..=7 => 3,
        8..=11 => 4,
        12..=16 => 5,
        17..=24 => 6,
        25..=64 => 7,
        65..=256 => 8,
        257..=1024 => 9,
        _ => 10,
    }
}

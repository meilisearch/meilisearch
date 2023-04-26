use fxhash::FxHashSet;
use roaring::RoaringBitmap;

use super::{ComputedCondition, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids_within_position;
use crate::search::new::SearchContext;
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PositionCondition {
    term: LocatedQueryTermSubset,
    position: u16,
}

pub enum PositionGraph {}

impl RankingRuleGraphTrait for PositionGraph {
    type Condition = PositionCondition;

    fn resolve_condition(
        ctx: &mut SearchContext,
        condition: &Self::Condition,
        universe: &RoaringBitmap,
    ) -> Result<ComputedCondition> {
        let PositionCondition { term, .. } = condition;
        // maybe compute_query_term_subset_docids_within_position_id should accept a universe as argument
        let mut docids = compute_query_term_subset_docids_within_position(
            ctx,
            &term.term_subset,
            condition.position,
        )?;
        docids &= universe;

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

        let mut edges = vec![];
        for position in all_positions {
            let cost = {
                let mut cost = 0;
                for i in 0..term.term_ids.len() {
                    // This is actually not fully correct and slightly penalises ngrams unfairly.
                    // Because if two words are in the same bucketed position (e.g. 32) and consecutive,
                    // then their position cost will be 32+32=64, but an ngram of these two words at the
                    // same position will have a cost of 32+32+1=65
                    cost += position as u32 + i as u32;
                }
                cost
            };

            // TODO: We can improve performances and relevancy by storing
            //       the term subsets associated to each position fetched.
            edges.push((
                cost,
                conditions_interner.insert(PositionCondition {
                    term: term.clone(), // TODO remove this ugly clone
                    position,
                }),
            ));
        }

        Ok(edges)
    }
}

use roaring::RoaringBitmap;

use super::{ComputedCondition, DeadEndsCache, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interned, MappedInterner};
use crate::search::new::query_graph::{QueryGraph, QueryNode};
use crate::search::new::query_term::{ExactTerm, LocatedQueryTermSubset};
use crate::{Result, SearchContext, SearchLogger};

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ExactnessCondition {
    ExactInAttribute(LocatedQueryTermSubset),
    Skip(LocatedQueryTermSubset),
}

pub enum ExactnessGraph {}

fn compute_docids(
    ctx: &mut SearchContext,
    dest_node: &LocatedQueryTermSubset,
    universe: &RoaringBitmap,
) -> Result<RoaringBitmap> {
    let exact_term = if let Some(exact_term) = dest_node.term_subset.exact_term(ctx) {
        exact_term
    } else {
        return Ok(Default::default());
    };
    let mut candidates = match exact_term {
        ExactTerm::Phrase(phrase) => ctx.get_phrase_docids(phrase)?.clone(),
        ExactTerm::Word(word) => {
            if let Some(word_candidates) = ctx.get_db_word_docids(word)? {
                word_candidates
            } else {
                return Ok(Default::default());
            }
        }
    };
    // TODO: synonyms?
    candidates &= universe;
    Ok(candidates)
}

impl RankingRuleGraphTrait for ExactnessGraph {
    type Condition = ExactnessCondition;

    fn resolve_condition(
        ctx: &mut SearchContext,
        condition: &Self::Condition,
        universe: &RoaringBitmap,
    ) -> Result<ComputedCondition> {
        let (docids, dest_node) = match condition {
            ExactnessCondition::ExactInAttribute(dest_node) => {
                (compute_docids(ctx, dest_node, universe)?, dest_node)
            }
            ExactnessCondition::Skip(dest_node) => (universe.clone(), dest_node),
        };
        Ok(ComputedCondition {
            docids,
            universe_len: universe.len(),
            start_term_subset: None,
            // TODO/FIXME:  modify `end_term_subset` to signal to the next ranking rules that the term cannot be removed
            end_term_subset: dest_node.clone(),
        })
    }

    fn build_edges(
        _ctx: &mut SearchContext,
        conditions_interner: &mut DedupInterner<Self::Condition>,
        _source_node: Option<&LocatedQueryTermSubset>,
        dest_node: &LocatedQueryTermSubset,
    ) -> Result<Vec<(u32, Interned<Self::Condition>)>> {
        let exact_condition = ExactnessCondition::ExactInAttribute(dest_node.clone());
        let exact_condition = conditions_interner.insert(exact_condition);

        let skip_condition = ExactnessCondition::Skip(dest_node.clone());
        let skip_condition = conditions_interner.insert(skip_condition);

        Ok(vec![(0, exact_condition), (dest_node.term_ids.len() as u32, skip_condition)])
    }

    fn log_state(
        _graph: &RankingRuleGraph<Self>,
        _paths: &[Vec<Interned<Self::Condition>>],
        _dead_ends_cache: &DeadEndsCache<Self::Condition>,
        _niverse: &RoaringBitmap,
        _costs: &MappedInterner<QueryNode, Vec<u64>>,
        _cost: u64,
        _logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
    }

    fn label_for_condition(
        _ctx: &mut SearchContext,
        condition: &Self::Condition,
    ) -> Result<String> {
        Ok(match condition {
            ExactnessCondition::ExactInAttribute(_) => "exact",
            ExactnessCondition::Skip(_) => "skip",
        }
        .to_owned())
    }
}

pub mod build;
pub mod compute_docids;

use roaring::RoaringBitmap;

use super::{ComputedCondition, DeadEndsCache, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interned, MappedInterner};
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ProximityCondition {
    Uninit { left_term: LocatedQueryTermSubset, right_term: LocatedQueryTermSubset, cost: u8 },
    Term { term: LocatedQueryTermSubset },
}

pub enum ProximityGraph {}

impl RankingRuleGraphTrait for ProximityGraph {
    type Condition = ProximityCondition;

    fn resolve_condition(
        ctx: &mut SearchContext,
        condition: &Self::Condition,
        universe: &RoaringBitmap,
    ) -> Result<ComputedCondition> {
        compute_docids::compute_docids(ctx, condition, universe)
    }

    fn build_edges(
        ctx: &mut SearchContext,
        conditions_interner: &mut DedupInterner<Self::Condition>,
        source_term: Option<&LocatedQueryTermSubset>,
        dest_term: &LocatedQueryTermSubset,
    ) -> Result<Vec<(u32, Interned<Self::Condition>)>> {
        build::build_edges(ctx, conditions_interner, source_term, dest_term)
    }

    fn log_state(
        graph: &RankingRuleGraph<Self>,
        paths: &[Vec<Interned<ProximityCondition>>],
        dead_ends_cache: &DeadEndsCache<Self::Condition>,
        universe: &RoaringBitmap,
        distances: &MappedInterner<QueryNode, Vec<u64>>,
        cost: u64,
        logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        logger.log_proximity_state(graph, paths, dead_ends_cache, universe, distances, cost);
    }

    fn label_for_condition(ctx: &mut SearchContext, condition: &Self::Condition) -> Result<String> {
        match condition {
            ProximityCondition::Uninit { cost, .. } => {
                //  TODO
                Ok(format!("{cost}: cost"))
            }
            ProximityCondition::Term { term } => {
                let original_term = ctx.term_interner.get(term.term_subset.original);
                let original_word = ctx.word_interner.get(original_term.original);
                Ok(format!("{original_word} : exists"))
            }
        }
    }
}

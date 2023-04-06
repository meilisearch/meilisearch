pub mod build;
pub mod compute_docids;

use roaring::RoaringBitmap;

use super::{ComputedCondition, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::SearchContext;
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
}

use std::marker::PhantomData;

use fxhash::FxHashMap;
use roaring::RoaringBitmap;

use super::{ComputedCondition, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::Interned;
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::SearchContext;
use crate::Result;

/// A cache storing the document ids associated with each ranking rule edge
pub struct ConditionDocIdsCache<G: RankingRuleGraphTrait> {
    pub cache: FxHashMap<Interned<G::Condition>, ComputedCondition>,
    _phantom: PhantomData<G>,
}
impl<G: RankingRuleGraphTrait> Default for ConditionDocIdsCache<G> {
    fn default() -> Self {
        Self { cache: Default::default(), _phantom: Default::default() }
    }
}
impl<G: RankingRuleGraphTrait> ConditionDocIdsCache<G> {
    pub fn get_subsets_used_by_condition(
        &mut self,
        interned_condition: Interned<G::Condition>,
    ) -> (&Option<LocatedQueryTermSubset>, &LocatedQueryTermSubset) {
        let c = &self.cache[&interned_condition];
        (&c.start_term_subset, &c.end_term_subset)
    }
    /// Retrieve the document ids for the given edge condition.
    ///
    /// If the cache does not yet contain these docids, they are computed
    /// and inserted in the cache.
    pub fn get_computed_condition<'s>(
        &'s mut self,
        ctx: &mut SearchContext<'_>,
        interned_condition: Interned<G::Condition>,
        graph: &mut RankingRuleGraph<G>,
        universe: &RoaringBitmap,
    ) -> Result<&'s ComputedCondition> {
        if self.cache.contains_key(&interned_condition) {
            let computed = self.cache.get_mut(&interned_condition).unwrap();
            if computed.universe_len == universe.len() {
                return Ok(computed);
            } else {
                computed.docids &= universe;
                computed.universe_len = universe.len();
                return Ok(computed);
            }
        }
        let condition = graph.conditions_interner.get_mut(interned_condition);
        let computed = G::resolve_condition(ctx, condition, universe)?;
        // Can we put an assert here for computed.universe_len == universe.len() ?
        let _ = self.cache.insert(interned_condition, computed);
        let computed = &self.cache[&interned_condition];
        Ok(computed)
    }
}

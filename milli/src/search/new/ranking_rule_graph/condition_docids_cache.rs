use std::marker::PhantomData;

use fxhash::FxHashMap;
use roaring::RoaringBitmap;

use super::{RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::Interned;
use crate::search::new::SearchContext;
use crate::Result;

/// A cache storing the document ids associated with each ranking rule edge
pub struct ConditionDocIdsCache<G: RankingRuleGraphTrait> {
    // TODO: should be FxHashMap<Interned<EdgeCondition>, RoaringBitmap>
    pub cache: FxHashMap<Interned<G::Condition>, RoaringBitmap>,
    _phantom: PhantomData<G>,
}
impl<G: RankingRuleGraphTrait> Default for ConditionDocIdsCache<G> {
    fn default() -> Self {
        Self { cache: Default::default(), _phantom: Default::default() }
    }
}
impl<G: RankingRuleGraphTrait> ConditionDocIdsCache<G> {
    /// Retrieve the document ids for the given edge condition.
    ///
    /// If the cache does not yet contain these docids, they are computed
    /// and inserted in the cache.
    pub fn get_condition_docids<'s, 'ctx>(
        &'s mut self,
        ctx: &mut SearchContext<'ctx>,
        interned_condition: Interned<G::Condition>,
        graph: &RankingRuleGraph<G>,
        // TODO: maybe universe doesn't belong here
        universe: &RoaringBitmap,
    ) -> Result<&'s RoaringBitmap> {
        if self.cache.contains_key(&interned_condition) {
            // TODO: should we update the bitmap in the cache if the new universe
            // reduces it?
            // TODO: maybe have a generation: u32 to track every time the universe was
            // reduced. Then only attempt to recompute the intersection when there is a chance
            // that condition_docids & universe changed
            return Ok(&self.cache[&interned_condition]);
        }
        // TODO: maybe universe doesn't belong here
        let condition = graph.conditions_interner.get(interned_condition);
        // TODO: faster way to do this?
        let docids = universe & G::resolve_condition(ctx, condition, universe)?;
        let _ = self.cache.insert(interned_condition, docids);
        let docids = &self.cache[&interned_condition];
        Ok(docids)
    }
}

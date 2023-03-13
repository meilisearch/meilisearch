use std::marker::PhantomData;

use fxhash::FxHashMap;
use roaring::RoaringBitmap;

use super::{RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::Interned;
use crate::search::new::SearchContext;
use crate::Result;

/// A cache storing the document ids associated with each ranking rule edge
pub struct EdgeConditionsCache<G: RankingRuleGraphTrait> {
    // TODO: should be FxHashMap<Interned<EdgeCondition>, RoaringBitmap>
    pub cache: FxHashMap<Interned<G::EdgeCondition>, RoaringBitmap>,
    _phantom: PhantomData<G>,
}
impl<G: RankingRuleGraphTrait> Default for EdgeConditionsCache<G> {
    fn default() -> Self {
        Self { cache: Default::default(), _phantom: Default::default() }
    }
}
impl<G: RankingRuleGraphTrait> EdgeConditionsCache<G> {
    /// Retrieve the document ids for the given edge condition.
    ///
    /// If the cache does not yet contain these docids, they are computed
    /// and inserted in the cache.
    pub fn get_edge_docids<'s, 'search>(
        &'s mut self,
        ctx: &mut SearchContext<'search>,
        // TODO: should be Interned<EdgeCondition>
        interned_edge_condition: Interned<G::EdgeCondition>,
        graph: &RankingRuleGraph<G>,
        // TODO: maybe universe doesn't belong here
        universe: &RoaringBitmap,
    ) -> Result<&'s RoaringBitmap> {
        if self.cache.contains_key(&interned_edge_condition) {
            // TODO: should we update the bitmap in the cache if the new universe
            // reduces it?
            // TODO: maybe have a generation: u32 to track every time the universe was
            // reduced. Then only attempt to recompute the intersection when there is a chance
            // that edge_docids & universe changed
            return Ok(&self.cache[&interned_edge_condition]);
        }
        // TODO: maybe universe doesn't belong here
        let edge_condition = graph.conditions_interner.get(interned_edge_condition);
        // TODO: faster way to do this?
        let docids = universe & G::resolve_edge_condition(ctx, edge_condition, universe)?;
        let _ = self.cache.insert(interned_edge_condition, docids);
        let docids = &self.cache[&interned_edge_condition];
        Ok(docids)
    }
}

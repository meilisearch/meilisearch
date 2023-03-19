use std::marker::PhantomData;

use fxhash::FxHashMap;
use roaring::RoaringBitmap;

use super::{RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::Interned;
use crate::search::new::SearchContext;
use crate::Result;

// TODO: give a generation to each universe, then be able to get the exact
// delta of docids between two universes of different generations!

/// A cache storing the document ids associated with each ranking rule edge
pub struct ConditionDocIdsCache<G: RankingRuleGraphTrait> {
    pub cache: FxHashMap<Interned<G::Condition>, (u64, RoaringBitmap)>,
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
            // TODO compare length of universe compared to the one in self
            // if it is smaller, then update the value
            let (universe_len, docids) = self.cache.entry(interned_condition).or_default();
            if *universe_len == universe.len() {
                return Ok(docids);
            } else {
                *docids &= universe;
                *universe_len = universe.len();
                return Ok(docids);
            }
        }
        // TODO: maybe universe doesn't belong here
        let condition = graph.conditions_interner.get(interned_condition);
        // TODO: faster way to do this?
        let docids = G::resolve_condition(ctx, condition, universe)?;
        let _ = self.cache.insert(interned_condition, (universe.len(), docids));
        let (_, docids) = &self.cache[&interned_condition];
        Ok(docids)
    }
}

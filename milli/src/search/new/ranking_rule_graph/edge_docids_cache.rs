use std::marker::PhantomData;

use fxhash::FxHashMap;
use roaring::RoaringBitmap;

use super::{EdgeCondition, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::{BitmapOrAllRef, SearchContext};
use crate::Result;

/// A cache storing the document ids associated with each ranking rule edge
pub struct EdgeDocidsCache<G: RankingRuleGraphTrait> {
    // TODO: should be FxHashMap<Interned<EdgeCondition>, RoaringBitmap>
    pub cache: FxHashMap<u16, RoaringBitmap>,
    _phantom: PhantomData<G>,
}
impl<G: RankingRuleGraphTrait> Default for EdgeDocidsCache<G> {
    fn default() -> Self {
        Self { cache: Default::default(), _phantom: Default::default() }
    }
}
impl<G: RankingRuleGraphTrait> EdgeDocidsCache<G> {
    /// Retrieve the document ids for the given edge condition.
    ///
    /// If the cache does not yet contain these docids, they are computed
    /// and inserted in the cache.
    pub fn get_edge_docids<'s, 'search>(
        &'s mut self,
        ctx: &mut SearchContext<'search>,
        // TODO: should be Interned<EdgeCondition>
        edge_index: u16,
        graph: &RankingRuleGraph<G>,
        // TODO: maybe universe doesn't belong here
        universe: &RoaringBitmap,
    ) -> Result<BitmapOrAllRef<'s>> {
        let edge = graph.edges_store[edge_index as usize].as_ref().unwrap();

        match &edge.condition {
            EdgeCondition::Unconditional => Ok(BitmapOrAllRef::All),
            EdgeCondition::Conditional(details) => {
                if self.cache.contains_key(&edge_index) {
                    // TODO: should we update the bitmap in the cache if the new universe
                    // reduces it?
                    // TODO: maybe have a generation: u32 to track every time the universe was
                    // reduced. Then only attempt to recompute the intersection when there is a chance
                    // that edge_docids & universe changed
                    return Ok(BitmapOrAllRef::Bitmap(&self.cache[&edge_index]));
                }
                // TODO: maybe universe doesn't belong here
                let docids = universe & G::resolve_edge_condition(ctx, details, universe)?;
                let _ = self.cache.insert(edge_index, docids);
                let docids = &self.cache[&edge_index];
                Ok(BitmapOrAllRef::Bitmap(docids))
            }
        }
    }
}

use std::marker::PhantomData;

use super::{EdgeDetails, RankingRuleGraph, RankingRuleGraphTrait};
use crate::new::{BitmapOrAllRef, SearchContext};
use crate::Result;
use fxhash::FxHashMap;
use roaring::RoaringBitmap;

// TODO: the cache should have a G::EdgeDetails as key
// but then it means that we should have a quick way of
// computing their hash and comparing them
// which can be done...
// by using a pointer (real, Rc, bumpalo, or in a vector)???
//
// But actually.... the edge details' docids are a subset of the universe at the
// moment they were computed.
// But the universes between two iterations of a ranking rule are completely different
// Thus, there is no point in doing this.
// UNLESS...
// we compute the whole docids corresponding to the edge details (potentially expensive in time and memory
// in the common case)
//
// But we could still benefit within a single iteration for requests like:
// `a a a a a a a a a` where we have many of the same edge details, repeated

pub struct EdgeDocidsCache<G: RankingRuleGraphTrait> {
    pub cache: FxHashMap<u16, RoaringBitmap>,
    _phantom: PhantomData<G>,
}
impl<G: RankingRuleGraphTrait> Default for EdgeDocidsCache<G> {
    fn default() -> Self {
        Self { cache: Default::default(), _phantom: Default::default() }
    }
}
impl<G: RankingRuleGraphTrait> EdgeDocidsCache<G> {
    pub fn get_edge_docids<'s, 'search>(
        &'s mut self,
        ctx: &mut SearchContext<'search>,
        edge_index: u16,
        graph: &RankingRuleGraph<G>,
        // TODO: maybe universe doesn't belong here
        universe: &RoaringBitmap,
    ) -> Result<BitmapOrAllRef<'s>> {
        let edge = graph.all_edges[edge_index as usize].as_ref().unwrap();

        match &edge.details {
            EdgeDetails::Unconditional => Ok(BitmapOrAllRef::All),
            EdgeDetails::Data(details) => {
                if self.cache.contains_key(&edge_index) {
                    // TODO: should we update the bitmap in the cache if the new universe
                    // reduces it?
                    // TODO: maybe have a generation: u32 to track every time the universe was
                    // reduced. Then only attempt to recompute the intersection when there is a chance
                    // that edge_docids & universe changed
                    return Ok(BitmapOrAllRef::Bitmap(&self.cache[&edge_index]));
                }
                // TODO: maybe universe doesn't belong here
                let docids = universe & G::compute_docids(ctx, details, universe)?;
                let _ = self.cache.insert(edge_index, docids);
                let docids = &self.cache[&edge_index];
                Ok(BitmapOrAllRef::Bitmap(docids))
            }
        }
    }
}

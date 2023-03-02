use std::marker::PhantomData;

use fxhash::FxHashMap;
use heed::RoTxn;
use roaring::RoaringBitmap;

use super::{EdgeDetails, RankingRuleGraph, RankingRuleGraphTrait};
use crate::new::db_cache::DatabaseCache;
use crate::new::BitmapOrAllRef;
use crate::{Index, Result};

// TODO: the cache should have a G::EdgeDetails as key
// but then it means that we should have a quick way of
// computing their hash and comparing them
// which can be done...
// by using a pointer (real, Rc, bumpalo, or in a vector)???

pub struct EdgeDocidsCache<G: RankingRuleGraphTrait> {
    pub cache: FxHashMap<u32, RoaringBitmap>,
    _phantom: PhantomData<G>,
}
impl<G: RankingRuleGraphTrait> Default for EdgeDocidsCache<G> {
    fn default() -> Self {
        Self { cache: Default::default(), _phantom: Default::default() }
    }
}
impl<G: RankingRuleGraphTrait> EdgeDocidsCache<G> {
    pub fn get_edge_docids<'s, 'transaction>(
        &'s mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        edge_index: u32,
        graph: &RankingRuleGraph<G>,
        // TODO: maybe universe doesn't belong here
        universe: &RoaringBitmap,
    ) -> Result<BitmapOrAllRef<'s>> {
        let edge = graph.all_edges[edge_index as usize].as_ref().unwrap();

        match &edge.details {
            EdgeDetails::Unconditional => Ok(BitmapOrAllRef::All),
            EdgeDetails::Data(details) => {
                if self.cache.contains_key(&edge_index) {
                    return Ok(BitmapOrAllRef::Bitmap(&self.cache[&edge_index]));
                }
                // TODO: maybe universe doesn't belong here
                let docids = universe & G::compute_docids(index, txn, db_cache, details)?;

                let _ = self.cache.insert(edge_index, docids);
                let docids = &self.cache[&edge_index];
                Ok(BitmapOrAllRef::Bitmap(docids))
            }
        }
    }
}

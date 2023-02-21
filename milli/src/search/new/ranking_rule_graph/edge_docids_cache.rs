use std::collections::HashMap;
use std::marker::PhantomData;

use heed::RoTxn;
use roaring::RoaringBitmap;

use super::{EdgeDetails, EdgeIndex, RankingRuleGraph, RankingRuleGraphTrait};
use crate::new::db_cache::DatabaseCache;
use crate::new::BitmapOrAllRef;
use crate::{Index, Result};

// TODO: the cache should have a G::EdgeDetails as key
// but then it means that we should have a quick way of
// computing their hash and comparing them
// which can be done...
// by using a pointer (real, Rc, bumpalo, or in a vector)???

pub struct EdgeDocidsCache<G: RankingRuleGraphTrait> {
    pub cache: HashMap<EdgeIndex, RoaringBitmap>,

    // TODO: There is a big difference between `cache`, which is always valid, and
    // `empty_path_prefixes`, which is only accurate for a particular universe
    // ALSO, we should have a universe-specific `empty_edge` to use
    // pub empty_path_prefixes: HashSet<Vec<EdgeIndex>>,
    _phantom: PhantomData<G>,
}
impl<G: RankingRuleGraphTrait> Default for EdgeDocidsCache<G> {
    fn default() -> Self {
        Self {
            cache: Default::default(),
            // empty_path_prefixes: Default::default(),
            _phantom: Default::default(),
        }
    }
}
impl<G: RankingRuleGraphTrait> EdgeDocidsCache<G> {
    pub fn get_edge_docids<'s, 'transaction>(
        &'s mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        edge_index: &EdgeIndex,
        graph: &RankingRuleGraph<G>,
    ) -> Result<BitmapOrAllRef<'s>> {
        if self.cache.contains_key(edge_index) {
            return Ok(BitmapOrAllRef::Bitmap(&self.cache[edge_index]));
        }
        let edge = graph.get_edge(*edge_index).as_ref().unwrap();

        match &edge.details {
            EdgeDetails::Unconditional => Ok(BitmapOrAllRef::All),
            EdgeDetails::Data(details) => {
                let docids = G::compute_docids(index, txn, db_cache, details)?;

                let _ = self.cache.insert(*edge_index, docids);
                let docids = &self.cache[edge_index];
                Ok(BitmapOrAllRef::Bitmap(docids))
            }
        }
    }
}

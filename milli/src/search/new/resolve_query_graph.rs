use std::collections::VecDeque;

use fxhash::FxHashMap;
use heed::{BytesDecode, RoTxn};
use roaring::{MultiOps, RoaringBitmap};

use super::db_cache::DatabaseCache;
use super::query_term::{QueryTerm, WordDerivations};
use super::QueryGraph;
use crate::{Index, Result, RoaringBitmapCodec};

// TODO: manual performance metrics: access to DB, bitmap deserializations/operations, etc.
#[derive(Default)]
pub struct NodeDocIdsCache {
    pub cache: FxHashMap<u32, RoaringBitmap>,
}
impl NodeDocIdsCache {
    fn get_docids<'cache, 'transaction>(
        &'cache mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        term: &QueryTerm,
        node_idx: u32,
    ) -> Result<&'cache RoaringBitmap> {
        if self.cache.contains_key(&node_idx) {
            return Ok(&self.cache[&node_idx]);
        };
        let docids = match term {
            QueryTerm::Phrase(_) => {
                todo!("resolve phrase")
            }
            QueryTerm::Word {
                derivations:
                    WordDerivations { original, zero_typo, one_typo, two_typos, use_prefix_db },
            } => {
                let derivations_docids = {
                    let mut or_docids = vec![];
                    for word in zero_typo.iter().chain(one_typo.iter()).chain(two_typos.iter()) {
                        if let Some(word_docids) = db_cache.get_word_docids(index, txn, word)? {
                            or_docids.push(word_docids);
                        }
                    }
                    if *use_prefix_db {
                        if let Some(prefix_docids) =
                            db_cache.get_prefix_docids(index, txn, original.as_str())?
                        {
                            or_docids.push(prefix_docids);
                        }
                    }
                    or_docids
                };
                let derivations_iter = derivations_docids
                    .into_iter()
                    .map(|slice| RoaringBitmapCodec::bytes_decode(slice).unwrap());
                MultiOps::union(derivations_iter)
            }
        };
        let _ = self.cache.insert(node_idx, docids);
        let docids = &self.cache[&node_idx];
        Ok(docids)
    }
}

pub fn resolve_query_graph<'transaction>(
    index: &Index,
    txn: &'transaction RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    node_docids_cache: &mut NodeDocIdsCache,
    q: &QueryGraph,
    universe: &RoaringBitmap,
) -> Result<RoaringBitmap> {
    // TODO: there is definitely a faster way to compute this big
    // roaring bitmap expression

    let mut nodes_resolved = RoaringBitmap::new();
    let mut path_nodes_docids = vec![RoaringBitmap::new(); q.nodes.len()];

    let mut next_nodes_to_visit = VecDeque::new();
    next_nodes_to_visit.push_front(q.root_node);

    while let Some(node) = next_nodes_to_visit.pop_front() {
        let predecessors = &q.edges[node as usize].predecessors;
        if !predecessors.is_subset(&nodes_resolved) {
            next_nodes_to_visit.push_back(node);
            continue;
        }
        // Take union of all predecessors
        let predecessors_iter = predecessors.iter().map(|p| &path_nodes_docids[p as usize]);
        let predecessors_docids = MultiOps::union(predecessors_iter);

        let n = &q.nodes[node as usize];
        // println!("resolving {node} {n:?}, predecessors: {predecessors:?}, their docids: {predecessors_docids:?}");
        let node_docids = match n {
            super::QueryNode::Term(located_term) => {
                let term = &located_term.value;
                let derivations_docids =
                    node_docids_cache.get_docids(index, txn, db_cache, term, node)?;
                predecessors_docids & derivations_docids
            }
            super::QueryNode::Deleted => {
                panic!()
            }
            super::QueryNode::Start => universe.clone(),
            super::QueryNode::End => {
                return Ok(predecessors_docids);
            }
        };
        nodes_resolved.insert(node);
        path_nodes_docids[node as usize] = node_docids;

        for succ in q.edges[node as usize].successors.iter() {
            if !next_nodes_to_visit.contains(&succ) && !nodes_resolved.contains(succ) {
                next_nodes_to_visit.push_back(succ);
            }
        }

        // This is currently slow but could easily be implemented very efficiently
        for prec in q.edges[node as usize].predecessors.iter() {
            if q.edges[prec as usize].successors.is_subset(&nodes_resolved) {
                path_nodes_docids[prec as usize].clear();
            }
        }
    }

    panic!()
}

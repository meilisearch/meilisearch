use std::collections::{BTreeSet, HashMap, HashSet};

use heed::RoTxn;
use roaring::RoaringBitmap;

use super::{Edge, RankingRuleGraph, RankingRuleGraphTrait};
use crate::new::db_cache::DatabaseCache;
use crate::new::{NodeIndex, QueryGraph};
use crate::{Index, Result};

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    pub fn build<'db_cache, 'transaction: 'db_cache>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        query_graph: QueryGraph,
    ) -> Result<Self> {
        let mut ranking_rule_graph =
            Self { query_graph, all_edges: vec![], node_edges: vec![], successors: vec![] };

        for (node_idx, node) in ranking_rule_graph.query_graph.nodes.iter().enumerate() {
            ranking_rule_graph.node_edges.push(RoaringBitmap::new());
            ranking_rule_graph.successors.push(RoaringBitmap::new());
            let new_edges = ranking_rule_graph.node_edges.last_mut().unwrap();
            let new_successors = ranking_rule_graph.successors.last_mut().unwrap();

            let Some(from_node_data) = G::build_visit_from_node(index, txn, db_cache, node)? else { continue };

            for successor_idx in ranking_rule_graph.query_graph.edges[node_idx].successors.iter() {
                let to_node = &ranking_rule_graph.query_graph.nodes[successor_idx as usize];
                let mut edges =
                    G::build_visit_to_node(index, txn, db_cache, to_node, &from_node_data)?;
                if edges.is_empty() {
                    continue;
                }
                edges.sort_by_key(|e| e.0);
                for (cost, details) in edges {
                    ranking_rule_graph.all_edges.push(Some(Edge {
                        from_node: NodeIndex(node_idx as u32),
                        to_node: NodeIndex(successor_idx),
                        cost,
                        details,
                    }));
                    new_edges.insert(ranking_rule_graph.all_edges.len() as u32 - 1);
                    new_successors.insert(successor_idx);
                }
            }
        }
        // ranking_rule_graph.simplify();

        Ok(ranking_rule_graph)
    }
}

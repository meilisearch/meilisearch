use std::collections::{BTreeSet, HashMap, HashSet};

use heed::RoTxn;

use super::{Edge, RankingRuleGraph, RankingRuleGraphTrait};
use crate::new::db_cache::DatabaseCache;
use crate::new::QueryGraph;
use crate::{Index, Result};

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    pub fn build<'db_cache, 'transaction: 'db_cache>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        query_graph: QueryGraph,
    ) -> Result<Self> {
        let mut ranking_rule_graph = Self { query_graph, all_edges: vec![], node_edges: vec![] };

        for (node_idx, node) in ranking_rule_graph.query_graph.nodes.iter().enumerate() {
            ranking_rule_graph.node_edges.push(BTreeSet::new());
            let new_edges = ranking_rule_graph.node_edges.last_mut().unwrap();

            let Some(from_node_data) = G::build_visit_from_node(index, txn, db_cache, node)? else { continue };

            for &successor_idx in ranking_rule_graph.query_graph.edges[node_idx].outgoing.iter() {
                let to_node = &ranking_rule_graph.query_graph.nodes[successor_idx];
                let Some(edges) = G::build_visit_to_node(index, txn, db_cache, to_node, &from_node_data)? else { continue };
                for (cost, details) in edges {
                    ranking_rule_graph.all_edges.push(Some(Edge {
                        from_node: node_idx,
                        to_node: successor_idx,
                        cost,
                        details,
                    }));
                    new_edges.insert(ranking_rule_graph.all_edges.len() - 1);
                }
            }
        }
        ranking_rule_graph.simplify();

        Ok(ranking_rule_graph)
    }
}

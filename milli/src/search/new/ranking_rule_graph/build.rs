use std::collections::HashSet;

use super::{Edge, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, SearchContext};
use crate::Result;

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    pub fn build(ctx: &mut SearchContext, query_graph: QueryGraph) -> Result<Self> {
        let QueryGraph { nodes: graph_nodes, edges: graph_edges, .. } = &query_graph;

        let mut all_edges = vec![];
        let mut node_edges = vec![];
        let mut successors = vec![];

        for (node_idx, node) in graph_nodes.iter().enumerate() {
            node_edges.push(HashSet::new());
            successors.push(HashSet::new());
            let new_edges = node_edges.last_mut().unwrap();
            let new_successors = successors.last_mut().unwrap();

            let Some(from_node_data) = G::build_visit_from_node(ctx, node)? else { continue };

            for successor_idx in graph_edges[node_idx].successors.iter() {
                let to_node = &graph_nodes[successor_idx as usize];
                let mut edges = G::build_visit_to_node(ctx, to_node, &from_node_data)?;
                if edges.is_empty() {
                    continue;
                }
                edges.sort_by_key(|e| e.0);
                for (cost, details) in edges {
                    all_edges.push(Some(Edge {
                        from_node: node_idx as u16,
                        to_node: successor_idx,
                        cost,
                        details,
                    }));
                    new_edges.insert(all_edges.len() as u16 - 1);
                    new_successors.insert(successor_idx);
                }
            }
        }
        let node_edges = node_edges
            .into_iter()
            .map(|edges| SmallBitmap::from_iter(edges.into_iter(), all_edges.len() as u16))
            .collect();
        let successors = successors
            .into_iter()
            .map(|edges| SmallBitmap::from_iter(edges.into_iter(), all_edges.len() as u16))
            .collect();

        Ok(RankingRuleGraph { query_graph, all_edges, node_edges, successors })
    }
}

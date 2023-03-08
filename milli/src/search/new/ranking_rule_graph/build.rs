use std::collections::HashSet;

use super::{Edge, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, SearchContext};
use crate::Result;

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    /// Build the ranking rule graph from the given query graph
    pub fn build(ctx: &mut SearchContext, query_graph: QueryGraph) -> Result<Self> {
        let QueryGraph { nodes: graph_nodes, edges: graph_edges, .. } = &query_graph;

        let mut edges_store = vec![];
        let mut edges_of_node = vec![];

        for (node_idx, node) in graph_nodes.iter().enumerate() {
            edges_of_node.push(HashSet::new());
            let new_edges = edges_of_node.last_mut().unwrap();

            let Some(source_node_data) = G::build_step_visit_source_node(ctx, node)? else { continue };

            for successor_idx in graph_edges[node_idx].successors.iter() {
                let dest_node = &graph_nodes[successor_idx as usize];
                let edges =
                    G::build_step_visit_destination_node(ctx, dest_node, &source_node_data)?;
                if edges.is_empty() {
                    continue;
                }

                for (cost, details) in edges {
                    edges_store.push(Some(Edge {
                        source_node: node_idx as u16,
                        dest_node: successor_idx,
                        cost,
                        condition: details,
                    }));
                    new_edges.insert(edges_store.len() as u16 - 1);
                }
            }
        }
        let edges_of_node = edges_of_node
            .into_iter()
            .map(|edges| SmallBitmap::from_iter(edges.into_iter(), edges_store.len() as u16))
            .collect();

        Ok(RankingRuleGraph { query_graph, edges_store, edges_of_node })
    }
}

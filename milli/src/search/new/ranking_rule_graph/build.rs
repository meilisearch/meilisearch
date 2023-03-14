use std::collections::HashSet;

use super::{Edge, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::Interner;
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, SearchContext};
use crate::Result;

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    // TODO: here, the docids of all the edges should already be computed!
    // an edge condition would then be reduced to a (ptr to) a roaring bitmap?
    // we could build fewer of them by directly comparing them with the universe
    // (e.g. for each word pairs?) with `deserialize_within_universe` maybe
    //

    /// Build the ranking rule graph from the given query graph
    pub fn build(ctx: &mut SearchContext, query_graph: QueryGraph) -> Result<Self> {
        let QueryGraph { nodes: graph_nodes, edges: graph_edges, .. } = &query_graph;

        let mut conditions_interner = Interner::default();

        let mut edges_store = vec![];
        let mut edges_of_node = vec![];

        for (source_idx, source_node) in graph_nodes.iter().enumerate() {
            edges_of_node.push(HashSet::new());
            let new_edges = edges_of_node.last_mut().unwrap();

            for dest_idx in graph_edges[source_idx].successors.iter() {
                let dest_node = &graph_nodes[dest_idx as usize];
                let edges = G::build_edges(ctx, &mut conditions_interner, source_node, dest_node)?;
                if edges.is_empty() {
                    continue;
                }

                for (cost, condition) in edges {
                    edges_store.push(Some(Edge {
                        source_node: source_idx as u16,
                        dest_node: dest_idx,
                        cost,
                        condition,
                    }));
                    new_edges.insert(edges_store.len() as u16 - 1);
                }
            }
        }
        let edges_of_node = edges_of_node
            .into_iter()
            .map(|edges| SmallBitmap::from_iter(edges.into_iter(), edges_store.len() as u16))
            .collect();

        Ok(RankingRuleGraph { query_graph, edges_store, edges_of_node, conditions_interner })
    }
}

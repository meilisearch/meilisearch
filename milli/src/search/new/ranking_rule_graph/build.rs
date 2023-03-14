use std::collections::HashSet;

use super::{Edge, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interner};
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
        let QueryGraph { nodes: graph_nodes, .. } = &query_graph;

        let mut conditions_interner = DedupInterner::default();

        let mut edges_store = Interner::default();
        let mut edges_of_node = query_graph.nodes.map(|_| HashSet::new());

        for (source_id, source_node) in graph_nodes.iter() {
            let new_edges = edges_of_node.get_mut(source_id);

            for dest_idx in source_node.successors.iter() {
                let dest_node = graph_nodes.get(dest_idx);
                let edges = G::build_edges(ctx, &mut conditions_interner, source_node, dest_node)?;
                if edges.is_empty() {
                    continue;
                }

                for (cost, condition) in edges {
                    let new_edge_id = edges_store.push(Some(Edge {
                        source_node: source_id,
                        dest_node: dest_idx,
                        cost,
                        condition,
                    }));
                    new_edges.insert(new_edge_id);
                }
            }
        }
        let edges_store = edges_store.freeze();
        let edges_of_node =
            edges_of_node.map(|edges| SmallBitmap::from_iter(edges.iter().copied(), &edges_store));

        Ok(RankingRuleGraph {
            query_graph,
            edges_store,
            edges_of_node,
            conditions_interner: conditions_interner.freeze(),
        })
    }
}

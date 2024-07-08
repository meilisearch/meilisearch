use std::collections::HashSet;

use super::{Edge, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, MappedInterner};
use crate::search::new::query_graph::{QueryNode, QueryNodeData};
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, SearchContext};
use crate::Result;

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    /// Build the ranking rule graph from the given query graph
    pub fn build(
        ctx: &mut SearchContext<'_>,
        query_graph: QueryGraph,
        cost_of_ignoring_node: MappedInterner<QueryNode, Option<(u32, SmallBitmap<QueryNode>)>>,
    ) -> Result<Self> {
        let QueryGraph { nodes: graph_nodes, .. } = &query_graph;

        let mut conditions_interner = DedupInterner::default();

        let mut edges_store = DedupInterner::default();
        let mut edges_of_node = query_graph.nodes.map(|_| HashSet::new());

        for (source_id, source_node) in graph_nodes.iter() {
            let new_edges = edges_of_node.get_mut(source_id);

            for dest_idx in source_node.successors.iter() {
                let src_term = match &source_node.data {
                    QueryNodeData::Term(t) => Some(t),
                    QueryNodeData::Start => None,
                    QueryNodeData::Deleted | QueryNodeData::End => panic!(),
                };
                let dest_node = graph_nodes.get(dest_idx);
                let dest_term = match &dest_node.data {
                    QueryNodeData::Term(t) => t,
                    QueryNodeData::End => {
                        let new_edge_id = edges_store.insert(Some(Edge {
                            source_node: source_id,
                            dest_node: dest_idx,
                            cost: 0,
                            condition: None,
                            nodes_to_skip: SmallBitmap::for_interned_values_in(graph_nodes),
                        }));
                        new_edges.insert(new_edge_id);
                        continue;
                    }
                    QueryNodeData::Deleted | QueryNodeData::Start => panic!(),
                };
                if let Some((cost_of_ignoring, forbidden_nodes)) =
                    cost_of_ignoring_node.get(dest_idx)
                {
                    let dest = graph_nodes.get(dest_idx);
                    let dest_size = match &dest.data {
                        QueryNodeData::Term(term) => term.term_ids.len(),
                        _ => panic!(),
                    };
                    let new_edge_id = edges_store.insert(Some(Edge {
                        source_node: source_id,
                        dest_node: dest_idx,
                        cost: *cost_of_ignoring * dest_size as u32,
                        condition: None,
                        nodes_to_skip: forbidden_nodes.clone(),
                    }));
                    new_edges.insert(new_edge_id);
                }

                let edges = G::build_edges(ctx, &mut conditions_interner, src_term, dest_term)?;
                if edges.is_empty() {
                    continue;
                }

                for (cost, condition) in edges {
                    let new_edge_id = edges_store.insert(Some(Edge {
                        source_node: source_id,
                        dest_node: dest_idx,
                        cost,
                        condition: Some(condition),
                        nodes_to_skip: SmallBitmap::for_interned_values_in(graph_nodes),
                    }));
                    new_edges.insert(new_edge_id);
                }
            }
        }
        let edges_store = edges_store.freeze();
        let edges_of_node =
            edges_of_node.map(|edges| SmallBitmap::from_iter(edges.iter().copied(), &edges_store));

        let conditions_interner = conditions_interner.freeze();

        Ok(RankingRuleGraph { query_graph, edges_store, edges_of_node, conditions_interner })
    }
}

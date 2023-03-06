use super::{Edge, RankingRuleGraph, RankingRuleGraphTrait};
use crate::new::{QueryGraph, SearchContext};
use crate::Result;
use roaring::RoaringBitmap;

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    pub fn build(ctx: &mut SearchContext, query_graph: QueryGraph) -> Result<Self> {
        let mut ranking_rule_graph =
            Self { query_graph, all_edges: vec![], node_edges: vec![], successors: vec![] };

        for (node_idx, node) in ranking_rule_graph.query_graph.nodes.iter().enumerate() {
            ranking_rule_graph.node_edges.push(RoaringBitmap::new());
            ranking_rule_graph.successors.push(RoaringBitmap::new());
            let new_edges = ranking_rule_graph.node_edges.last_mut().unwrap();
            let new_successors = ranking_rule_graph.successors.last_mut().unwrap();

            let Some(from_node_data) = G::build_visit_from_node(ctx, node)? else { continue };

            for successor_idx in ranking_rule_graph.query_graph.edges[node_idx].successors.iter() {
                let to_node = &ranking_rule_graph.query_graph.nodes[successor_idx as usize];
                let mut edges = G::build_visit_to_node(ctx, to_node, &from_node_data)?;
                if edges.is_empty() {
                    continue;
                }
                edges.sort_by_key(|e| e.0);
                for (cost, details) in edges {
                    ranking_rule_graph.all_edges.push(Some(Edge {
                        from_node: node_idx as u32,
                        to_node: successor_idx,
                        cost,
                        details,
                    }));
                    new_edges.insert(ranking_rule_graph.all_edges.len() as u32 - 1);
                    new_successors.insert(successor_idx);
                }
            }
        }
        Ok(ranking_rule_graph)
    }
}

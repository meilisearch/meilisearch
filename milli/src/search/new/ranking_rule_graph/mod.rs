pub mod build;
pub mod cheapest_paths;
pub mod edge_docids_cache;
pub mod empty_paths_cache;
pub mod paths_map;
pub mod proximity;
pub mod resolve_paths;
use std::collections::{BTreeSet, HashSet};
use std::ops::ControlFlow;

use heed::RoTxn;
use roaring::RoaringBitmap;

use super::db_cache::DatabaseCache;
use super::{QueryGraph, QueryNode};
use crate::{Index, Result};

#[derive(Debug, Clone)]
pub enum EdgeDetails<E> {
    Unconditional,
    Data(E),
}

#[derive(Debug, Clone)]
pub struct Edge<E> {
    from_node: u32,
    to_node: u32,
    cost: u8,
    details: EdgeDetails<E>,
}

#[derive(Debug, Clone)]
pub struct EdgePointer<'graph, E> {
    pub index: u32,
    pub edge: &'graph Edge<E>,
}

pub trait RankingRuleGraphTrait {
    /// The details of an edge connecting two query nodes. These details
    /// should be sufficient to compute the edge's cost and associated document ids
    /// in [`compute_docids`](RankingRuleGraphTrait).
    type EdgeDetails: Sized;

    type BuildVisitedFromNode;

    /// Return the label of the given edge details, to be used when visualising
    /// the ranking rule graph using GraphViz.
    fn graphviz_edge_details_label(edge: &Self::EdgeDetails) -> String;

    /// Compute the document ids associated with the given edge.
    fn compute_docids<'transaction>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        edge_details: &Self::EdgeDetails,
    ) -> Result<RoaringBitmap>;

    /// Prepare to build the edges outgoing from `from_node`.
    ///
    /// This call is followed by zero, one or more calls to [`build_visit_to_node`](RankingRuleGraphTrait::build_visit_to_node),
    /// which builds the actual edges.
    fn build_visit_from_node<'transaction>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        from_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>>;

    /// Return the cost and details of the edges going from the previously visited node
    /// (with [`build_visit_from_node`](RankingRuleGraphTrait::build_visit_from_node)) to `to_node`.
    fn build_visit_to_node<'from_data, 'transaction: 'from_data>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        to_node: &QueryNode,
        from_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Vec<(u8, EdgeDetails<Self::EdgeDetails>)>>;
}

pub struct RankingRuleGraph<G: RankingRuleGraphTrait> {
    pub query_graph: QueryGraph,
    // pub edges: Vec<HashMap<usize, Vec<Edge<G::EdgeDetails>>>>,
    pub all_edges: Vec<Option<Edge<G::EdgeDetails>>>,

    pub node_edges: Vec<RoaringBitmap>,

    pub successors: Vec<RoaringBitmap>,
    // TODO: to get the edges between two nodes:
    // 1. get node_outgoing_edges[from]
    // 2. get node_incoming_edges[to]
    // 3. take intersection betweem the two
}
impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    // Visit all edges between the two given nodes in order of increasing cost.
    pub fn visit_edges<'graph, O>(
        &'graph self,
        from: u32,
        to: u32,
        mut visit: impl FnMut(u32, &'graph Edge<G::EdgeDetails>) -> ControlFlow<O>,
    ) -> Option<O> {
        let from_edges = &self.node_edges[from as usize];
        for edge_idx in from_edges {
            let edge = self.all_edges[edge_idx as usize].as_ref().unwrap();
            if edge.to_node == to {
                let cf = visit(edge_idx, edge);
                match cf {
                    ControlFlow::Continue(_) => continue,
                    ControlFlow::Break(o) => return Some(o),
                }
            }
        }

        None
    }

    fn remove_edge(&mut self, edge_index: u32) {
        let edge_opt = &mut self.all_edges[edge_index as usize];
        let Some(edge) = &edge_opt else { return };
        let (from_node, to_node) = (edge.from_node, edge.to_node);
        *edge_opt = None;

        let from_node_edges = &mut self.node_edges[from_node as usize];
        from_node_edges.remove(edge_index);

        let mut new_successors_from_node = RoaringBitmap::new();
        for from_node_edge in from_node_edges.iter() {
            let Edge { to_node, .. } = &self.all_edges[from_node_edge as usize].as_ref().unwrap();
            new_successors_from_node.insert(*to_node);
        }
        self.successors[from_node as usize] = new_successors_from_node;
    }

    pub fn graphviz(&self) -> String {
        let mut desc = String::new();
        desc.push_str("digraph G {\nrankdir = LR;\nnode [shape = \"record\"]\n");

        for (node_idx, node) in self.query_graph.nodes.iter().enumerate() {
            if matches!(node, QueryNode::Deleted) {
                continue;
            }
            desc.push_str(&format!("{node_idx} [label = {:?}]", node));
            if node_idx == self.query_graph.root_node as usize {
                desc.push_str("[color = blue]");
            } else if node_idx == self.query_graph.end_node as usize {
                desc.push_str("[color = red]");
            }
            desc.push_str(";\n");
        }
        for edge in self.all_edges.iter().flatten() {
            let Edge { from_node, to_node, cost, details } = edge;

            match &details {
                EdgeDetails::Unconditional => {
                    desc.push_str(&format!(
                        "{from_node} -> {to_node} [label = \"always cost {cost}\"];\n",
                        cost = edge.cost,
                    ));
                }
                EdgeDetails::Data(details) => {
                    desc.push_str(&format!(
                        "{from_node} -> {to_node} [label = \"cost {cost} {edge_label}\"];\n",
                        cost = edge.cost,
                        edge_label = G::graphviz_edge_details_label(details)
                    ));
                }
            }
        }

        desc.push('}');
        desc
    }
}

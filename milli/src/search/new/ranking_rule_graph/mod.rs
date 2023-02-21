pub mod build;
pub mod cheapest_paths;
pub mod edge_docids_cache;
pub mod empty_paths_cache;
pub mod paths_map;
pub mod proximity;
pub mod resolve_paths;

use std::collections::{BTreeSet, HashMap, HashSet};
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
    from_node: usize,
    to_node: usize,
    cost: u8,
    details: EdgeDetails<E>,
}

#[derive(Debug, Clone)]
pub struct EdgePointer<'graph, E> {
    pub index: EdgeIndex,
    pub edge: &'graph Edge<E>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeIndex(pub usize);
//  {
//     // TODO: they could all be u16 instead
//     // There may be a way to store all the edge indices in a u32 as well,
//     // if the edges are in a vector
//     // then we can store sets of edges in a bitmap efficiently
//     pub from: usize,
//     pub to: usize,
//     pub edge_idx: usize,
// }

pub trait RankingRuleGraphTrait {
    type EdgeDetails: Sized;
    type BuildVisitedFromNode;

    fn edge_details_dot_label(edge: &Self::EdgeDetails) -> String;

    fn compute_docids<'transaction>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        edge_details: &Self::EdgeDetails,
    ) -> Result<RoaringBitmap>;

    fn build_visit_from_node<'transaction>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        from_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>>;

    fn build_visit_to_node<'from_data, 'transaction: 'from_data>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        to_node: &QueryNode,
        from_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Option<Vec<(u8, EdgeDetails<Self::EdgeDetails>)>>>;
}

pub struct RankingRuleGraph<G: RankingRuleGraphTrait> {
    pub query_graph: QueryGraph,
    // pub edges: Vec<HashMap<usize, Vec<Edge<G::EdgeDetails>>>>,
    pub all_edges: Vec<Option<Edge<G::EdgeDetails>>>,
    pub node_edges: Vec<BTreeSet<usize>>,
    // pub removed_edges: HashSet<EdgeIndex>,
    // pub tmp_removed_edges: HashSet<EdgeIndex>,
}
impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    // NOTE: returns the edge even if it was removed
    pub fn get_edge(&self, edge_index: EdgeIndex) -> &Option<Edge<G::EdgeDetails>> {
        &self.all_edges[edge_index.0]
    }
    pub fn visit_edges<'graph, O>(
        &'graph self,
        from: usize,
        to: usize,
        mut visit: impl FnMut(EdgeIndex, &'graph Edge<G::EdgeDetails>) -> ControlFlow<O>,
    ) -> Option<O> {
        let from_edges = &self.node_edges[from];
        for &edge_idx in from_edges {
            let edge = self.all_edges[edge_idx].as_ref().unwrap();
            if edge.to_node == to {
                let cf = visit(EdgeIndex(edge_idx), edge);
                match cf {
                    ControlFlow::Continue(_) => continue,
                    ControlFlow::Break(o) => return Some(o),
                }
            }
        }

        None
    }

    fn remove_edge(&mut self, edge_index: EdgeIndex) {
        let edge_opt = &mut self.all_edges[edge_index.0];
        let Some(Edge { from_node, to_node, cost, details }) = &edge_opt else { return };

        let node_edges = &mut self.node_edges[*from_node];
        node_edges.remove(&edge_index.0);

        *edge_opt = None;
    }
    pub fn remove_nodes(&mut self, nodes: &[usize]) {
        for &node in nodes {
            let edge_indices = &mut self.node_edges[node];
            for edge_index in edge_indices.iter() {
                self.all_edges[*edge_index] = None;
            }
            edge_indices.clear();

            let preds = &self.query_graph.edges[node].incoming;
            for pred in preds {
                let edge_indices = &mut self.node_edges[*pred];
                for edge_index in edge_indices.iter() {
                    let edge_opt = &mut self.all_edges[*edge_index];
                    let Some(edge) = edge_opt else { continue; };
                    if edge.to_node == node {
                        *edge_opt = None;
                    }
                }
                panic!("remove nodes is incorrect at the moment");
                edge_indices.clear();
            }
        }
        self.query_graph.remove_nodes(nodes);
    }
    pub fn simplify(&mut self) {
        loop {
            let mut nodes_to_remove = vec![];
            for (node_idx, node) in self.query_graph.nodes.iter().enumerate() {
                if !matches!(node, QueryNode::End | QueryNode::Deleted)
                    && self.node_edges[node_idx].is_empty()
                {
                    nodes_to_remove.push(node_idx);
                }
            }
            if nodes_to_remove.is_empty() {
                break;
            } else {
                self.remove_nodes(&nodes_to_remove);
            }
        }
    }
    // fn is_removed_edge(&self, edge: EdgeIndex) -> bool {
    //     self.removed_edges.contains(&edge) || self.tmp_removed_edges.contains(&edge)
    // }

    pub fn graphviz(&self) -> String {
        let mut desc = String::new();
        desc.push_str("digraph G {\nrankdir = LR;\nnode [shape = \"record\"]\n");

        for (node_idx, node) in self.query_graph.nodes.iter().enumerate() {
            if matches!(node, QueryNode::Deleted) {
                continue;
            }
            desc.push_str(&format!("{node_idx} [label = {:?}]", node));
            if node_idx == self.query_graph.root_node {
                desc.push_str("[color = blue]");
            } else if node_idx == self.query_graph.end_node {
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
                        edge_label = G::edge_details_dot_label(details)
                    ));
                }
            }
        }

        desc.push('}');
        desc
    }
}

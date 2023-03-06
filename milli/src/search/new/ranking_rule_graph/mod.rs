mod build;
mod cheapest_paths;
mod edge_docids_cache;
mod empty_paths_cache;
mod paths_map;
mod proximity;
mod resolve_paths;
mod typo;

use super::logger::SearchLogger;
use super::{QueryGraph, QueryNode, SearchContext};
use crate::Result;
pub use edge_docids_cache::EdgeDocidsCache;
pub use empty_paths_cache::EmptyPathsCache;
pub use proximity::ProximityGraph;
use roaring::RoaringBitmap;
use std::ops::ControlFlow;
pub use typo::TypoGraph;

#[derive(Debug, Clone)]
pub enum EdgeDetails<E> {
    Unconditional,
    Data(E),
}

#[derive(Debug, Clone)]
pub struct Edge<E> {
    pub from_node: u32,
    pub to_node: u32,
    pub cost: u8,
    pub details: EdgeDetails<E>,
}

#[derive(Debug, Clone)]
pub struct EdgePointer<'graph, E> {
    pub index: u32,
    pub edge: &'graph Edge<E>,
}

// pub struct SubWordDerivations {
//     words: FxHashSet<Interned<String>>,
//     synonyms: FxHashSet<Interned<Phrase>>, // NO! they're phrases, not strings
//     split_words: bool,
//     use_prefix_db: bool,
// }

// pub struct EdgeWordDerivations {
//     // TODO: not Option, instead: Any | All | Subset(SubWordDerivations)
//     from_words: Option<SubWordDerivations>, // ???
//     to_words: Option<SubWordDerivations>,   // + use prefix db?
// }

// fn aggregate_edge_word_derivations(
//     graph: (),
//     edges: Vec<usize>,
// ) -> BTreeMap<usize, SubWordDerivations> {
//     todo!()
// }

// fn reduce_word_term_to_sub_word_derivations(
//     term: &mut WordDerivations,
//     derivations: &SubWordDerivations,
// ) {
//     let mut new_one_typo = vec![];
//     for w in term.one_typo {
//         if derivations.words.contains(w) {
//             new_one_typo.push(w);
//         }
//     }
//     if term.use_prefix_db && !derivations.use_prefix_db {
//         term.use_prefix_db = false;
//     }
//     // etc.
// }

// fn word_derivations_used_by_edge<G: RankingRuleGraphTrait>(
//     edge: G::EdgeDetails,
// ) -> SubWordDerivations {
//     todo!()
// }

pub trait RankingRuleGraphTrait: Sized {
    /// The details of an edge connecting two query nodes. These details
    /// should be sufficient to compute the edge's cost and associated document ids
    /// in [`compute_docids`](RankingRuleGraphTrait).
    type EdgeDetails: Sized + Clone;

    type BuildVisitedFromNode;

    /// Return the label of the given edge details, to be used when visualising
    /// the ranking rule graph using GraphViz.
    fn graphviz_edge_details_label(edge: &Self::EdgeDetails) -> String;

    /// Compute the document ids associated with the given edge.
    fn compute_docids<'search>(
        ctx: &mut SearchContext<'search>,
        edge_details: &Self::EdgeDetails,
    ) -> Result<RoaringBitmap>;

    /// Prepare to build the edges outgoing from `from_node`.
    ///
    /// This call is followed by zero, one or more calls to [`build_visit_to_node`](RankingRuleGraphTrait::build_visit_to_node),
    /// which builds the actual edges.
    fn build_visit_from_node<'search>(
        ctx: &mut SearchContext<'search>,
        from_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>>;

    /// Return the cost and details of the edges going from the previously visited node
    /// (with [`build_visit_from_node`](RankingRuleGraphTrait::build_visit_from_node)) to `to_node`.
    fn build_visit_to_node<'from_data, 'search: 'from_data>(
        ctx: &mut SearchContext<'search>,
        to_node: &QueryNode,
        from_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Vec<(u8, EdgeDetails<Self::EdgeDetails>)>>;

    fn log_state(
        graph: &RankingRuleGraph<Self>,
        paths: &[Vec<u32>],
        empty_paths_cache: &EmptyPathsCache,
        universe: &RoaringBitmap,
        distances: &[Vec<u64>],
        cost: u64,
        logger: &mut dyn SearchLogger<QueryGraph>,
    );
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
impl<G: RankingRuleGraphTrait> Clone for RankingRuleGraph<G> {
    fn clone(&self) -> Self {
        Self {
            query_graph: self.query_graph.clone(),
            all_edges: self.all_edges.clone(),
            node_edges: self.node_edges.clone(),
            successors: self.successors.clone(),
        }
    }
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

    pub fn remove_edge(&mut self, edge_index: u32) {
        let edge_opt = &mut self.all_edges[edge_index as usize];
        let Some(edge) = &edge_opt else { return };
        let (from_node, _to_node) = (edge.from_node, edge.to_node);
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
}

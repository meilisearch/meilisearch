/*! Module implementing the graph used for the graph-based ranking rules
and its related algorithms.

A ranking rule graph is built on top of the [`QueryGraph`]: the nodes stay
the same but the edges are replaced.
*/

mod build;
mod cheapest_paths;
mod edge_docids_cache;
mod empty_paths_cache;
mod paths_map;

/// Implementation of the `proximity` ranking rule
mod proximity;
/// Implementation of the `typo` ranking rule
mod typo;

pub use edge_docids_cache::EdgeDocidsCache;
pub use empty_paths_cache::EmptyPathsCache;
pub use proximity::ProximityGraph;
use roaring::RoaringBitmap;
pub use typo::TypoGraph;

use super::logger::SearchLogger;
use super::small_bitmap::SmallBitmap;
use super::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

/// The condition that is associated with an edge in the ranking rule graph.
///
/// Some edges are unconditional, which means that traversing them does not reduce
/// the set of candidates.
///
/// Most edges, however, have a condition attached to them. For example, for the
/// proximity ranking rule, the condition could be that a word is N-close to another one.
/// When the edge is traversed, some database operations are executed to retrieve the set
/// of documents that satisfy the condition, which reduces the list of candidate document ids.
#[derive(Debug, Clone)]
pub enum EdgeCondition<E> {
    Unconditional,
    Conditional(E),
}

/// An edge in the ranking rule graph.
///
/// It contains:
/// 1. The source and destination nodes
/// 2. The cost of traversing this edge
/// 3. The condition associated with it
#[derive(Debug, Clone)]
pub struct Edge<E> {
    pub source_node: u16,
    pub dest_node: u16,
    pub cost: u8,
    pub condition: EdgeCondition<E>,
}

// pub struct SubWordDerivations {
//     words: FxHashSet<Interned<String>>,
//     phrases: FxHashSet<Interned<Phrase>>,
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
//     edge: G::EdgeCondition,
// ) -> SubWordDerivations {
//     todo!()
// }

/// A trait to be implemented by a marker type to build a graph-based ranking rule.
///
/// It mostly describes how to:
/// 1. Retrieve the set of edges (their cost and condition) between two nodes.
/// 2. Compute the document ids satisfying a condition
pub trait RankingRuleGraphTrait: Sized {
    /// The condition of an edge connecting two query nodes. The condition
    /// should be sufficient to compute the edge's cost and associated document ids
    /// in [`resolve_edge_condition`](RankingRuleGraphTrait::resolve_edge_condition).
    type EdgeCondition: Sized + Clone;

    /// A structure used in the construction of the graph, created when a
    /// query graph source node is visited. It is used to determine the cost
    /// and condition of a ranking rule edge when the destination node is visited.
    type BuildVisitedFromNode;

    /// Return the label of the given edge condition, to be used when visualising
    /// the ranking rule graph.
    fn label_for_edge_condition(edge: &Self::EdgeCondition) -> String;

    /// Compute the document ids associated with the given edge condition,
    /// restricted to the given universe.
    fn resolve_edge_condition<'search>(
        ctx: &mut SearchContext<'search>,
        edge_condition: &Self::EdgeCondition,
        universe: &RoaringBitmap,
    ) -> Result<RoaringBitmap>;

    /// Prepare to build the edges outgoing from `source_node`.
    ///
    /// This call is followed by zero, one or more calls to [`build_step_visit_destination_node`](RankingRuleGraphTrait::build_step_visit_destination_node),
    /// which builds the actual edges.
    fn build_step_visit_source_node<'search>(
        ctx: &mut SearchContext<'search>,
        source_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>>;

    /// Return the cost and condition of the edges going from the previously visited node
    /// (with [`build_step_visit_source_node`](RankingRuleGraphTrait::build_step_visit_source_node)) to `dest_node`.
    fn build_step_visit_destination_node<'from_data, 'search: 'from_data>(
        ctx: &mut SearchContext<'search>,
        dest_node: &QueryNode,
        source_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Vec<(u8, EdgeCondition<Self::EdgeCondition>)>>;

    fn log_state(
        graph: &RankingRuleGraph<Self>,
        paths: &[Vec<u16>],
        empty_paths_cache: &EmptyPathsCache,
        universe: &RoaringBitmap,
        distances: &[Vec<(u16, SmallBitmap)>],
        cost: u16,
        logger: &mut dyn SearchLogger<QueryGraph>,
    );
}

/// The graph used by graph-based ranking rules.
///
/// It is built on top of a [`QueryGraph`], keeping the same nodes
/// but replacing the edges.
pub struct RankingRuleGraph<G: RankingRuleGraphTrait> {
    pub query_graph: QueryGraph,
    pub edges_store: Vec<Option<Edge<G::EdgeCondition>>>,
    pub edges_of_node: Vec<SmallBitmap>,
}
impl<G: RankingRuleGraphTrait> Clone for RankingRuleGraph<G> {
    fn clone(&self) -> Self {
        Self {
            query_graph: self.query_graph.clone(),
            edges_store: self.edges_store.clone(),
            edges_of_node: self.edges_of_node.clone(),
        }
    }
}
impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    /// Remove the given edge from the ranking rule graph
    pub fn remove_ranking_rule_edge(&mut self, edge_index: u16) {
        let edge_opt = &mut self.edges_store[edge_index as usize];
        let Some(edge) = &edge_opt else { return };
        let (source_node, _dest_node) = (edge.source_node, edge.dest_node);
        *edge_opt = None;

        self.edges_of_node[source_node as usize].remove(edge_index);
    }
}

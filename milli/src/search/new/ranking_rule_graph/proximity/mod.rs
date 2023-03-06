pub mod build;
pub mod compute_docids;

use super::empty_paths_cache::EmptyPathsCache;
use super::{EdgeDetails, RankingRuleGraphTrait};
use crate::new::interner::Interned;
use crate::new::logger::SearchLogger;
use crate::new::query_term::WordDerivations;
use crate::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;
use roaring::RoaringBitmap;

// TODO: intern the strings, refer to them by their pointer?

#[derive(Clone)]
pub enum WordPair {
    Words { left: Interned<String>, right: Interned<String> },
    WordPrefix { left: Interned<String>, right_prefix: Interned<String> },
    WordPrefixSwapped { left_prefix: Interned<String>, right: Interned<String> },
}

#[derive(Clone)]
pub struct ProximityEdge {
    // TODO: use a list of pointers to the word pairs instead?
    pairs: Vec<WordPair>,
    proximity: u8,
}

pub enum ProximityGraph {}

impl RankingRuleGraphTrait for ProximityGraph {
    type EdgeDetails = ProximityEdge;
    type BuildVisitedFromNode = (WordDerivations, i8);

    fn graphviz_edge_details_label(edge: &Self::EdgeDetails) -> String {
        let ProximityEdge { pairs, proximity } = edge;
        format!(", prox {proximity}, {} pairs", pairs.len())
    }

    fn compute_docids<'search>(
        ctx: &mut SearchContext<'search>,
        edge: &Self::EdgeDetails,
    ) -> Result<roaring::RoaringBitmap> {
        compute_docids::compute_docids(ctx, edge)
    }

    fn build_visit_from_node<'search>(
        ctx: &mut SearchContext<'search>,
        from_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>> {
        build::visit_from_node(ctx, from_node)
    }

    fn build_visit_to_node<'from_data, 'search: 'from_data>(
        ctx: &mut SearchContext<'search>,
        to_node: &QueryNode,
        from_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Vec<(u8, EdgeDetails<Self::EdgeDetails>)>> {
        build::visit_to_node(ctx, to_node, from_node_data)
    }

    fn log_state(
        graph: &super::RankingRuleGraph<Self>,
        paths: &[Vec<u32>],
        empty_paths_cache: &EmptyPathsCache,
        universe: &RoaringBitmap,
        distances: &[Vec<u64>],
        cost: u64,
        logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        logger.log_proximity_state(
            graph,
            paths,
            empty_paths_cache,
            universe,
            distances.to_vec(),
            cost,
        );
    }
}

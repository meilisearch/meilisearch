pub mod build;
pub mod compute_docids;

use roaring::RoaringBitmap;

use super::empty_paths_cache::EmptyPathsCache;
use super::{EdgeCondition, RankingRuleGraphTrait};
use crate::search::new::interner::{Interned, Interner};
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_term::WordDerivations;
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum WordPair {
    Words { left: Interned<String>, right: Interned<String> },
    WordPrefix { left: Interned<String>, right_prefix: Interned<String> },
    WordPrefixSwapped { left_prefix: Interned<String>, right: Interned<String> },
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ProximityEdge {
    pairs: Box<[WordPair]>,
    proximity: u8,
}

pub enum ProximityGraph {}

impl RankingRuleGraphTrait for ProximityGraph {
    type EdgeCondition = ProximityEdge;
    type BuildVisitedFromNode = (WordDerivations, i8);

    fn label_for_edge_condition(edge: &Self::EdgeCondition) -> String {
        let ProximityEdge { pairs, proximity } = edge;
        format!(", prox {proximity}, {} pairs", pairs.len())
    }

    fn resolve_edge_condition<'search>(
        ctx: &mut SearchContext<'search>,
        edge: &Self::EdgeCondition,
        universe: &RoaringBitmap,
    ) -> Result<roaring::RoaringBitmap> {
        compute_docids::compute_docids(ctx, edge, universe)
    }

    fn build_step_visit_source_node<'search>(
        ctx: &mut SearchContext<'search>,
        from_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>> {
        build::visit_from_node(ctx, from_node)
    }

    fn build_step_visit_destination_node<'from_data, 'search: 'from_data>(
        ctx: &mut SearchContext<'search>,
        conditions_interner: &mut Interner<Self::EdgeCondition>,
        to_node: &QueryNode,
        from_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Vec<(u8, EdgeCondition<Self::EdgeCondition>)>> {
        build::visit_to_node(ctx, conditions_interner, to_node, from_node_data)
    }

    fn log_state(
        graph: &super::RankingRuleGraph<Self>,
        paths: &[Vec<u16>],
        empty_paths_cache: &EmptyPathsCache,
        universe: &RoaringBitmap,
        distances: &[Vec<(u16, SmallBitmap)>],
        cost: u16,
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

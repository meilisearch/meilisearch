pub mod build;
pub mod compute_docids;

use roaring::RoaringBitmap;

use super::empty_paths_cache::EmptyPathsCache;
use super::{EdgeCondition, RankingRuleGraphTrait};
use crate::search::new::interner::{Interned, Interner};
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_term::Phrase;
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum WordPair {
    Words {
        phrases: Vec<Interned<Phrase>>,
        left: Interned<String>,
        right: Interned<String>,
    },
    WordPrefix {
        phrases: Vec<Interned<Phrase>>,
        left: Interned<String>,
        right_prefix: Interned<String>,
    },
    WordPrefixSwapped {
        left_prefix: Interned<String>,
        right: Interned<String>,
    },
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ProximityEdge {
    pairs: Box<[WordPair]>,
    proximity: u8,
}

pub enum ProximityGraph {}

impl RankingRuleGraphTrait for ProximityGraph {
    type EdgeCondition = ProximityEdge;

    fn label_for_edge_condition(edge: &Self::EdgeCondition) -> String {
        let ProximityEdge { pairs, proximity } = edge;
        format!(", prox {proximity}, {} pairs", pairs.len())
    }

    fn resolve_edge_condition<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        edge: &Self::EdgeCondition,
        universe: &RoaringBitmap,
    ) -> Result<roaring::RoaringBitmap> {
        compute_docids::compute_docids(ctx, edge, universe)
    }

    fn build_edges<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        conditions_interner: &mut Interner<Self::EdgeCondition>,
        source_node: &QueryNode,
        dest_node: &QueryNode,
    ) -> Result<Vec<(u8, EdgeCondition<Self::EdgeCondition>)>> {
        build::build_edges(ctx, conditions_interner, source_node, dest_node)
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

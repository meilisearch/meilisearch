pub mod build;
pub mod compute_docids;

use roaring::RoaringBitmap;

use super::empty_paths_cache::DeadEndPathCache;
use super::{EdgeCondition, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interned, MappedInterner};
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_term::{Phrase, QueryTerm};
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
pub enum ProximityCondition {
    Term { term: Interned<QueryTerm> },
    Pairs { pairs: Box<[WordPair]>, proximity: u8 },
}

pub enum ProximityGraph {}

impl RankingRuleGraphTrait for ProximityGraph {
    type EdgeCondition = ProximityCondition;

    fn label_for_edge_condition(edge: &Self::EdgeCondition) -> String {
        match edge {
            ProximityCondition::Term { term } => {
                format!("term {term}")
            }
            ProximityCondition::Pairs { pairs, proximity } => {
                format!("prox {proximity}, {} pairs", pairs.len())
            }
        }
    }

    fn resolve_edge_condition<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        condition: &Self::EdgeCondition,
        universe: &RoaringBitmap,
    ) -> Result<roaring::RoaringBitmap> {
        compute_docids::compute_docids(ctx, condition, universe)
    }

    fn build_edges<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        conditions_interner: &mut DedupInterner<Self::EdgeCondition>,
        source_node: &QueryNode,
        dest_node: &QueryNode,
    ) -> Result<Vec<(u8, EdgeCondition<Self::EdgeCondition>)>> {
        build::build_edges(ctx, conditions_interner, source_node, dest_node)
    }

    fn log_state(
        graph: &RankingRuleGraph<Self>,
        paths: &[Vec<u16>],
        empty_paths_cache: &DeadEndPathCache<Self>,
        universe: &RoaringBitmap,
        distances: &MappedInterner<Vec<(u16, SmallBitmap<ProximityCondition>)>, QueryNode>,
        cost: u16,
        logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        logger.log_proximity_state(graph, paths, empty_paths_cache, universe, distances, cost);
    }
}

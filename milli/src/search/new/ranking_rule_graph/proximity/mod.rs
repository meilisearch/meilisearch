pub mod build;
pub mod compute_docids;

use heed::RoTxn;
use roaring::RoaringBitmap;

use super::empty_paths_cache::EmptyPathsCache;

use super::{EdgeDetails, RankingRuleGraphTrait};
use crate::new::db_cache::DatabaseCache;
use crate::new::logger::SearchLogger;
use crate::new::query_term::WordDerivations;
use crate::new::{QueryGraph, QueryNode};
use crate::{Index, Result};

// TODO: intern the strings, refer to them by their pointer?

#[derive(Debug, Clone)]
pub enum WordPair {
    Words { left: String, right: String },
    WordsSwapped { left: String, right: String },
    WordPrefix { left: String, right_prefix: String },
    WordPrefixSwapped { left: String, right_prefix: String },
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

    fn compute_docids<'db_cache, 'transaction>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        edge: &Self::EdgeDetails,
    ) -> Result<roaring::RoaringBitmap> {
        compute_docids::compute_docids(index, txn, db_cache, edge)
    }

    fn build_visit_from_node<'transaction>(
        _index: &Index,
        _txn: &'transaction RoTxn,
        _db_cache: &mut DatabaseCache<'transaction>,
        from_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>> {
        build::visit_from_node(from_node)
    }

    fn build_visit_to_node<'from_data, 'transaction: 'from_data>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        to_node: &QueryNode,
        from_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Vec<(u8, EdgeDetails<Self::EdgeDetails>)>> {
        build::visit_to_node(index, txn, db_cache, to_node, from_node_data)
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

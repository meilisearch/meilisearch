mod db_cache;
mod graph_based_ranking_rule;
mod logger;
mod query_graph;
mod query_term;
mod ranking_rule_graph;
mod ranking_rules;
mod resolve_query_graph;
mod sort;
mod words;

use std::collections::BTreeSet;

pub use ranking_rules::{
    apply_ranking_rules, RankingRule, RankingRuleOutput, RankingRuleOutputIter,
    RankingRuleOutputIterWrapper, RankingRuleQueryTrait,
};

use crate::{
    new::query_term::located_query_terms_from_string, Filter, Index, Result, TermsMatchingStrategy,
};
use charabia::Tokenize;
use db_cache::DatabaseCache;
use heed::RoTxn;
use query_graph::{QueryGraph, QueryNode};
use roaring::RoaringBitmap;

use self::{
    logger::SearchLogger,
    resolve_query_graph::{resolve_query_graph, NodeDocIdsCache},
};

pub enum BitmapOrAllRef<'s> {
    Bitmap(&'s RoaringBitmap),
    All,
}

#[allow(clippy::too_many_arguments)]
pub fn resolve_maximally_reduced_query_graph<'transaction>(
    index: &Index,
    txn: &'transaction heed::RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    universe: &RoaringBitmap,
    query_graph: &QueryGraph,
    node_docids_cache: &mut NodeDocIdsCache,
    matching_strategy: TermsMatchingStrategy,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<RoaringBitmap> {
    let mut graph = query_graph.clone();
    let mut positions_to_remove = match matching_strategy {
        TermsMatchingStrategy::Last => {
            let mut all_positions = BTreeSet::new();
            for n in query_graph.nodes.iter() {
                match n {
                    QueryNode::Term(term) => {
                        all_positions.extend(term.positions.clone().into_iter());
                    }
                    QueryNode::Deleted | QueryNode::Start | QueryNode::End => {}
                }
            }
            all_positions.into_iter().collect()
        }
        TermsMatchingStrategy::All => vec![],
    };
    // don't remove the first term
    positions_to_remove.remove(0);
    loop {
        if positions_to_remove.is_empty() {
            break;
        } else {
            let position_to_remove = positions_to_remove.pop().unwrap();
            let _ = graph.remove_words_at_position(position_to_remove);
        }
    }
    logger.query_for_universe(&graph);
    let docids = resolve_query_graph(index, txn, db_cache, node_docids_cache, &graph, universe)?;

    Ok(docids)
}

#[allow(clippy::too_many_arguments)]
pub fn execute_search<'transaction>(
    index: &Index,
    txn: &'transaction RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    query: &str,
    filters: Option<Filter>,
    from: usize,
    length: usize,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<Vec<u32>> {
    assert!(!query.is_empty());
    let query_terms = located_query_terms_from_string(index, txn, query.tokenize(), None).unwrap();
    let graph = QueryGraph::from_query(index, txn, db_cache, query_terms)?;

    logger.initial_query(&graph);

    let universe = if let Some(filters) = filters {
        filters.evaluate(txn, index)?
    } else {
        index.documents_ids(txn)?
    };

    let mut node_docids_cache = NodeDocIdsCache::default();

    let universe = resolve_maximally_reduced_query_graph(
        index,
        txn,
        db_cache,
        &universe,
        &graph,
        &mut node_docids_cache,
        TermsMatchingStrategy::Last,
        logger,
    )?;
    // TODO: create ranking rules here, reuse the node docids cache for the words ranking rule

    logger.initial_universe(&universe);

    apply_ranking_rules(index, txn, db_cache, &graph, &universe, from, length, logger)
}

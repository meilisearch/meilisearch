mod db_cache;
mod graph_based_ranking_rule;
mod interner;
mod logger;
mod query_graph;
mod query_term;
mod ranking_rule_graph;
mod ranking_rules;
mod resolve_query_graph;
mod small_bitmap;
mod sort;
mod words;

pub use logger::{DefaultSearchLogger, SearchLogger};

use std::collections::BTreeSet;

use charabia::Tokenize;
use db_cache::DatabaseCache;
use heed::RoTxn;
use query_graph::{QueryGraph, QueryNode};
pub use ranking_rules::{
    apply_ranking_rules, RankingRule, RankingRuleOutput, RankingRuleOutputIter,
    RankingRuleOutputIterWrapper, RankingRuleQueryTrait,
};
use roaring::RoaringBitmap;

use self::interner::Interner;
use self::query_term::Phrase;
use self::resolve_query_graph::{resolve_query_graph, NodeDocIdsCache};
use crate::search::new::query_term::located_query_terms_from_string;
use crate::{Filter, Index, Result, TermsMatchingStrategy};

pub enum BitmapOrAllRef<'s> {
    Bitmap(&'s RoaringBitmap),
    All,
}

pub struct SearchContext<'search> {
    pub index: &'search Index,
    pub txn: &'search RoTxn<'search>,
    pub db_cache: DatabaseCache<'search>,
    pub word_interner: Interner<String>,
    pub phrase_interner: Interner<Phrase>,
    pub node_docids_cache: NodeDocIdsCache,
}
impl<'search> SearchContext<'search> {
    pub fn new(index: &'search Index, txn: &'search RoTxn<'search>) -> Self {
        Self {
            index,
            txn,
            db_cache: <_>::default(),
            word_interner: <_>::default(),
            phrase_interner: <_>::default(),
            node_docids_cache: <_>::default(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_maximally_reduced_query_graph<'search>(
    ctx: &mut SearchContext<'search>,
    universe: &RoaringBitmap,
    query_graph: &QueryGraph,
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
    let docids = resolve_query_graph(ctx, &graph, universe)?;

    Ok(docids)
}

#[allow(clippy::too_many_arguments)]
pub fn execute_search<'search>(
    ctx: &mut SearchContext<'search>,
    query: &str,
    filters: Option<Filter>,
    from: usize,
    length: usize,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<Vec<u32>> {
    assert!(!query.is_empty());
    let query_terms = located_query_terms_from_string(ctx, query.tokenize(), None)?;
    let graph = QueryGraph::from_query(ctx, query_terms)?;

    logger.initial_query(&graph);

    let universe = if let Some(filters) = filters {
        filters.evaluate(ctx.txn, ctx.index)?
    } else {
        ctx.index.documents_ids(ctx.txn)?
    };

    let universe = resolve_maximally_reduced_query_graph(
        ctx,
        &universe,
        &graph,
        TermsMatchingStrategy::Last,
        logger,
    )?;
    // TODO: create ranking rules here

    logger.initial_universe(&universe);

    apply_ranking_rules(ctx, &graph, &universe, from, length, logger)
}

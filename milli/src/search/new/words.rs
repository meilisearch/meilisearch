use std::collections::BTreeSet;

use heed::RoTxn;
use roaring::RoaringBitmap;

use super::db_cache::DatabaseCache;
use super::logger::SearchLogger;
use super::resolve_query_graph::{resolve_query_graph, NodeDocIdsCache};
use super::{QueryGraph, QueryNode, RankingRule, RankingRuleOutput};
use crate::{Index, Result, TermsMatchingStrategy};

pub struct Words {
    exhausted: bool,
    query_graph: Option<QueryGraph>,
    iterating: bool,
    positions_to_remove: Vec<i8>,
    terms_matching_strategy: TermsMatchingStrategy,
    node_docids_cache: NodeDocIdsCache,
}
impl Words {
    pub fn new(terms_matching_strategy: TermsMatchingStrategy) -> Self {
        Self {
            exhausted: true,
            query_graph: None,
            iterating: false,
            positions_to_remove: vec![],
            terms_matching_strategy,
            node_docids_cache: <_>::default(),
        }
    }
}

impl<'transaction> RankingRule<'transaction, QueryGraph> for Words {
    fn id(&self) -> String {
        "words".to_owned()
    }
    fn start_iteration(
        &mut self,
        _index: &Index,
        _txn: &'transaction RoTxn,
        _db_cache: &mut DatabaseCache<'transaction>,
        logger: &mut dyn SearchLogger<QueryGraph>,
        parent_candidates: &RoaringBitmap,
        parent_query_graph: &QueryGraph,
    ) -> Result<()> {
        // println!("Words: start iteration");
        self.exhausted = false;
        self.query_graph = Some(parent_query_graph.clone());

        // TODO: a phrase can contain many positions, but represents a single node.
        // That's a problem.
        let positions_to_remove = match self.terms_matching_strategy {
            TermsMatchingStrategy::Last => {
                let mut all_positions = BTreeSet::new();
                for n in parent_query_graph.nodes.iter() {
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
        // println!("positions to remove: {positions_to_remove:?}");
        self.positions_to_remove = positions_to_remove;
        self.iterating = true;
        Ok(())
    }

    fn next_bucket(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<QueryGraph>>> {
        // println!("Words: next bucket");
        assert!(self.iterating);
        assert!(universe.len() > 1);

        if self.exhausted {
            return Ok(None);
        }
        let Some(query_graph) = &mut self.query_graph else { panic!() };

        logger.log_words_state(query_graph);

        let this_bucket = resolve_query_graph(
            index,
            txn,
            db_cache,
            &mut self.node_docids_cache,
            query_graph,
            universe,
        )?;

        let child_query_graph = query_graph.clone();
        // TODO: Check whether a position exists in the graph before removing it and
        // returning the next bucket.
        // while graph.does_not_contain(positions_to_remove.last()) { positions_to_remove.pop() }
        if self.positions_to_remove.is_empty() {
            self.exhausted = true;
        } else {
            let position_to_remove = self.positions_to_remove.pop().unwrap();
            query_graph.remove_words_at_position(position_to_remove);
        }

        Ok(Some(RankingRuleOutput { query: child_query_graph, candidates: this_bucket }))
    }

    fn end_iteration(
        &mut self,
        _index: &Index,
        _txn: &'transaction RoTxn,
        _db_cache: &mut DatabaseCache<'transaction>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        // println!("Words: end iteration");
        self.iterating = false;
        self.exhausted = true;
        self.positions_to_remove = vec![];
    }
}

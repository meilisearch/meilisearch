use heed::RoTxn;
use roaring::RoaringBitmap;

use super::db_cache::DatabaseCache;
use super::logger::SearchLogger;
use super::ranking_rule_graph::cheapest_paths::KCheapestPathsState;
use super::ranking_rule_graph::edge_docids_cache::EdgeDocidsCache;
use super::ranking_rule_graph::empty_paths_cache::EmptyPathsCache;
use super::ranking_rule_graph::paths_map::PathsMap;
use super::ranking_rule_graph::{RankingRuleGraph, RankingRuleGraphTrait};
use super::{QueryGraph, RankingRule, RankingRuleOutput};
use crate::new::ranking_rule_graph::cheapest_paths::{self, Path};
use crate::{Index, Result};

pub struct GraphBasedRankingRule<G: RankingRuleGraphTrait> {
    id: String,
    state: Option<GraphBasedRankingRuleState<G>>,
}
impl<G: RankingRuleGraphTrait> GraphBasedRankingRule<G> {
    pub fn new(id: String) -> Self {
        Self { id, state: None }
    }
}

pub struct GraphBasedRankingRuleState<G: RankingRuleGraphTrait> {
    graph: RankingRuleGraph<G>,
    cheapest_paths_state: Option<KCheapestPathsState>,
    edge_docids_cache: EdgeDocidsCache<G>,
    empty_paths_cache: EmptyPathsCache,
}

impl<'transaction, G: RankingRuleGraphTrait> RankingRule<'transaction, QueryGraph>
    for GraphBasedRankingRule<G>
{
    fn id(&self) -> String {
        self.id.clone()
    }
    fn start_iteration(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &RoaringBitmap,
        query_graph: &QueryGraph,
    ) -> Result<()> {
        // TODO: update old state instead of starting from scratch
        let graph = RankingRuleGraph::build(index, txn, db_cache, query_graph.clone())?;

        let cheapest_paths_state = KCheapestPathsState::new(&graph);
        let state = GraphBasedRankingRuleState {
            graph,
            cheapest_paths_state,
            edge_docids_cache: <_>::default(),
            empty_paths_cache: <_>::default(),
        };

        self.state = Some(state);

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
        assert!(universe.len() > 1);
        let mut state = self.state.take().unwrap();

        let Some(cheapest_paths_state) = state.cheapest_paths_state.take() else {
            return Ok(None);
        };

        let mut paths = PathsMap::default();

        if let Some(next_cheapest_paths_state) = cheapest_paths_state
            .compute_paths_of_next_lowest_cost(
                &mut state.graph,
                &state.empty_paths_cache,
                &mut paths,
            )
        {
            state.cheapest_paths_state = Some(next_cheapest_paths_state);
        } else {
            state.cheapest_paths_state = None;
        }

        if paths.is_empty() {
            self.state = None;
            return Ok(None);
        }

        G::log_state(&state.graph, &paths, logger);

        let bucket = state.graph.resolve_paths(
            index,
            txn,
            db_cache,
            &mut state.edge_docids_cache,
            &mut state.empty_paths_cache,
            universe,
            paths,
        )?;

        let next_query_graph = state.graph.query_graph.clone();

        self.state = Some(state);

        Ok(Some(RankingRuleOutput { query: next_query_graph, candidates: bucket }))
    }

    fn end_iteration(
        &mut self,
        _index: &Index,
        _txn: &'transaction RoTxn,
        _db_cache: &mut DatabaseCache<'transaction>,
        logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        self.state = None;
    }
}

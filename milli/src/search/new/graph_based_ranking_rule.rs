use heed::RoTxn;
use roaring::RoaringBitmap;

use super::db_cache::DatabaseCache;
use super::logger::SearchLogger;
use super::ranking_rule_graph::edge_docids_cache::EdgeDocidsCache;
use super::ranking_rule_graph::empty_paths_cache::EmptyPathsCache;

use super::ranking_rule_graph::{RankingRuleGraph, RankingRuleGraphTrait};
use super::{BitmapOrAllRef, QueryGraph, RankingRule, RankingRuleOutput};

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
    edge_docids_cache: EdgeDocidsCache<G>,
    empty_paths_cache: EmptyPathsCache,
    all_distances: Vec<Vec<u64>>,
    cur_distance_idx: usize,
}

fn remove_empty_edges<'transaction, G: RankingRuleGraphTrait>(
    graph: &mut RankingRuleGraph<G>,
    edge_docids_cache: &mut EdgeDocidsCache<G>,
    index: &Index,
    txn: &'transaction RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    universe: &RoaringBitmap,
    empty_paths_cache: &mut EmptyPathsCache,
) -> Result<()> {
    for edge_index in 0..graph.all_edges.len() as u32 {
        if graph.all_edges[edge_index as usize].is_none() {
            continue;
        }
        let docids = edge_docids_cache
            .get_edge_docids(index, txn, db_cache, edge_index, &*graph, universe)?;
        match docids {
            BitmapOrAllRef::Bitmap(bitmap) => {
                if bitmap.is_disjoint(universe) {
                    graph.remove_edge(edge_index);
                    empty_paths_cache.forbid_edge(edge_index);
                    edge_docids_cache.cache.remove(&edge_index);
                    continue;
                }
            }
            BitmapOrAllRef::All => continue,
        }
    }
    Ok(())
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
        _logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &RoaringBitmap,
        query_graph: &QueryGraph,
    ) -> Result<()> {
        // TODO: update old state instead of starting from scratch
        let mut graph = RankingRuleGraph::build(index, txn, db_cache, query_graph.clone())?;
        let mut edge_docids_cache = EdgeDocidsCache::default();
        let mut empty_paths_cache = EmptyPathsCache::new(graph.all_edges.len());

        remove_empty_edges(
            &mut graph,
            &mut edge_docids_cache,
            index,
            txn,
            db_cache,
            universe,
            &mut empty_paths_cache,
        )?;
        let all_distances = graph.initialize_distances_cheapest();

        let state = GraphBasedRankingRuleState {
            graph,
            edge_docids_cache,
            empty_paths_cache,
            all_distances,
            cur_distance_idx: 0,
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
        remove_empty_edges(
            &mut state.graph,
            &mut state.edge_docids_cache,
            index,
            txn,
            db_cache,
            universe,
            &mut state.empty_paths_cache,
        )?;

        if state.cur_distance_idx
            >= state.all_distances[state.graph.query_graph.root_node as usize].len()
        {
            self.state = None;
            return Ok(None);
        }
        let cost =
            state.all_distances[state.graph.query_graph.root_node as usize][state.cur_distance_idx];
        state.cur_distance_idx += 1;

        let paths = state.graph.paths_of_cost(
            state.graph.query_graph.root_node as usize,
            cost,
            &state.all_distances,
            &state.empty_paths_cache,
        );

        G::log_state(
            &state.graph,
            &paths,
            &state.empty_paths_cache,
            universe,
            &state.all_distances,
            cost,
            logger,
        );

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
        _logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        self.state = None;
    }
}

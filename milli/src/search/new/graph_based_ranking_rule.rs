use heed::RoTxn;
use roaring::RoaringBitmap;

use crate::{
    new::ranking_rule_graph::cheapest_paths::{self, Path},
    Index, Result,
};

use super::{
    db_cache::DatabaseCache,
    ranking_rule_graph::{
        cheapest_paths::KCheapestPathsState, edge_docids_cache::EdgeDocidsCache,
        empty_paths_cache::EmptyPathsCache, paths_map::PathsMap, RankingRuleGraph,
        RankingRuleGraphTrait,
    },
    QueryGraph, RankingRule, RankingRuleOutput,
};

pub struct GraphBasedRankingRule<G: RankingRuleGraphTrait> {
    state: Option<GraphBasedRankingRuleState<G>>,
}
impl<G: RankingRuleGraphTrait> Default for GraphBasedRankingRule<G> {
    fn default() -> Self {
        Self { state: None }
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
    fn start_iteration(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        universe: &RoaringBitmap,
        query_graph: &QueryGraph,
    ) -> Result<()> {
        // if let Some(state) = &mut self.state {
        //     // TODO: update the previous state
        //     // TODO: update the existing graph incrementally, based on a diff

        // } else {
        let graph = RankingRuleGraph::build(index, txn, db_cache, query_graph.clone())?;
        // println!("Initialized Proximity Ranking Rule.");
        // println!("GRAPH:");
        // let graphviz = graph.graphviz();
        // println!("{graphviz}");

        let cheapest_paths_state = KCheapestPathsState::new(&graph);
        let state = GraphBasedRankingRuleState {
            graph,
            cheapest_paths_state,
            edge_docids_cache: <_>::default(),
            empty_paths_cache: <_>::default(),
        };

        // let desc = state.graph.graphviz_with_path(
        //     &state.cheapest_paths_state.as_ref().unwrap().kth_cheapest_path.clone(),
        // );
        // println!("Cheapest path: {desc}");

        self.state = Some(state);
        // }

        Ok(())
    }

    fn next_bucket(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<QueryGraph>>> {
        assert!(universe.len() > 1);
        let mut state = self.state.take().unwrap();

        let Some(cheapest_paths_state) = state.cheapest_paths_state.take() else {
            return Ok(None);
        };
        // println!("Proximity: Next Bucket");

        let mut paths = PathsMap::default();

        // let desc = state.graph.dot_description_with_path(&cheapest_paths_state.kth_cheapest_path);
        // println!("CHeapest Path: {desc}");
        // TODO: when does it return None? -> when there is no cheapest path
        // How to handle it? -> ... return all document ids from the universe?
        //
        // TODO: Give an empty_edge and empty_prefix argument to the
        // compute_paths_of_next_lowest_cost function
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
            // If returns None if there are no longer any paths to compute
            // BUT! paths_map may not be empty, and we need to compute the current bucket still
        }

        // println!("PATHS: {}", paths.graphviz(&state.graph));

        // paths.iterate(|path, cost| {
        //     let desc = state.graph.graphviz_with_path(&Path { edges: path.clone(), cost: *cost });
        //     println!("Path to resolve of cost {cost}: {desc}");
        // });

        // let desc = state.graph.dot_description_with_path(
        //     &state.cheapest_paths_state.as_ref().unwrap().kth_cheapest_path.clone(),
        // );
        // println!("Cheapest path: {desc}");

        // TODO: verify that this is correct
        // If the paths are empty, we should probably return the universe?
        // BUT! Is there a case where the paths are empty AND the universe is
        // not empty?
        if paths.is_empty() {
            self.state = None;
            return Ok(None);
        }
        // Here, log all the paths?

        let bucket = state.graph.resolve_paths(
            index,
            txn,
            db_cache,
            &mut state.edge_docids_cache,
            &mut state.empty_paths_cache,
            universe,
            paths,
        )?;
        // The call above also updated the graph such that it doesn't contain the empty edges anymore.
        // println!("Resolved all the paths: {bucket:?} from universe {:?}", state.universe);
        // let graphviz = state.graph.graphviz();
        // println!("{graphviz}");

        let next_query_graph = state.graph.query_graph.clone();

        self.state = Some(state);

        Ok(Some(RankingRuleOutput { query: next_query_graph, candidates: bucket }))
    }

    fn end_iteration(
        &mut self,
        _index: &Index,
        _txn: &'transaction RoTxn,
        _db_cache: &mut DatabaseCache<'transaction>,
    ) {
        // println!("PROXIMITY: end iteration");
        self.state = None;
    }
}

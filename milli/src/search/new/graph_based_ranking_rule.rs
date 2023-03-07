use super::logger::SearchLogger;
use super::ranking_rule_graph::EdgeDocidsCache;
use super::ranking_rule_graph::EmptyPathsCache;
use super::ranking_rule_graph::{RankingRuleGraph, RankingRuleGraphTrait};
use super::SearchContext;
use super::{BitmapOrAllRef, QueryGraph, RankingRule, RankingRuleOutput};
use crate::Result;
use roaring::RoaringBitmap;

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
    all_distances: Vec<Vec<u16>>,
    cur_distance_idx: usize,
}

fn remove_empty_edges<'search, G: RankingRuleGraphTrait>(
    ctx: &mut SearchContext<'search>,
    graph: &mut RankingRuleGraph<G>,
    edge_docids_cache: &mut EdgeDocidsCache<G>,
    universe: &RoaringBitmap,
    empty_paths_cache: &mut EmptyPathsCache,
) -> Result<()> {
    for edge_index in 0..graph.all_edges.len() as u16 {
        if graph.all_edges[edge_index as usize].is_none() {
            continue;
        }
        let docids = edge_docids_cache.get_edge_docids(ctx, edge_index, &*graph, universe)?;
        match docids {
            BitmapOrAllRef::Bitmap(docids) => {
                if docids.is_disjoint(universe) {
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

impl<'search, G: RankingRuleGraphTrait> RankingRule<'search, QueryGraph>
    for GraphBasedRankingRule<G>
{
    fn id(&self) -> String {
        self.id.clone()
    }
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'search>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &RoaringBitmap,
        query_graph: &QueryGraph,
    ) -> Result<()> {
        // TODO: update old state instead of starting from scratch
        let mut graph = RankingRuleGraph::build(ctx, query_graph.clone())?;
        let mut edge_docids_cache = EdgeDocidsCache::default();
        let mut empty_paths_cache = EmptyPathsCache::new(graph.all_edges.len() as u16);

        remove_empty_edges(
            ctx,
            &mut graph,
            &mut edge_docids_cache,
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
        ctx: &mut SearchContext<'search>,
        logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<QueryGraph>>> {
        assert!(universe.len() > 1);
        let mut state = self.state.take().unwrap();
        remove_empty_edges(
            ctx,
            &mut state.graph,
            &mut state.edge_docids_cache,
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

        let mut bucket = RoaringBitmap::new();

        let GraphBasedRankingRuleState {
            graph,
            edge_docids_cache,
            empty_paths_cache,
            all_distances,
            cur_distance_idx: _,
        } = &mut state;

        let mut paths = vec![];
        let original_universe = universe;
        let mut universe = universe.clone();

        graph.visit_paths_of_cost(
            graph.query_graph.root_node as usize,
            cost,
            all_distances,
            empty_paths_cache,
            |path, graph, empty_paths_cache| {
                let mut path_docids = universe.clone();
                let mut visited_edges = vec![];
                let mut cached_edge_docids = vec![];
                for &edge_index in path {
                    visited_edges.push(edge_index);
                    let edge_docids =
                        edge_docids_cache.get_edge_docids(ctx, edge_index, graph, &universe)?;
                    let edge_docids = match edge_docids {
                        BitmapOrAllRef::Bitmap(b) => b,
                        BitmapOrAllRef::All => continue,
                    };
                    cached_edge_docids.push((edge_index, edge_docids.clone()));
                    if edge_docids.is_disjoint(&universe) {
                        // 1. Store in the cache that this edge is empty for this universe
                        empty_paths_cache.forbid_edge(edge_index);
                        // 2. remove this edge from the ranking rule graph
                        graph.remove_edge(edge_index);
                        edge_docids_cache.cache.remove(&edge_index);
                        return Ok(());
                    }
                    path_docids &= edge_docids;

                    if path_docids.is_disjoint(&universe) {
                        empty_paths_cache.forbid_prefix(&visited_edges);
                        // if the intersection between this edge and any
                        // previous one is disjoint with the universe,
                        // then we add these two edges to the empty_path_cache
                        for (edge_index2, edge_docids2) in
                            cached_edge_docids[..cached_edge_docids.len() - 1].iter()
                        {
                            let intersection = edge_docids & edge_docids2;
                            if intersection.is_disjoint(&universe) {
                                // needs_filtering_empty_couple_edges = true;
                                empty_paths_cache.forbid_couple_edges(*edge_index2, edge_index);
                            }
                        }
                        return Ok(());
                    }
                }
                paths.push(path.to_vec());
                bucket |= &path_docids;
                universe -= path_docids;
                Ok(())
            },
        )?;

        G::log_state(
            &state.graph,
            &paths,
            &state.empty_paths_cache,
            original_universe,
            &state.all_distances,
            cost,
            logger,
        );

        let next_query_graph = state.graph.query_graph.clone();

        self.state = Some(state);

        Ok(Some(RankingRuleOutput { query: next_query_graph, candidates: bucket }))
    }

    fn end_iteration(
        &mut self,
        _ctx: &mut SearchContext<'search>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        self.state = None;
    }
}

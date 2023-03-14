/*! Implementation of a generic graph-based ranking rule.

A graph-based ranking rule is a ranking rule that works by representing
its possible operations and their relevancy cost as a directed acyclic multi-graph
built on top of the query graph. It then computes its buckets by finding the
cheapest paths from the start node to the end node and computing the document ids
that satisfy those paths.

For example, the proximity ranking rule builds a graph where the edges between two
nodes represent a condition that the term of the source node is in a certain proximity
to the term of the destination node. With the query "pretty house by" where the term
"pretty" has three possible proximities to the term "house" and "house" has two
proximities to "by", the graph will look like this:

```txt
┌───────┐     ┌───────┐─────1────▶┌───────┐──1──▶┌─────┐    ┌───────┐
│ START │──0─▶│pretty │─────2────▶│ house │      │ by  │─0─▶│  END  │
└───────┘     └───────┘─────3────▶└───────┘──2-─▶└─────┘    └───────┘
```
The proximity ranking rule's first bucket will be determined by the union of all
the shortest paths from START to END, which in this case is:
```txt
START --0-> pretty --1--> house --1--> by --0--> end
```
The path's corresponding document ids are found by taking the intersection of the
document ids of each edge. That is, we find the documents where both `pretty` is
1-close to `house` AND `house` is 1-close to `by`.

For the second bucket, we get the union of the second-cheapest paths, which are:
```txt
START --0-> pretty --1--> house --2--> by --0--> end
START --0-> pretty --2--> house --1--> by --0--> end
```
That is we find the documents where either:
- `pretty` is 1-close to `house` AND `house` is 2-close to `by`
- OR: `pretty` is 2-close to `house` AND `house` is 1-close to `by`
*/

use roaring::RoaringBitmap;

use super::interner::MappedInterner;
use super::logger::SearchLogger;
use super::query_graph::QueryNode;
use super::ranking_rule_graph::{
    DeadEndPathCache, EdgeCondition, EdgeConditionDocIdsCache, ProximityGraph, RankingRuleGraph,
    RankingRuleGraphTrait, TypoGraph,
};
use super::small_bitmap::SmallBitmap;
use super::{QueryGraph, RankingRule, RankingRuleOutput, SearchContext};
use crate::search::new::interner::Interned;
use crate::Result;

pub type Proximity = GraphBasedRankingRule<ProximityGraph>;
impl Default for GraphBasedRankingRule<ProximityGraph> {
    fn default() -> Self {
        Self::new("proximity".to_owned())
    }
}
pub type Typo = GraphBasedRankingRule<TypoGraph>;
impl Default for GraphBasedRankingRule<TypoGraph> {
    fn default() -> Self {
        Self::new("typo".to_owned())
    }
}

/// A generic graph-based ranking rule
pub struct GraphBasedRankingRule<G: RankingRuleGraphTrait> {
    id: String,
    // When the ranking rule is not iterating over its buckets,
    // its state is `None`.
    state: Option<GraphBasedRankingRuleState<G>>,
}
impl<G: RankingRuleGraphTrait> GraphBasedRankingRule<G> {
    /// Creates the ranking rule with the given identifier
    pub fn new(id: String) -> Self {
        Self { id, state: None }
    }
}

/// The internal state of a graph-based ranking rule during iteration
pub struct GraphBasedRankingRuleState<G: RankingRuleGraphTrait> {
    /// The current graph
    graph: RankingRuleGraph<G>,
    /// Cache to retrieve the docids associated with each edge
    edge_conditions_cache: EdgeConditionDocIdsCache<G>,
    /// Cache used to optimistically discard paths that resolve to no documents.
    empty_paths_cache: DeadEndPathCache<G>,
    /// A structure giving the list of possible costs from each node to the end node,
    /// along with a set of unavoidable edges that must be traversed to achieve that distance.
    all_distances: MappedInterner<Vec<(u16, SmallBitmap<G::EdgeCondition>)>, QueryNode>,
    /// An index in the first element of `all_distances`, giving the cost of the next bucket
    cur_distance_idx: usize,
}

/// Traverse each edge of the graph, computes its associated document ids,
/// and remove this edge from the graph if its docids are disjoint with the
/// given universe.
fn remove_empty_edges<'ctx, G: RankingRuleGraphTrait>(
    ctx: &mut SearchContext<'ctx>,
    graph: &mut RankingRuleGraph<G>,
    edge_docids_cache: &mut EdgeConditionDocIdsCache<G>,
    universe: &RoaringBitmap,
    empty_paths_cache: &mut DeadEndPathCache<G>,
) -> Result<()> {
    for edge_id in graph.edges_store.indexes() {
        let Some(edge) = graph.edges_store.get(edge_id).as_ref() else {
            continue;
        };
        let condition = edge.condition;

        match condition {
            EdgeCondition::Unconditional => continue,
            EdgeCondition::Conditional(condition) => {
                let docids = edge_docids_cache.get_edge_docids(ctx, condition, graph, universe)?;
                if docids.is_disjoint(universe) {
                    graph.remove_edges_with_condition(condition);
                    empty_paths_cache.add_condition(condition);
                    edge_docids_cache.cache.remove(&condition);
                    continue;
                }
            }
        }
    }
    Ok(())
}

impl<'ctx, G: RankingRuleGraphTrait> RankingRule<'ctx, QueryGraph> for GraphBasedRankingRule<G> {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &RoaringBitmap,
        query_graph: &QueryGraph,
    ) -> Result<()> {
        let mut graph = RankingRuleGraph::build(ctx, query_graph.clone())?;
        let mut edge_docids_cache = EdgeConditionDocIdsCache::default();
        let mut empty_paths_cache = DeadEndPathCache::new(&graph.conditions_interner);

        // First simplify the graph as much as possible, by computing the docids of the edges
        // within the rule's universe and removing the edges that have no associated docids.
        remove_empty_edges(
            ctx,
            &mut graph,
            &mut edge_docids_cache,
            universe,
            &mut empty_paths_cache,
        )?;

        // Then pre-compute the cost of all paths from each node to the end node
        let all_distances = graph.initialize_distances_with_necessary_edges();

        let state = GraphBasedRankingRuleState {
            graph,
            edge_conditions_cache: edge_docids_cache,
            empty_paths_cache,
            all_distances,
            cur_distance_idx: 0,
        };

        self.state = Some(state);

        Ok(())
    }

    fn next_bucket(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<QueryGraph>>> {
        // If universe.len() <= 1, the bucket sort algorithm
        // should not have called this function.
        assert!(universe.len() > 1);
        // Will crash if `next_bucket` is called before `start_iteration` or after `end_iteration`,
        // should never happen
        let mut state = self.state.take().unwrap();

        // TODO: does this have a real positive performance impact?
        remove_empty_edges(
            ctx,
            &mut state.graph,
            &mut state.edge_conditions_cache,
            universe,
            &mut state.empty_paths_cache,
        )?;

        // If the cur_distance_idx does not point to a valid cost in the `all_distances`
        // structure, then we have computed all the buckets and can return.
        if state.cur_distance_idx
            >= state.all_distances.get(state.graph.query_graph.root_node).len()
        {
            self.state = None;
            return Ok(None);
        }

        // Retrieve the cost of the paths to compute
        let (cost, _) =
            state.all_distances.get(state.graph.query_graph.root_node)[state.cur_distance_idx];
        state.cur_distance_idx += 1;

        let mut bucket = RoaringBitmap::new();

        let GraphBasedRankingRuleState {
            graph,
            edge_conditions_cache: edge_docids_cache,
            empty_paths_cache,
            all_distances,
            cur_distance_idx: _,
        } = &mut state;

        let original_universe = universe;
        let mut universe = universe.clone();

        // TODO: remove this unnecessary clone
        let original_graph = graph.clone();
        // and this vector as well
        let mut paths = vec![];

        // For each path of the given cost, we will compute its associated
        // document ids.
        // In case the path does not resolve to any document id, we try to figure out why
        // and update the `empty_paths_cache` accordingly.
        // For example, it may be that the path is empty because one of its edges is disjoint
        // with the universe, or because a prefix of the path is disjoint with the universe, or because
        // the path contains two edges that are disjoint from each other within the universe.
        // Updating the empty_paths_cache helps speed up the execution of `visit_paths_of_cost` and reduces
        // the number of future candidate paths given by that same function.
        graph.visit_paths_of_cost(
            graph.query_graph.root_node,
            cost,
            all_distances,
            empty_paths_cache,
            |path, graph, empty_paths_cache| {
                // Accumulate the path for logging purposes only
                paths.push(path.to_vec());
                let mut path_docids = universe.clone();

                // We store the edges and their docids in vectors in case the path turns out to be
                // empty and we need to figure out why it was empty.
                let mut visited_conditions = vec![];
                let mut cached_edge_docids =
                    graph.conditions_interner.map(|_| RoaringBitmap::new());

                for &condition_interned_raw in path {
                    let condition = Interned::new(condition_interned_raw);
                    visited_conditions.push(condition_interned_raw);

                    let edge_docids =
                        edge_docids_cache.get_edge_docids(ctx, condition, graph, &universe)?;

                    *cached_edge_docids.get_mut(condition) = edge_docids.clone();

                    // If the edge is empty, then the path will be empty as well, we update the graph
                    // and caches accordingly and skip to the next candidate path.
                    if edge_docids.is_disjoint(&universe) {
                        // 1. Store in the cache that this edge is empty for this universe
                        empty_paths_cache.add_condition(condition);
                        // 2. remove this edge from the ranking rule graph
                        // ouch, no! :( need to link a condition to one or more ranking rule edges
                        graph.remove_edges_with_condition(condition);
                        // 3. Also remove the entry from the edge_docids_cache, since we don't need it anymore
                        edge_docids_cache.cache.remove(&condition);
                        return Ok(());
                    }
                    path_docids &= edge_docids;

                    // If the (sub)path is empty, we try to figure out why and update the caches accordingly.
                    if path_docids.is_disjoint(&universe) {
                        // First, we know that this path is empty, and thus any path
                        // that is a superset of it will also be empty.
                        empty_paths_cache.add_prefix(&visited_conditions);
                        // Second, if the intersection between this edge and any
                        // previous one is disjoint with the universe,
                        // then we also know that any path containing the same couple of
                        // edges will also be empty.
                        for (past_condition, edge_docids2) in cached_edge_docids.iter() {
                            if past_condition == condition {
                                continue;
                            };
                            let intersection = edge_docids & edge_docids2;
                            if intersection.is_disjoint(&universe) {
                                empty_paths_cache.add_condition_couple(past_condition, condition);
                            }
                        }
                        // We should maybe instead try to compute:
                        // 0th & nth & 1st & n-1th & 2nd & etc...
                        return Ok(());
                    }
                }
                bucket |= &path_docids;
                // Reduce the size of the universe so that we can more optimistically discard candidate paths
                universe -= path_docids;
                // TODO: if the universe is empty, stop iterating
                Ok(())
            },
        )?;

        G::log_state(
            &original_graph,
            &paths,
            &state.empty_paths_cache,
            original_universe,
            &state.all_distances,
            cost,
            logger,
        );

        // TODO: Graph-based ranking rules do not (yet) modify the query graph. We could, however,
        // remove nodes and/or terms within nodes that weren't present in any of the paths.
        let next_query_graph = state.graph.query_graph.clone();

        self.state = Some(state);

        Ok(Some(RankingRuleOutput { query: next_query_graph, candidates: bucket }))
    }

    fn end_iteration(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        self.state = None;
    }
}

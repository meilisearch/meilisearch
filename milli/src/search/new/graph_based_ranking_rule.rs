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

use std::collections::HashSet;
use std::ops::ControlFlow;

use roaring::RoaringBitmap;

use super::interner::MappedInterner;
use super::logger::SearchLogger;
use super::query_graph::QueryNode;
use super::ranking_rule_graph::{
    ConditionDocIdsCache, DeadEndsCache, ProximityGraph, RankingRuleGraph, RankingRuleGraphTrait,
    TypoGraph,
};
use super::small_bitmap::SmallBitmap;
use super::{QueryGraph, RankingRule, RankingRuleOutput, SearchContext};
use crate::search::new::query_graph::QueryNodeData;
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
    conditions_cache: ConditionDocIdsCache<G>,
    /// Cache used to optimistically discard paths that resolve to no documents.
    dead_ends_cache: DeadEndsCache<G::Condition>,
    /// A structure giving the list of possible costs from each node to the end node
    all_distances: MappedInterner<QueryNode, Vec<u16>>,
    /// An index in the first element of `all_distances`, giving the cost of the next bucket
    cur_distance_idx: usize,
}

impl<'ctx, G: RankingRuleGraphTrait> RankingRule<'ctx, QueryGraph> for GraphBasedRankingRule<G> {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
        _universe: &RoaringBitmap,
        query_graph: &QueryGraph,
    ) -> Result<()> {
        let graph = RankingRuleGraph::build(ctx, query_graph.clone())?;
        let condition_docids_cache = ConditionDocIdsCache::default();
        let dead_ends_cache = DeadEndsCache::new(&graph.conditions_interner);

        // Then pre-compute the cost of all paths from each node to the end node
        let all_distances = graph.initialize_distances_with_necessary_edges();

        let state = GraphBasedRankingRuleState {
            graph,
            conditions_cache: condition_docids_cache,
            dead_ends_cache,
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

        // If the cur_distance_idx does not point to a valid cost in the `all_distances`
        // structure, then we have computed all the buckets and can return.
        if state.cur_distance_idx
            >= state.all_distances.get(state.graph.query_graph.root_node).len()
        {
            self.state = None;
            return Ok(None);
        }

        // Retrieve the cost of the paths to compute
        let cost =
            state.all_distances.get(state.graph.query_graph.root_node)[state.cur_distance_idx];
        state.cur_distance_idx += 1;

        let mut bucket = RoaringBitmap::new();

        let GraphBasedRankingRuleState {
            graph,
            conditions_cache: condition_docids_cache,
            dead_ends_cache,
            all_distances,
            cur_distance_idx: _,
        } = &mut state;

        let original_universe = universe;
        let mut universe = universe.clone();

        let original_graph = graph.clone();
        let mut used_conditions = SmallBitmap::for_interned_values_in(&graph.conditions_interner);
        let mut considered_paths = vec![];
        let mut good_paths = vec![];

        // For each path of the given cost, we will compute its associated
        // document ids.
        // In case the path does not resolve to any document id, we try to figure out why
        // and update the `dead_ends_cache` accordingly.
        // Updating the dead_ends_cache helps speed up the execution of `visit_paths_of_cost` and reduces
        // the number of future candidate paths given by that same function.
        graph.visit_paths_of_cost(
            graph.query_graph.root_node,
            cost,
            all_distances,
            dead_ends_cache,
            |path, graph, dead_ends_cache| {
                if universe.is_empty() {
                    return Ok(ControlFlow::Break(()));
                }
                // Accumulate the path for logging purposes only
                considered_paths.push(path.to_vec());

                let mut path_docids = universe.clone();

                // We store the edges and their docids in vectors in case the path turns out to be
                // empty and we need to figure out why it was empty.
                let mut visited_conditions = vec![];
                // let mut cached_condition_docids = vec![];
                let mut subpath_docids = vec![];

                for (latest_condition_path_idx, &latest_condition) in path.iter().enumerate() {
                    visited_conditions.push(latest_condition);

                    let condition_docids = condition_docids_cache.get_condition_docids(
                        ctx,
                        latest_condition,
                        graph,
                        &universe,
                    )?;

                    // If the edge is empty, then the path will be empty as well, we update the graph
                    // and caches accordingly and skip to the next candidate path.
                    if condition_docids.is_empty() {
                        // 1. Store in the cache that this edge is empty for this universe
                        dead_ends_cache.forbid_condition(latest_condition);
                        // 2. remove all the edges with this condition from the ranking rule graph
                        graph.remove_edges_with_condition(latest_condition);
                        return Ok(ControlFlow::Continue(()));
                    }
                    path_docids &= condition_docids;
                    subpath_docids.push(path_docids.clone());

                    // If the (sub)path is empty, we try to figure out why and update the caches accordingly.
                    if path_docids.is_empty() {
                        let len_prefix = subpath_docids.len() - 1;
                        // First, we know that this path is empty, and thus any path
                        // that is a superset of it will also be empty.
                        dead_ends_cache.forbid_condition_after_prefix(
                            visited_conditions[..len_prefix].iter().copied(),
                            latest_condition,
                        );

                        if visited_conditions.len() > 1 {
                            let mut subprefix = vec![];
                            // Deadend if the intersection between this edge and any
                            // previous prefix is disjoint with the universe
                            for (past_condition, subpath_docids) in visited_conditions[..len_prefix]
                                .iter()
                                .zip(subpath_docids[..len_prefix].iter())
                            {
                                if *past_condition == latest_condition {
                                    todo!();
                                };
                                subprefix.push(*past_condition);
                                if condition_docids.is_disjoint(subpath_docids) {
                                    dead_ends_cache.forbid_condition_after_prefix(
                                        subprefix.iter().copied(),
                                        latest_condition,
                                    );
                                }
                            }

                            // keep the same prefix and check the intersection with
                            // all the remaining conditions
                            let mut forbidden = dead_ends_cache.forbidden.clone();
                            let mut cursor = dead_ends_cache;
                            for &c in visited_conditions[..len_prefix].iter() {
                                cursor = cursor.advance(c).unwrap();
                                forbidden.union(&cursor.forbidden);
                            }

                            let past_path_docids = &subpath_docids[subpath_docids.len() - 2];

                            let remaining_conditions =
                                path[latest_condition_path_idx..].iter().skip(1);
                            for next_condition in remaining_conditions {
                                if forbidden.contains(*next_condition) {
                                    continue;
                                }
                                let next_condition_docids = condition_docids_cache
                                    .get_condition_docids(ctx, *next_condition, graph, &universe)?;

                                if past_path_docids.is_disjoint(next_condition_docids) {
                                    cursor.forbid_condition(*next_condition);
                                }
                            }
                        }

                        return Ok(ControlFlow::Continue(()));
                    }
                }
                assert!(!path_docids.is_empty());
                // Accumulate the path for logging purposes only
                good_paths.push(path.to_vec());
                for condition in path {
                    used_conditions.insert(*condition);
                }
                bucket |= &path_docids;
                // Reduce the size of the universe so that we can more optimistically discard candidate paths
                universe -= path_docids;

                if universe.is_empty() {
                    Ok(ControlFlow::Break(()))
                } else {
                    Ok(ControlFlow::Continue(()))
                }
            },
        )?;
        // println!("  {} paths of cost {} in {}", paths.len(), cost, self.id);
        G::log_state(
            &original_graph,
            &good_paths,
            dead_ends_cache,
            original_universe,
            all_distances,
            cost,
            logger,
        );

        // We modify the next query graph so that it only contains the subgraph
        // that was used to compute this bucket
        // But we only do it in case the bucket length is >1, because otherwise
        // we know the child ranking rule won't be called anyway
        let mut next_query_graph = original_graph.query_graph;
        if bucket.len() > 1 {
            next_query_graph.simplify();
            // 1. Gather all the words and phrases used in the computation of this bucket
            let mut used_words = HashSet::new();
            let mut used_phrases = HashSet::new();
            for condition in used_conditions.iter() {
                let (ws, ps) =
                    condition_docids_cache.get_condition_used_words_and_phrases(condition);
                used_words.extend(ws);
                used_phrases.extend(ps);
            }
            // 2. Remove the unused words and phrases from all the nodes in the graph
            let mut nodes_to_remove = vec![];
            for (node_id, node) in next_query_graph.nodes.iter_mut() {
                let term = match &mut node.data {
                    QueryNodeData::Term(term) => term,
                    QueryNodeData::Deleted | QueryNodeData::Start | QueryNodeData::End => continue,
                };
                if let Some(new_term) = ctx
                    .term_interner
                    .get(term.value)
                    .removing_forbidden_terms(&used_words, &used_phrases)
                {
                    if new_term.is_empty() {
                        nodes_to_remove.push(node_id);
                    } else {
                        term.value = ctx.term_interner.insert(new_term);
                    }
                }
            }
            // 3. Remove the empty nodes from the graph
            next_query_graph.remove_nodes(&nodes_to_remove);
        }

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

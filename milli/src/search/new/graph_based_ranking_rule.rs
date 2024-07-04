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

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use roaring::RoaringBitmap;

use super::interner::{Interned, MappedInterner};
use super::logger::SearchLogger;
use super::query_graph::QueryNode;
use super::ranking_rule_graph::{
    ConditionDocIdsCache, DeadEndsCache, ExactnessGraph, FidGraph, PositionGraph, ProximityGraph,
    RankingRuleGraph, RankingRuleGraphTrait, TypoGraph, WordsGraph,
};
use super::small_bitmap::SmallBitmap;
use super::{QueryGraph, RankingRule, RankingRuleOutput, SearchContext};
use crate::score_details::Rank;
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::ranking_rule_graph::PathVisitor;
use crate::{Result, TermsMatchingStrategy};

pub type Words = GraphBasedRankingRule<WordsGraph>;
impl GraphBasedRankingRule<WordsGraph> {
    pub fn new(terms_matching_strategy: TermsMatchingStrategy) -> Self {
        Self::new_with_id("words".to_owned(), Some(terms_matching_strategy))
    }
}
pub type Proximity = GraphBasedRankingRule<ProximityGraph>;
impl GraphBasedRankingRule<ProximityGraph> {
    pub fn new(terms_matching_strategy: Option<TermsMatchingStrategy>) -> Self {
        Self::new_with_id("proximity".to_owned(), terms_matching_strategy)
    }
}
pub type Fid = GraphBasedRankingRule<FidGraph>;
impl GraphBasedRankingRule<FidGraph> {
    pub fn new(terms_matching_strategy: Option<TermsMatchingStrategy>) -> Self {
        Self::new_with_id("fid".to_owned(), terms_matching_strategy)
    }
}
pub type Position = GraphBasedRankingRule<PositionGraph>;
impl GraphBasedRankingRule<PositionGraph> {
    pub fn new(terms_matching_strategy: Option<TermsMatchingStrategy>) -> Self {
        Self::new_with_id("position".to_owned(), terms_matching_strategy)
    }
}
pub type Typo = GraphBasedRankingRule<TypoGraph>;
impl GraphBasedRankingRule<TypoGraph> {
    pub fn new(terms_matching_strategy: Option<TermsMatchingStrategy>) -> Self {
        Self::new_with_id("typo".to_owned(), terms_matching_strategy)
    }
}
pub type Exactness = GraphBasedRankingRule<ExactnessGraph>;
impl GraphBasedRankingRule<ExactnessGraph> {
    pub fn new() -> Self {
        Self::new_with_id("exactness".to_owned(), None)
    }
}

/// A generic graph-based ranking rule
pub struct GraphBasedRankingRule<G: RankingRuleGraphTrait> {
    id: String,
    terms_matching_strategy: Option<TermsMatchingStrategy>,
    // When the ranking rule is not iterating over its buckets,
    // its state is `None`.
    state: Option<GraphBasedRankingRuleState<G>>,
}
impl<G: RankingRuleGraphTrait> GraphBasedRankingRule<G> {
    /// Creates the ranking rule with the given identifier
    pub fn new_with_id(id: String, terms_matching_strategy: Option<TermsMatchingStrategy>) -> Self {
        Self { id, terms_matching_strategy, state: None }
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
    all_costs: MappedInterner<QueryNode, Vec<u64>>,
    /// An index in the first element of `all_distances`, giving the cost of the next bucket
    cur_cost: u64,
    /// One above the highest possible cost for this rule
    next_max_cost: u64,
}

impl<'ctx, G: RankingRuleGraphTrait> RankingRule<'ctx, QueryGraph> for GraphBasedRankingRule<G> {
    fn id(&self) -> String {
        self.id.clone()
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::graph_based")]
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
        _universe: &RoaringBitmap,
        query_graph: &QueryGraph,
    ) -> Result<()> {
        // the `next_max_cost` is the successor integer to the maximum cost of the paths in the graph.
        //
        // When there is a matching strategy, it also factors the additional costs of:
        // 1. The words that are matched in phrases
        // 2. Skipping words (by adding them to the paths with a cost)
        let mut next_max_cost = 1;
        let removal_cost = if let Some(terms_matching_strategy) = self.terms_matching_strategy {
            // add the cost of the phrase to the next_max_cost
            next_max_cost += query_graph
                .words_in_phrases_count(ctx)
                // remove 1 from the words in phrases count, because when there is a phrase we can now have a document
                // where only the phrase is matching, and none of the non-phrase words.
                // With the `1` that `next_max_cost` is initialized with, this gets counted twice.
                .saturating_sub(1) as u64;
            match terms_matching_strategy {
                TermsMatchingStrategy::Last => {
                    let removal_order =
                        query_graph.removal_order_for_terms_matching_strategy_last(ctx);
                    let mut forbidden_nodes =
                        SmallBitmap::for_interned_values_in(&query_graph.nodes);
                    let mut costs = query_graph.nodes.map(|_| None);
                    // FIXME: this works because only words uses termsmatchingstrategy at the moment.
                    for ns in removal_order {
                        for n in ns.iter() {
                            *costs.get_mut(n) = Some((1, forbidden_nodes.clone()));
                        }
                        forbidden_nodes.union(&ns);
                    }
                    costs
                }
                TermsMatchingStrategy::Frequency => {
                    let removal_order =
                        query_graph.removal_order_for_terms_matching_strategy_frequency(ctx)?;
                    let mut forbidden_nodes =
                        SmallBitmap::for_interned_values_in(&query_graph.nodes);
                    let mut costs = query_graph.nodes.map(|_| None);
                    // FIXME: this works because only words uses termsmatchingstrategy at the moment.
                    for ns in removal_order {
                        for n in ns.iter() {
                            *costs.get_mut(n) = Some((1, forbidden_nodes.clone()));
                        }
                        forbidden_nodes.union(&ns);
                    }
                    costs
                }
                TermsMatchingStrategy::All => query_graph.nodes.map(|_| None),
            }
        } else {
            query_graph.nodes.map(|_| None)
        };

        let graph = RankingRuleGraph::build(ctx, query_graph.clone(), removal_cost)?;
        let condition_docids_cache = ConditionDocIdsCache::default();
        let dead_ends_cache = DeadEndsCache::new(&graph.conditions_interner);

        // Then pre-compute the cost of all paths from each node to the end node
        let all_costs = graph.find_all_costs_to_end();

        next_max_cost +=
            all_costs.get(graph.query_graph.root_node).iter().copied().max().unwrap_or(0);

        let state = GraphBasedRankingRuleState {
            graph,
            conditions_cache: condition_docids_cache,
            dead_ends_cache,
            all_costs,
            cur_cost: 0,
            next_max_cost,
        };

        self.state = Some(state);

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::graph_based")]
    fn next_bucket(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<QueryGraph>>> {
        // Will crash if `next_bucket` is called before `start_iteration` or after `end_iteration`,
        // should never happen
        let mut state = self.state.take().unwrap();

        let all_costs = state.all_costs.get(state.graph.query_graph.root_node);
        // Retrieve the cost of the paths to compute
        let Some(&cost) = all_costs.iter().find(|c| **c >= state.cur_cost) else {
            self.state = None;
            return Ok(None);
        };
        state.cur_cost = cost + 1;

        let mut bucket = RoaringBitmap::new();

        let GraphBasedRankingRuleState {
            graph,
            conditions_cache: condition_docids_cache,
            dead_ends_cache,
            all_costs,
            cur_cost: _,
            next_max_cost,
        } = &mut state;

        let rank = *next_max_cost - cost;
        let score = G::rank_to_score(Rank { rank: rank as u32, max_rank: *next_max_cost as u32 });

        let mut universe = universe.clone();

        let mut used_conditions = SmallBitmap::for_interned_values_in(&graph.conditions_interner);
        let mut good_paths = vec![];
        let mut considered_paths = vec![];

        // For each path of the given cost, we will compute its associated
        // document ids.
        // In case the path does not resolve to any document id, we try to figure out why
        // and update the `dead_ends_cache` accordingly.
        // Updating the dead_ends_cache helps speed up the execution of `visit_paths_of_cost` and reduces
        // the number of future candidate paths given by that same function.

        let mut subpaths_docids: Vec<(Interned<G::Condition>, RoaringBitmap)> = vec![];

        let mut nodes_with_removed_outgoing_conditions = BTreeSet::new();
        let visitor = PathVisitor::new(cost, graph, all_costs, dead_ends_cache);

        visitor.visit_paths(&mut |path, graph, dead_ends_cache| {
            considered_paths.push(path.to_vec());
            // If the universe is empty, stop exploring the graph, since no docids will ever be found anymore.
            if universe.is_empty() {
                return Ok(ControlFlow::Break(()));
            }
            // `visit_paths` performs a depth-first search, so the previously visited path
            // is likely to share a prefix with the current one.
            // We stored the previous path and the docids associated to each of its prefixes in `subpaths_docids`.
            // We take advantage of this to avoid computing the docids associated with the common prefix between
            // the old and current path.
            let idx_of_first_different_condition = {
                let mut idx = 0;
                for (&last_c, cur_c) in path.iter().zip(subpaths_docids.iter().map(|x| x.0)) {
                    if last_c == cur_c {
                        idx += 1;
                    } else {
                        break;
                    }
                }
                subpaths_docids.truncate(idx);
                idx
            };
            // Then for the remaining of the path, we continue computing docids.
            for latest_condition in path[idx_of_first_different_condition..].iter().copied() {
                let success = visit_path_condition(
                    ctx,
                    graph,
                    &universe,
                    dead_ends_cache,
                    condition_docids_cache,
                    &mut subpaths_docids,
                    &mut nodes_with_removed_outgoing_conditions,
                    latest_condition,
                )?;
                if !success {
                    return Ok(ControlFlow::Continue(()));
                }
            }
            assert!(subpaths_docids.iter().map(|x| x.0).eq(path.iter().copied()));

            let path_docids =
                subpaths_docids.pop().map(|x| x.1).unwrap_or_else(|| universe.clone());
            assert!(!path_docids.is_empty());

            // Accumulate the path for logging purposes only
            good_paths.push(path.to_vec());
            for &condition in path {
                used_conditions.insert(condition);
            }
            bucket |= &path_docids;
            // Reduce the size of the universe so that we can more optimistically discard candidate paths
            universe -= &path_docids;
            for (_, docids) in subpaths_docids.iter_mut() {
                *docids -= &path_docids;
            }

            if universe.is_empty() {
                Ok(ControlFlow::Break(()))
            } else {
                Ok(ControlFlow::Continue(()))
            }
        })?;
        logger.log_internal_state(graph);
        logger.log_internal_state(&good_paths);

        // We modify the next query graph so that it only contains the subgraph
        // that was used to compute this bucket

        let paths: Vec<Vec<(Option<LocatedQueryTermSubset>, LocatedQueryTermSubset)>> = good_paths
            .into_iter()
            .map(|path| {
                path.into_iter()
                    .map(|condition| {
                        let (a, b) =
                            condition_docids_cache.get_subsets_used_by_condition(condition);
                        (a.clone(), b.clone())
                    })
                    .collect()
            })
            .collect();

        let next_query_graph = QueryGraph::build_from_paths(paths);

        #[allow(clippy::comparison_chain)]
        if nodes_with_removed_outgoing_conditions.len() == 1 {
            graph.update_all_costs_before_node(
                *nodes_with_removed_outgoing_conditions.first().unwrap(),
                all_costs,
            );
        } else if nodes_with_removed_outgoing_conditions.len() > 1 {
            *all_costs = graph.find_all_costs_to_end();
        }

        self.state = Some(state);

        Ok(Some(RankingRuleOutput { query: next_query_graph, candidates: bucket, score }))
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::graph_based")]
    fn end_iteration(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        self.state = None;
    }
}

/// Returns false if the intersection between the condition
/// docids and the previous path docids is empty.
#[allow(clippy::too_many_arguments)]
fn visit_path_condition<G: RankingRuleGraphTrait>(
    ctx: &mut SearchContext<'_>,
    graph: &mut RankingRuleGraph<G>,
    universe: &RoaringBitmap,
    dead_ends_cache: &mut DeadEndsCache<G::Condition>,
    condition_docids_cache: &mut ConditionDocIdsCache<G>,
    subpath: &mut Vec<(Interned<G::Condition>, RoaringBitmap)>,
    nodes_with_removed_outgoing_conditions: &mut BTreeSet<Interned<QueryNode>>,
    latest_condition: Interned<G::Condition>,
) -> Result<bool> {
    let condition_docids = &condition_docids_cache
        .get_computed_condition(ctx, latest_condition, graph, universe)?
        .docids;
    if condition_docids.is_empty() {
        // 1. Store in the cache that this edge is empty for this universe
        dead_ends_cache.forbid_condition(latest_condition);
        // 2. remove all the edges with this condition from the ranking rule graph
        let source_nodes = graph.remove_edges_with_condition(latest_condition);
        nodes_with_removed_outgoing_conditions.extend(source_nodes);
        return Ok(false);
    }

    let latest_path_docids = if let Some((_, prev_docids)) = subpath.last() {
        prev_docids & condition_docids
    } else {
        condition_docids.clone()
    };
    if !latest_path_docids.is_empty() {
        subpath.push((latest_condition, latest_path_docids));
        return Ok(true);
    }
    // If the (sub)path is empty, we try to figure out why and update the caches accordingly.

    // First, we know that this path is empty, and thus any path
    // that is a superset of it will also be empty.
    dead_ends_cache.forbid_condition_after_prefix(subpath.iter().map(|x| x.0), latest_condition);

    if subpath.len() <= 1 {
        return Ok(false);
    }
    let mut subprefix = vec![];
    // Deadend if the intersection between this edge and any
    // previous prefix is disjoint with the universe
    // We already know that the intersection with the last one
    // is empty,
    for (past_condition, sp_docids) in subpath[..subpath.len() - 1].iter() {
        subprefix.push(*past_condition);
        if condition_docids.is_disjoint(sp_docids) {
            dead_ends_cache
                .forbid_condition_after_prefix(subprefix.iter().copied(), latest_condition);
        }
    }

    Ok(false)
}

#![allow(clippy::too_many_arguments)]

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, VecDeque};
use std::ops::ControlFlow;

use super::empty_paths_cache::DeadEndPathCache;
use super::{EdgeCondition, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{Interned, MappedInterner};
use crate::search::new::query_graph::QueryNode;
use crate::search::new::small_bitmap::SmallBitmap;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    pub edges: Vec<u16>,
    pub cost: u64,
}

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    pub fn visit_paths_of_cost(
        &mut self,
        from: Interned<QueryNode>,
        cost: u16,
        all_distances: &MappedInterner<Vec<(u16, SmallBitmap<G::EdgeCondition>)>, QueryNode>,
        empty_paths_cache: &mut DeadEndPathCache<G>,
        mut visit: impl FnMut(&[u16], &mut Self, &mut DeadEndPathCache<G>) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let _ = self.visit_paths_of_cost_rec(
            from,
            cost,
            all_distances,
            empty_paths_cache,
            &mut visit,
            &mut vec![],
            &mut SmallBitmap::for_interned_values_in(&self.conditions_interner),
            &mut empty_paths_cache.conditions.clone(),
        )?;
        Ok(())
    }
    pub fn visit_paths_of_cost_rec(
        &mut self,
        from: Interned<QueryNode>,
        cost: u16,
        all_distances: &MappedInterner<Vec<(u16, SmallBitmap<G::EdgeCondition>)>, QueryNode>,
        empty_paths_cache: &mut DeadEndPathCache<G>,
        visit: &mut impl FnMut(&[u16], &mut Self, &mut DeadEndPathCache<G>) -> Result<ControlFlow<()>>,
        prev_conditions: &mut Vec<u16>,
        cur_path: &mut SmallBitmap<G::EdgeCondition>,
        forbidden_conditions: &mut SmallBitmap<G::EdgeCondition>,
    ) -> Result<bool> {
        let mut any_valid = false;

        let edges = self.edges_of_node.get(from).clone();
        for edge_idx in edges.iter() {
            let Some(edge) = self.edges_store.get(edge_idx).as_ref() else { continue };
            if cost < edge.cost as u16 {
                continue;
            }
            let next_any_valid = match edge.condition {
                EdgeCondition::Unconditional => {
                    if edge.dest_node == self.query_graph.end_node {
                        any_valid = true;
                        let control_flow = visit(prev_conditions, self, empty_paths_cache)?;
                        match control_flow {
                            ControlFlow::Continue(_) => {}
                            ControlFlow::Break(_) => return Ok(true),
                        }
                        true
                    } else {
                        self.visit_paths_of_cost_rec(
                            edge.dest_node,
                            cost - edge.cost as u16,
                            all_distances,
                            empty_paths_cache,
                            visit,
                            prev_conditions,
                            cur_path,
                            forbidden_conditions,
                        )?
                    }
                }
                EdgeCondition::Conditional(condition) => {
                    if forbidden_conditions.contains(condition)
                        || !all_distances.get(edge.dest_node).iter().any(
                            |(next_cost, necessary_conditions)| {
                                (*next_cost == cost - edge.cost as u16)
                                    && !forbidden_conditions.intersects(necessary_conditions)
                            },
                        )
                    {
                        continue;
                    }
                    cur_path.insert(condition);
                    // TODO: typed path set
                    prev_conditions.push(condition.into_inner());

                    let mut new_forbidden_conditions = forbidden_conditions.clone();
                    new_forbidden_conditions
                        .union(empty_paths_cache.condition_couples.get(condition));
                    empty_paths_cache.prefixes.final_edges_after_prefix(
                        prev_conditions,
                        &mut |x| {
                            new_forbidden_conditions.insert(Interned::new(x));
                        },
                    );
                    let next_any_valid = if edge.dest_node == self.query_graph.end_node {
                        any_valid = true;
                        let control_flow = visit(prev_conditions, self, empty_paths_cache)?;
                        match control_flow {
                            ControlFlow::Continue(_) => {}
                            ControlFlow::Break(_) => return Ok(true),
                        }
                        true
                    } else {
                        self.visit_paths_of_cost_rec(
                            edge.dest_node,
                            cost - edge.cost as u16,
                            all_distances,
                            empty_paths_cache,
                            visit,
                            prev_conditions,
                            cur_path,
                            &mut new_forbidden_conditions,
                        )?
                    };
                    cur_path.remove(condition);
                    prev_conditions.pop();
                    next_any_valid
                }
            };
            any_valid |= next_any_valid;

            if next_any_valid {
                if empty_paths_cache.path_is_dead_end(prev_conditions, cur_path) {
                    return Ok(any_valid);
                }
                forbidden_conditions.union(&empty_paths_cache.conditions);
                for prev_condition in prev_conditions.iter() {
                    forbidden_conditions.union(
                        empty_paths_cache.condition_couples.get(Interned::new(*prev_condition)),
                    );
                }
                empty_paths_cache.prefixes.final_edges_after_prefix(prev_conditions, &mut |x| {
                    forbidden_conditions.insert(Interned::new(x));
                });
            }
        }

        Ok(any_valid)
    }

    pub fn initialize_distances_with_necessary_edges(
        &self,
    ) -> MappedInterner<Vec<(u16, SmallBitmap<G::EdgeCondition>)>, QueryNode> {
        let mut distances_to_end = self.query_graph.nodes.map(|_| vec![]);
        let mut enqueued = SmallBitmap::new(self.query_graph.nodes.len());

        let mut node_stack = VecDeque::new();

        *distances_to_end.get_mut(self.query_graph.end_node) =
            vec![(0, SmallBitmap::for_interned_values_in(&self.conditions_interner))];

        for prev_node in self.query_graph.nodes.get(self.query_graph.end_node).predecessors.iter() {
            node_stack.push_back(prev_node);
            enqueued.insert(prev_node);
        }

        while let Some(cur_node) = node_stack.pop_front() {
            let mut self_distances = BTreeMap::<u16, SmallBitmap<G::EdgeCondition>>::new();

            let cur_node_edges = &self.edges_of_node.get(cur_node);
            for edge_idx in cur_node_edges.iter() {
                let edge = self.edges_store.get(edge_idx).as_ref().unwrap();
                let condition = match edge.condition {
                    EdgeCondition::Unconditional => None,
                    EdgeCondition::Conditional(condition) => Some(condition),
                };
                let succ_node = edge.dest_node;
                let succ_distances = distances_to_end.get(succ_node);
                for (succ_distance, succ_necessary_conditions) in succ_distances {
                    let mut potential_necessary_edges =
                        SmallBitmap::for_interned_values_in(&self.conditions_interner);
                    for condition in condition.into_iter().chain(succ_necessary_conditions.iter()) {
                        potential_necessary_edges.insert(condition);
                    }

                    match self_distances.entry(edge.cost as u16 + succ_distance) {
                        Entry::Occupied(mut prev_necessary_edges) => {
                            prev_necessary_edges.get_mut().intersection(&potential_necessary_edges);
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(potential_necessary_edges);
                        }
                    }
                }
            }
            let distances_to_end_cur_node = distances_to_end.get_mut(cur_node);
            for (cost, necessary_edges) in self_distances.iter() {
                distances_to_end_cur_node.push((*cost, necessary_edges.clone()));
            }
            *distances_to_end.get_mut(cur_node) = self_distances.into_iter().collect();
            for prev_node in self.query_graph.nodes.get(cur_node).predecessors.iter() {
                if !enqueued.contains(prev_node) {
                    node_stack.push_back(prev_node);
                    enqueued.insert(prev_node);
                }
            }
        }
        distances_to_end
    }
}

#![allow(clippy::too_many_arguments)]

use std::collections::{BTreeSet, VecDeque};
use std::ops::ControlFlow;

use super::{DeadEndsCache, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{Interned, MappedInterner};
use crate::search::new::query_graph::QueryNode;
use crate::search::new::small_bitmap::SmallBitmap;
use crate::Result;

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    pub fn visit_paths_of_cost(
        &mut self,
        from: Interned<QueryNode>,
        cost: u16,
        all_distances: &MappedInterner<Vec<u16>, QueryNode>,
        dead_ends_cache: &mut DeadEndsCache<G::Condition>,
        mut visit: impl FnMut(
            &[Interned<G::Condition>],
            &mut Self,
            &mut DeadEndsCache<G::Condition>,
        ) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let _ = self.visit_paths_of_cost_rec(
            from,
            cost,
            all_distances,
            dead_ends_cache,
            &mut visit,
            &mut vec![],
            &mut SmallBitmap::for_interned_values_in(&self.conditions_interner),
            dead_ends_cache.forbidden.clone(),
        )?;
        Ok(())
    }
    pub fn visit_paths_of_cost_rec(
        &mut self,
        from: Interned<QueryNode>,
        cost: u16,
        all_distances: &MappedInterner<Vec<u16>, QueryNode>,
        dead_ends_cache: &mut DeadEndsCache<G::Condition>,
        visit: &mut impl FnMut(
            &[Interned<G::Condition>],
            &mut Self,
            &mut DeadEndsCache<G::Condition>,
        ) -> Result<ControlFlow<()>>,
        prev_conditions: &mut Vec<Interned<G::Condition>>,
        cur_path: &mut SmallBitmap<G::Condition>,
        mut forbidden_conditions: SmallBitmap<G::Condition>,
    ) -> Result<bool> {
        let mut any_valid = false;

        let edges = self.edges_of_node.get(from).clone();
        'edges_loop: for edge_idx in edges.iter() {
            let Some(edge) = self.edges_store.get(edge_idx).as_ref() else { continue };
            if cost < edge.cost as u16 {
                continue;
            }
            let next_any_valid = match edge.condition {
                None => {
                    if edge.dest_node == self.query_graph.end_node {
                        any_valid = true;
                        let control_flow = visit(prev_conditions, self, dead_ends_cache)?;
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
                            dead_ends_cache,
                            visit,
                            prev_conditions,
                            cur_path,
                            forbidden_conditions.clone(),
                        )?
                    }
                }
                Some(condition) => {
                    if forbidden_conditions.contains(condition)
                        || all_distances
                            .get(edge.dest_node)
                            .iter()
                            .all(|next_cost| *next_cost != cost - edge.cost as u16)
                    {
                        continue;
                    }
                    cur_path.insert(condition);
                    prev_conditions.push(condition);
                    let mut new_forbidden_conditions = forbidden_conditions.clone();
                    if let Some(next_forbidden) =
                        dead_ends_cache.forbidden_conditions_after_prefix(prev_conditions)
                    {
                        new_forbidden_conditions.union(&next_forbidden);
                    }

                    let next_any_valid = if edge.dest_node == self.query_graph.end_node {
                        any_valid = true;
                        let control_flow = visit(prev_conditions, self, dead_ends_cache)?;
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
                            dead_ends_cache,
                            visit,
                            prev_conditions,
                            cur_path,
                            new_forbidden_conditions,
                        )?
                    };
                    cur_path.remove(condition);
                    prev_conditions.pop();
                    next_any_valid
                }
            };
            any_valid |= next_any_valid;

            if next_any_valid {
                forbidden_conditions =
                    dead_ends_cache.forbidden_conditions_for_all_prefixes_up_to(prev_conditions);
                if cur_path.intersects(&forbidden_conditions) {
                    break 'edges_loop;
                }
            }
        }

        Ok(any_valid)
    }

    pub fn initialize_distances_with_necessary_edges(&self) -> MappedInterner<Vec<u16>, QueryNode> {
        let mut distances_to_end = self.query_graph.nodes.map(|_| vec![]);
        let mut enqueued = SmallBitmap::new(self.query_graph.nodes.len());

        let mut node_stack = VecDeque::new();

        *distances_to_end.get_mut(self.query_graph.end_node) = vec![0];

        for prev_node in self.query_graph.nodes.get(self.query_graph.end_node).predecessors.iter() {
            node_stack.push_back(prev_node);
            enqueued.insert(prev_node);
        }

        while let Some(cur_node) = node_stack.pop_front() {
            let mut self_distances = BTreeSet::<u16>::new();

            let cur_node_edges = &self.edges_of_node.get(cur_node);
            for edge_idx in cur_node_edges.iter() {
                let edge = self.edges_store.get(edge_idx).as_ref().unwrap();
                let succ_node = edge.dest_node;
                let succ_distances = distances_to_end.get(succ_node);
                for succ_distance in succ_distances {
                    self_distances.insert(edge.cost as u16 + succ_distance);
                }
            }
            let distances_to_end_cur_node = distances_to_end.get_mut(cur_node);
            for cost in self_distances.iter() {
                distances_to_end_cur_node.push(*cost);
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

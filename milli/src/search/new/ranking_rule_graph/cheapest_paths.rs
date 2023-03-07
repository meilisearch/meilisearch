#![allow(clippy::too_many_arguments)]

use super::empty_paths_cache::EmptyPathsCache;
use super::{RankingRuleGraph, RankingRuleGraphTrait};
use crate::new::small_bitmap::SmallBitmap;
use crate::Result;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    pub edges: Vec<u16>,
    pub cost: u64,
}

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    pub fn visit_paths_of_cost(
        &mut self,
        from: usize,
        cost: u16,
        all_distances: &[Vec<u16>],
        empty_paths_cache: &mut EmptyPathsCache,
        mut visit: impl FnMut(&[u16], &mut Self, &mut EmptyPathsCache) -> Result<()>,
    ) -> Result<()> {
        let _ = self.visit_paths_of_cost_rec(
            from,
            cost,
            all_distances,
            empty_paths_cache,
            &mut visit,
            &mut vec![],
            &mut SmallBitmap::new(self.all_edges.len() as u16),
            empty_paths_cache.empty_edges.clone(),
        )?;
        Ok(())
    }
    pub fn visit_paths_of_cost_rec(
        &mut self,
        from: usize,
        cost: u16,
        // TODO: replace all_distances with a Vec<SmallBitmap> where the SmallBitmap contains true if the cost exists and false otherwise
        all_distances: &[Vec<u16>],
        empty_paths_cache: &mut EmptyPathsCache,
        visit: &mut impl FnMut(&[u16], &mut Self, &mut EmptyPathsCache) -> Result<()>,
        // replace prev edges by:
        // (1) a small bitmap representing the path
        // (2) a pointer within the EmptyPathsCache::forbidden_prefixes structure
        prev_edges: &mut Vec<u16>,
        cur_path: &mut SmallBitmap,
        mut forbidden_edges: SmallBitmap,
    ) -> Result<bool> {
        let mut any_valid = false;

        let edges = self.node_edges[from].clone();
        for edge_idx in edges.iter() {
            let Some(edge) = self.all_edges[edge_idx as usize].as_ref() else { continue };
            if cost < edge.cost as u16
                || forbidden_edges.contains(edge_idx)
                || !all_distances[edge.to_node as usize].contains(&(cost - edge.cost as u16))
            {
                continue;
            }
            cur_path.insert(edge_idx);
            prev_edges.push(edge_idx);

            let mut new_forbidden_edges = forbidden_edges.clone();
            new_forbidden_edges.union(&empty_paths_cache.empty_couple_edges[edge_idx as usize]);
            empty_paths_cache.empty_prefixes.final_edges_after_prefix(prev_edges, &mut |x| {
                new_forbidden_edges.insert(x);
            });

            let next_any_valid = if edge.to_node == self.query_graph.end_node {
                any_valid = true;
                visit(prev_edges, self, empty_paths_cache)?;
                true
            } else {
                self.visit_paths_of_cost_rec(
                    edge.to_node as usize,
                    cost - edge.cost as u16,
                    all_distances,
                    empty_paths_cache,
                    visit,
                    prev_edges,
                    cur_path,
                    new_forbidden_edges,
                )?
            };
            any_valid |= next_any_valid;
            cur_path.remove(edge_idx);
            prev_edges.pop();
            if next_any_valid {
                if empty_paths_cache.path_is_empty(prev_edges, cur_path) {
                    return Ok(any_valid);
                }
                forbidden_edges.union(&empty_paths_cache.empty_edges);
                for edge in prev_edges.iter() {
                    forbidden_edges.union(&empty_paths_cache.empty_couple_edges[*edge as usize]);
                }
                empty_paths_cache.empty_prefixes.final_edges_after_prefix(prev_edges, &mut |x| {
                    forbidden_edges.insert(x);
                });
            }
            if next_any_valid && empty_paths_cache.path_is_empty(prev_edges, cur_path) {
                return Ok(any_valid);
            }
        }

        Ok(any_valid)
    }

    pub fn initialize_distances_cheapest(&self) -> Vec<Vec<u16>> {
        let mut distances_to_end: Vec<Vec<u16>> = vec![vec![]; self.query_graph.nodes.len()];
        let mut enqueued = SmallBitmap::new(self.query_graph.nodes.len() as u16);

        let mut node_stack = VecDeque::new();

        distances_to_end[self.query_graph.end_node as usize] = vec![0];

        for prev_node in
            self.query_graph.edges[self.query_graph.end_node as usize].predecessors.iter()
        {
            node_stack.push_back(prev_node as usize);
            enqueued.insert(prev_node);
        }

        while let Some(cur_node) = node_stack.pop_front() {
            let mut self_distances = vec![];

            let cur_node_edges = &self.node_edges[cur_node];
            for edge_idx in cur_node_edges.iter() {
                let edge = self.all_edges[edge_idx as usize].as_ref().unwrap();
                let succ_node = edge.to_node;
                let succ_distances = &distances_to_end[succ_node as usize];
                for succ_distance in succ_distances {
                    self_distances.push(edge.cost as u16 + succ_distance);
                }
            }

            self_distances.sort_unstable();
            self_distances.dedup();
            distances_to_end[cur_node] = self_distances;
            for prev_node in self.query_graph.edges[cur_node].predecessors.iter() {
                if !enqueued.contains(prev_node) {
                    node_stack.push_back(prev_node as usize);
                    enqueued.insert(prev_node);
                }
            }
        }
        distances_to_end
    }
}

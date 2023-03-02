#![allow(clippy::too_many_arguments)]

use super::empty_paths_cache::EmptyPathsCache;
use super::{RankingRuleGraph, RankingRuleGraphTrait};
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    pub edges: Vec<u32>,
    pub cost: u64,
}

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    pub fn paths_of_cost(
        &self,
        from: usize,
        cost: u64,
        all_distances: &[Vec<u64>],
        empty_paths_cache: &EmptyPathsCache,
    ) -> Vec<Vec<u32>> {
        let mut paths = vec![];
        self.paths_of_cost_rec(
            from,
            all_distances,
            cost,
            &mut vec![],
            &mut paths,
            &vec![false; self.all_edges.len()],
            empty_paths_cache,
        );
        paths
    }
    pub fn paths_of_cost_rec(
        &self,
        from: usize,
        all_distances: &[Vec<u64>],
        cost: u64,
        prev_edges: &mut Vec<u32>,
        paths: &mut Vec<Vec<u32>>,
        forbidden_edges: &[bool],
        empty_paths_cache: &EmptyPathsCache,
    ) {
        let distances = &all_distances[from];
        if !distances.contains(&cost) {
            panic!();
        }
        let tos = &self.query_graph.edges[from].successors;
        let mut valid_edges = vec![];
        for to in tos {
            self.visit_edges::<()>(from as u32, to, |edge_idx, edge| {
                if cost >= edge.cost as u64
                    && all_distances[to as usize].contains(&(cost - edge.cost as u64))
                    && !forbidden_edges[edge_idx as usize]
                {
                    valid_edges.push((edge_idx, edge.cost, to));
                }
                std::ops::ControlFlow::Continue(())
            });
        }

        for (edge_idx, edge_cost, to) in valid_edges {
            prev_edges.push(edge_idx);
            if empty_paths_cache.empty_prefixes.contains_prefix_of_path(prev_edges) {
                continue;
            }
            let mut new_forbidden_edges = forbidden_edges.to_vec();
            for edge_idx in empty_paths_cache.empty_couple_edges[edge_idx as usize].iter() {
                new_forbidden_edges[*edge_idx as usize] = true;
            }
            for edge_idx in empty_paths_cache.empty_prefixes.final_edges_ater_prefix(prev_edges) {
                new_forbidden_edges[edge_idx as usize] = true;
            }

            if to == self.query_graph.end_node {
                paths.push(prev_edges.clone());
            } else {
                self.paths_of_cost_rec(
                    to as usize,
                    all_distances,
                    cost - edge_cost as u64,
                    prev_edges,
                    paths,
                    &new_forbidden_edges,
                    empty_paths_cache,
                )
            }
            prev_edges.pop();
        }
    }

    pub fn initialize_distances_cheapest(&self) -> Vec<Vec<u64>> {
        let mut distances_to_end: Vec<Vec<u64>> = vec![vec![]; self.query_graph.nodes.len()];
        let mut enqueued = vec![false; self.query_graph.nodes.len()];

        let mut node_stack = VecDeque::new();

        distances_to_end[self.query_graph.end_node as usize] = vec![0];
        for prev_node in
            self.query_graph.edges[self.query_graph.end_node as usize].predecessors.iter()
        {
            node_stack.push_back(prev_node as usize);
            enqueued[prev_node as usize] = true;
        }

        while let Some(cur_node) = node_stack.pop_front() {
            let mut self_distances = vec![];
            for succ_node in self.query_graph.edges[cur_node].successors.iter() {
                let succ_distances = &distances_to_end[succ_node as usize];
                let _ = self.visit_edges::<()>(cur_node as u32, succ_node, |_, edge| {
                    for succ_distance in succ_distances {
                        self_distances.push(edge.cost as u64 + succ_distance);
                    }
                    std::ops::ControlFlow::Continue(())
                });
            }
            self_distances.sort_unstable();
            self_distances.dedup();
            distances_to_end[cur_node] = self_distances;
            for prev_node in self.query_graph.edges[cur_node].predecessors.iter() {
                if !enqueued[prev_node as usize] {
                    node_stack.push_back(prev_node as usize);
                    enqueued[prev_node as usize] = true;
                }
            }
        }
        distances_to_end
    }
}

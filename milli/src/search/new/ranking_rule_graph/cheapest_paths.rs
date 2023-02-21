use std::collections::{BTreeMap, HashSet};

use itertools::Itertools;

use super::{
    empty_paths_cache::EmptyPathsCache, paths_map::PathsMap, Edge, EdgeIndex, RankingRuleGraph,
    RankingRuleGraphTrait,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    pub edges: Vec<EdgeIndex>,
    pub cost: u64,
}

struct DijkstraState {
    unvisited: HashSet<usize>, // should be a small bitset
    distances: Vec<u64>,       // or binary heap (f64, usize)
    edges: Vec<EdgeIndex>,
    edge_costs: Vec<u8>,
    paths: Vec<Option<usize>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PathEdgeId<Id> {
    pub from: usize,
    pub to: usize,
    pub id: Id,
}

pub struct KCheapestPathsState {
    cheapest_paths: PathsMap<u64>,
    potential_cheapest_paths: BTreeMap<u64, PathsMap<u64>>,
    pub kth_cheapest_path: Path,
}

impl KCheapestPathsState {
    pub fn next_cost(&self) -> u64 {
        self.kth_cheapest_path.cost
    }

    pub fn new<G: RankingRuleGraphTrait>(
        graph: &RankingRuleGraph<G>,
    ) -> Option<KCheapestPathsState> {
        let Some(cheapest_path) = graph.cheapest_path_to_end(graph.query_graph.root_node) else {
            return None
        };
        let cheapest_paths = PathsMap::from_paths(&[cheapest_path.clone()]);
        let potential_cheapest_paths = BTreeMap::new();
        Some(KCheapestPathsState {
            cheapest_paths,
            potential_cheapest_paths,
            kth_cheapest_path: cheapest_path,
        })
    }

    pub fn remove_empty_paths(mut self, empty_paths_cache: &EmptyPathsCache) -> Option<Self> {
        self.cheapest_paths.remove_edges(&empty_paths_cache.empty_edges);
        self.cheapest_paths.remove_prefixes(&empty_paths_cache.empty_prefixes);

        let mut costs_to_delete = HashSet::new();
        for (cost, potential_cheapest_paths) in self.potential_cheapest_paths.iter_mut() {
            potential_cheapest_paths.remove_edges(&empty_paths_cache.empty_edges);
            potential_cheapest_paths.remove_prefixes(&empty_paths_cache.empty_prefixes);
            if potential_cheapest_paths.is_empty() {
                costs_to_delete.insert(*cost);
            }
        }
        for cost in costs_to_delete {
            self.potential_cheapest_paths.remove(&cost);
        }

        if self.cheapest_paths.is_empty() {}

        todo!()
    }

    pub fn compute_paths_of_next_lowest_cost<G: RankingRuleGraphTrait>(
        mut self,
        graph: &mut RankingRuleGraph<G>,
        empty_paths_cache: &EmptyPathsCache,
        into_map: &mut PathsMap<u64>,
    ) -> Option<Self> {
        into_map.add_path(&self.kth_cheapest_path);
        let cur_cost = self.kth_cheapest_path.cost;
        while self.kth_cheapest_path.cost <= cur_cost {
            if let Some(next_self) = self.compute_next_cheapest_paths(graph, empty_paths_cache) {
                self = next_self;
                if self.kth_cheapest_path.cost == cur_cost {
                    into_map.add_path(&self.kth_cheapest_path);
                }
            } else {
                return None;
            }
        }
        Some(self)
    }

    // TODO: use the cache to potentially remove edges that return an empty RoaringBitmap
    // TODO: return an Option<&'self Path>?
    fn compute_next_cheapest_paths<G: RankingRuleGraphTrait>(
        mut self,
        graph: &mut RankingRuleGraph<G>,
        empty_paths_cache: &EmptyPathsCache,
    ) -> Option<KCheapestPathsState> {
        // for all nodes in the last cheapest path (called spur_node), except last one...
        for (i, edge_idx) in self.kth_cheapest_path.edges[..self.kth_cheapest_path.edges.len() - 1]
            .iter()
            .enumerate()
        {
            let Some(edge) = graph.all_edges[edge_idx.0].as_ref() else { continue; };
            let Edge { from_node: spur_node, .. } = edge;

            // TODO:
            // Here, check that the root path is not dicarded by the empty_paths_cache
            // If it is, then continue to the next spur_node
            let root_path = &self.kth_cheapest_path.edges[..i];
            if empty_paths_cache.path_is_empty(root_path) {
                continue;
            }

            let root_cost = root_path
                .iter()
                .fold(0, |sum, next| sum + graph.get_edge(*next).as_ref().unwrap().cost as u64);

            let mut tmp_removed_edges = vec![];
            // for all the paths already found that share a common prefix with the root path
            // we delete the edge from the spur node to the next one
            for edge_index_to_remove in self.cheapest_paths.edge_indices_after_prefix(root_path) {
                let was_removed = graph.node_edges[*spur_node].remove(&edge_index_to_remove.0);
                if was_removed {
                    tmp_removed_edges.push(edge_index_to_remove.0);
                }
            }

            // Compute the cheapest path from the spur node to the destination
            // we will combine it with the root path to get a potential kth cheapest path
            let spur_path = graph.cheapest_path_to_end(*spur_node);
            // restore the temporarily removed edges
            graph.node_edges[*spur_node].extend(tmp_removed_edges);

            let Some(spur_path) = spur_path else { continue; };
            let total_cost = root_cost + spur_path.cost;
            let total_path = Path {
                edges: root_path.iter().chain(spur_path.edges.iter()).cloned().collect(),
                cost: total_cost,
            };
            let entry = self.potential_cheapest_paths.entry(total_cost).or_default();
            entry.add_path(&total_path);
        }
        while let Some(mut next_cheapest_paths_entry) = self.potential_cheapest_paths.first_entry()
        {
            // This could be implemented faster
            // Here, maybe I should filter the potential cheapest paths so that they
            // don't contain any removed edge?

            let cost = *next_cheapest_paths_entry.key();
            let next_cheapest_paths = next_cheapest_paths_entry.get_mut();

            while let Some((next_cheapest_path, cost2)) = next_cheapest_paths.remove_first() {
                assert_eq!(cost, cost2);
                if next_cheapest_path
                    .iter()
                    .any(|edge_index| graph.all_edges.get(edge_index.0).is_none())
                {
                    continue;
                } else {
                    self.cheapest_paths.insert(next_cheapest_path.iter().copied(), cost);

                    if next_cheapest_paths.is_empty() {
                        next_cheapest_paths_entry.remove();
                    }
                    self.kth_cheapest_path = Path { edges: next_cheapest_path, cost };

                    return Some(self);
                }
            }
            let _ = next_cheapest_paths_entry.remove_entry();
        }
        None
    }
}

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    fn cheapest_path_to_end(&self, from: usize) -> Option<Path> {
        let mut dijkstra = DijkstraState {
            unvisited: (0..self.query_graph.nodes.len()).collect(),
            distances: vec![u64::MAX; self.query_graph.nodes.len()],
            edges: vec![EdgeIndex(usize::MAX); self.query_graph.nodes.len()],
            edge_costs: vec![u8::MAX; self.query_graph.nodes.len()],
            paths: vec![None; self.query_graph.nodes.len()],
        };
        dijkstra.distances[from] = 0;

        // TODO: could use a binary heap here to store the distances
        while let Some(&cur_node) =
            dijkstra.unvisited.iter().min_by_key(|&&n| dijkstra.distances[n])
        {
            let cur_node_dist = dijkstra.distances[cur_node];
            if cur_node_dist == u64::MAX {
                return None;
            }
            if cur_node == self.query_graph.end_node {
                break;
            }

            let succ_cur_node: HashSet<_> = self.node_edges[cur_node]
                .iter()
                .map(|e| self.all_edges[*e].as_ref().unwrap().to_node)
                .collect();
            // TODO: this intersection may be slow but shouldn't be,
            // can use a bitmap intersection instead
            let unvisited_succ_cur_node = succ_cur_node.intersection(&dijkstra.unvisited);
            for &succ in unvisited_succ_cur_node {
                let Some((cheapest_edge, cheapest_edge_cost)) = self.cheapest_edge(cur_node, succ) else {
                    continue
                };

                // println!("cur node dist {cur_node_dist}");
                let old_dist_succ = &mut dijkstra.distances[succ];
                let new_potential_distance = cur_node_dist + cheapest_edge_cost as u64;
                if new_potential_distance < *old_dist_succ {
                    *old_dist_succ = new_potential_distance;
                    dijkstra.edges[succ] = cheapest_edge;
                    dijkstra.edge_costs[succ] = cheapest_edge_cost;
                    dijkstra.paths[succ] = Some(cur_node);
                }
            }
            dijkstra.unvisited.remove(&cur_node);
        }

        let mut cur = self.query_graph.end_node;
        // let mut edge_costs = vec![];
        // let mut distances = vec![];
        let mut path_edges = vec![];
        while let Some(n) = dijkstra.paths[cur] {
            path_edges.push(dijkstra.edges[cur]);
            cur = n;
        }
        path_edges.reverse();
        Some(Path { edges: path_edges, cost: dijkstra.distances[self.query_graph.end_node] })
    }

    // TODO: this implementation is VERY fragile, as we assume that the edges are ordered by cost
    // already. Change it.
    pub fn cheapest_edge(&self, cur_node: usize, succ: usize) -> Option<(EdgeIndex, u8)> {
        self.visit_edges(cur_node, succ, |edge_idx, edge| {
            std::ops::ControlFlow::Break((edge_idx, edge.cost))
        })
    }
}

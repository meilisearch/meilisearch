use super::paths_map::PathsMap;

#[derive(Clone)]
pub struct EmptyPathsCache {
    pub empty_edges: Vec<bool>,
    pub empty_prefixes: PathsMap<()>,
    pub empty_couple_edges: Vec<Vec<u32>>,
}
impl EmptyPathsCache {
    pub fn new(all_edges_len: usize) -> Self {
        Self {
            empty_edges: vec![false; all_edges_len],
            empty_prefixes: PathsMap::default(),
            empty_couple_edges: vec![vec![]; all_edges_len],
        }
    }
    pub fn forbid_edge(&mut self, edge_idx: u32) {
        self.empty_edges[edge_idx as usize] = true;
        self.empty_couple_edges[edge_idx as usize] = vec![];
        self.empty_prefixes.remove_edge(&edge_idx);
        for edges2 in self.empty_couple_edges.iter_mut() {
            if let Some(edge2_pos) = edges2.iter().position(|e| *e == edge_idx) {
                edges2.swap_remove(edge2_pos);
            }
        }
    }
    pub fn forbid_prefix(&mut self, prefix: &[u32]) {
        self.empty_prefixes.insert(prefix.iter().copied(), ());
    }
    pub fn forbid_couple_edges(&mut self, edge1: u32, edge2: u32) {
        assert!(!self.empty_couple_edges[edge1 as usize].contains(&edge2));
        self.empty_couple_edges[edge1 as usize].push(edge2);
    }
    pub fn path_is_empty(&self, path: &[u32]) -> bool {
        for edge in path {
            if self.empty_edges[*edge as usize] {
                return true;
            }
        }
        if self.empty_prefixes.contains_prefix_of_path(path) {
            return true;
        }
        for (edge1, edges2) in self.empty_couple_edges.iter().enumerate() {
            if let Some(pos_edge1) = path.iter().position(|e| *e == edge1 as u32) {
                if path[pos_edge1..].iter().any(|e| edges2.contains(e)) {
                    return true;
                }
            }
        }
        // for (edge1, edge2) in self.empty_couple_edges.iter() {
        //     if path.contains(edge1) && path.contains(edge2) {
        //         return true;
        //     }
        // }
        // if self.empty_prefixes.contains_prefix_of_path(path) {
        //     return true;
        // }
        false
    }
}

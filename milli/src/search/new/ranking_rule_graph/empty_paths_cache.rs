use crate::new::small_bitmap::SmallBitmap;

use super::paths_map::PathsMap;

#[derive(Clone)]
pub struct EmptyPathsCache {
    pub empty_edges: SmallBitmap,
    pub empty_prefixes: PathsMap<()>,
    pub empty_couple_edges: Vec<SmallBitmap>,
}
impl EmptyPathsCache {
    pub fn new(all_edges_len: u16) -> Self {
        Self {
            empty_edges: SmallBitmap::new(all_edges_len),
            empty_prefixes: PathsMap::default(),
            empty_couple_edges: vec![SmallBitmap::new(all_edges_len); all_edges_len as usize],
        }
    }
    pub fn forbid_edge(&mut self, edge_idx: u16) {
        self.empty_edges.insert(edge_idx);
        self.empty_couple_edges[edge_idx as usize].clear();
        self.empty_prefixes.remove_edge(&edge_idx);
        for edges2 in self.empty_couple_edges.iter_mut() {
            edges2.remove(edge_idx);
        }
    }
    pub fn forbid_prefix(&mut self, prefix: &[u16]) {
        self.empty_prefixes.insert(prefix.iter().copied(), ());
    }
    pub fn forbid_couple_edges(&mut self, edge1: u16, edge2: u16) {
        self.empty_couple_edges[edge1 as usize].insert(edge2);
    }
    pub fn path_is_empty(&self, path: &[u16], path_bitmap: &SmallBitmap) -> bool {
        if path_bitmap.intersects(&self.empty_edges) {
            return true;
        }
        for edge in path.iter() {
            let forbidden_other_edges = &self.empty_couple_edges[*edge as usize];
            if path_bitmap.intersects(forbidden_other_edges) {
                return true;
            }
        }
        if self.empty_prefixes.contains_prefix_of_path(path) {
            return true;
        }
        false
    }
}

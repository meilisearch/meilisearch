use roaring::RoaringBitmap;

use super::paths_map::PathsMap;

#[derive(Default, Clone)]
pub struct EmptyPathsCache {
    pub empty_edges: RoaringBitmap,
    pub empty_prefixes: PathsMap<()>,
}
impl EmptyPathsCache {
    pub fn forbid_edge(&mut self, edge_idx: u32) {
        self.empty_edges.insert(edge_idx);
        self.empty_prefixes.remove_edge(&edge_idx);
    }
    pub fn path_is_empty(&self, path: &[u32]) -> bool {
        for edge in path {
            if self.empty_edges.contains(*edge) {
                return true;
            }
        }
        if self.empty_prefixes.contains_prefix_of_path(path) {
            return true;
        }
        false
    }
}

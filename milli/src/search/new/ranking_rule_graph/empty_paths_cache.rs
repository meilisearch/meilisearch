use std::collections::HashSet;

use roaring::RoaringBitmap;

use super::paths_map::PathsMap;

#[derive(Default)]
pub struct EmptyPathsCache {
    pub empty_edges: RoaringBitmap,
    pub empty_prefixes: PathsMap<()>,
}
impl EmptyPathsCache {
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

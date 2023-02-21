use std::collections::HashSet;

use super::{paths_map::PathsMap, EdgeIndex};

#[derive(Default)]
pub struct EmptyPathsCache {
    pub empty_edges: HashSet<EdgeIndex>,
    pub empty_prefixes: PathsMap<()>,
}
impl EmptyPathsCache {
    pub fn path_is_empty(&self, path: &[EdgeIndex]) -> bool {
        for edge in path {
            // TODO: should be a bitmap intersection
            if self.empty_edges.contains(edge) {
                return true;
            }
        }
        if self.empty_prefixes.contains_prefix_of_path(path) {
            return true;
        }
        false
    }
}

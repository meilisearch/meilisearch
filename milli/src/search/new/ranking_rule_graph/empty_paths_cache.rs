use super::paths_map::PathSet;
use crate::search::new::small_bitmap::SmallBitmap;

/// A cache which stores sufficient conditions for a path
/// to resolve to an empty set of candidates within the current
/// universe.
#[derive(Clone)]
pub struct EmptyPathsCache {
    /// The set of edge indexes that resolve to no documents.
    pub empty_edges: SmallBitmap,
    /// A set of path prefixes that resolve to no documents.
    pub empty_prefixes: PathSet,
    /// A set of empty couple of edge indexes that resolve to no documents.
    pub empty_couple_edges: Vec<SmallBitmap>,
}
impl EmptyPathsCache {
    /// Create a new cache for a ranking rule graph containing at most `all_edges_len` edges.
    pub fn new(all_edges_len: u16) -> Self {
        Self {
            empty_edges: SmallBitmap::new(all_edges_len),
            empty_prefixes: PathSet::default(),
            empty_couple_edges: vec![SmallBitmap::new(all_edges_len); all_edges_len as usize],
        }
    }

    /// Store in the cache that every path containing the given edge resolves to no documents.
    pub fn forbid_edge(&mut self, edge_idx: u16) {
        self.empty_edges.insert(edge_idx);
        self.empty_couple_edges[edge_idx as usize].clear();
        self.empty_prefixes.remove_edge(&edge_idx);
        for edges2 in self.empty_couple_edges.iter_mut() {
            edges2.remove(edge_idx);
        }
    }
    /// Store in the cache that every path containing the given prefix resolves to no documents.
    pub fn forbid_prefix(&mut self, prefix: &[u16]) {
        self.empty_prefixes.insert(prefix.iter().copied());
    }

    /// Store in the cache that every path containing the two given edges resolves to no documents.
    pub fn forbid_couple_edges(&mut self, edge1: u16, edge2: u16) {
        self.empty_couple_edges[edge1 as usize].insert(edge2);
    }

    /// Returns true if the cache can determine that the given path resolves to no documents.
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

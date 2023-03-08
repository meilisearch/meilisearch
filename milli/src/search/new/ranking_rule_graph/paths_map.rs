use super::cheapest_paths::Path;
use crate::search::new::small_bitmap::SmallBitmap;

// What is PathsMap used for?
// For the empty_prefixes field in the EmptyPathsCache only :/
// but it could be used for more, like efficient computing of a set of paths

#[derive(Debug, Clone)]
pub struct PathsMap<V> {
    pub nodes: Vec<(u16, PathsMap<V>)>,
    pub value: Option<V>,
}
impl<V> Default for PathsMap<V> {
    fn default() -> Self {
        Self { nodes: vec![], value: None }
    }
}

impl PathsMap<u64> {
    pub fn from_paths(paths: &[Path]) -> Self {
        let mut result = Self::default();
        for p in paths {
            result.add_path(p);
        }
        result
    }
    pub fn add_path(&mut self, path: &Path) {
        self.insert(path.edges.iter().copied(), path.cost);
    }
}
impl<V> PathsMap<V> {
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.value.is_none()
    }

    pub fn insert(&mut self, mut edges: impl Iterator<Item = u16>, value: V) {
        match edges.next() {
            None => {
                self.value = Some(value);
            }
            Some(first_edge) => {
                // comment
                for (edge, next_node) in &mut self.nodes {
                    if edge == &first_edge {
                        return next_node.insert(edges, value);
                    }
                }
                let mut rest = PathsMap::default();
                rest.insert(edges, value);
                self.nodes.push((first_edge, rest));
            }
        }
    }
    fn remove_first_rec(&mut self, cur: &mut Vec<u16>) -> (bool, V) {
        let Some((first_edge, rest)) = self.nodes.first_mut() else {
            // The PathsMap has to be correct by construction here, otherwise
            // the unwrap() will crash
            return (true, self.value.take().unwrap())
        };
        cur.push(*first_edge);
        let (rest_is_empty, value) = rest.remove_first_rec(cur);
        if rest_is_empty {
            self.nodes.remove(0);
            (self.nodes.is_empty(), value)
        } else {
            (false, value)
        }
    }
    pub fn remove_first(&mut self) -> Option<(Vec<u16>, V)> {
        if self.is_empty() {
            return None;
        }

        let mut result = vec![];
        let (_, value) = self.remove_first_rec(&mut result);
        Some((result, value))
    }
    pub fn iterate_rec(&self, cur: &mut Vec<u16>, visit: &mut impl FnMut(&Vec<u16>, &V)) {
        if let Some(value) = &self.value {
            visit(cur, value);
        }
        for (first_edge, rest) in self.nodes.iter() {
            cur.push(*first_edge);
            rest.iterate_rec(cur, visit);
            cur.pop();
        }
    }
    pub fn iterate(&self, mut visit: impl FnMut(&Vec<u16>, &V)) {
        self.iterate_rec(&mut vec![], &mut visit)
    }

    pub fn remove_prefixes<U>(&mut self, prefixes: &PathsMap<U>) {
        prefixes.iterate(|prefix, _v| {
            self.remove_prefix(prefix);
        });
    }
    pub fn remove_edges(&mut self, forbidden_edges: &SmallBitmap) {
        let mut i = 0;
        while i < self.nodes.len() {
            let should_remove = if forbidden_edges.contains(self.nodes[i].0) {
                true
            } else if !self.nodes[i].1.nodes.is_empty() {
                self.nodes[i].1.remove_edges(forbidden_edges);
                self.nodes[i].1.nodes.is_empty()
            } else {
                false
            };
            if should_remove {
                self.nodes.remove(i);
            } else {
                i += 1;
            }
        }
    }
    pub fn remove_edge(&mut self, forbidden_edge: &u16) {
        let mut i = 0;
        while i < self.nodes.len() {
            let should_remove = if &self.nodes[i].0 == forbidden_edge {
                true
            } else if !self.nodes[i].1.nodes.is_empty() {
                self.nodes[i].1.remove_edge(forbidden_edge);
                self.nodes[i].1.nodes.is_empty()
            } else {
                false
            };
            if should_remove {
                self.nodes.remove(i);
            } else {
                i += 1;
            }
        }
    }
    pub fn remove_prefix(&mut self, forbidden_prefix: &[u16]) {
        let [first_edge, remaining_prefix @ ..] = forbidden_prefix else {
            self.nodes.clear();
            self.value = None;
            return;
        };

        let mut i = 0;
        while i < self.nodes.len() {
            let edge = self.nodes[i].0;
            let should_remove = if edge == *first_edge {
                self.nodes[i].1.remove_prefix(remaining_prefix);
                self.nodes[i].1.nodes.is_empty()
            } else {
                false
            };
            if should_remove {
                self.nodes.remove(i);
            } else {
                i += 1;
            }
        }
    }

    pub fn final_edges_after_prefix(&self, prefix: &[u16], visit: &mut impl FnMut(u16)) {
        let [first_edge, remaining_prefix @ ..] = prefix else {
            for node in self.nodes.iter() {
                if node.1.value.is_some() {
                    visit(node.0)
                }
            }
            return
        };
        for (edge, rest) in self.nodes.iter() {
            if edge == first_edge {
                return rest.final_edges_after_prefix(remaining_prefix, visit);
            }
        }
    }

    pub fn edge_indices_after_prefix(&self, prefix: &[u16]) -> Vec<u16> {
        let [first_edge, remaining_prefix @ ..] = prefix else {
            return self.nodes.iter().map(|n| n.0).collect();
        };
        for (edge, rest) in self.nodes.iter() {
            if edge == first_edge {
                return rest.edge_indices_after_prefix(remaining_prefix);
            }
        }
        vec![]
    }

    pub fn contains_prefix_of_path(&self, path: &[u16]) -> bool {
        if self.value.is_some() {
            return true;
        }
        match path {
            [] => false,
            [first_edge, remaining_path @ ..] => {
                for (edge, rest) in self.nodes.iter() {
                    if edge == first_edge {
                        return rest.contains_prefix_of_path(remaining_path);
                    }
                }
                false
            }
        }
    }
}

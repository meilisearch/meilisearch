// What is PathSet used for?
// For the empty_prefixes field in the EmptyPathsCache only :/
// but it could be used for more, like efficient computing of a set of paths

use crate::search::new::interner::Interned;

/// A set of [`Path`]
pub struct PathSet<T> {
    nodes: Vec<(Interned<T>, Self)>,
    is_end: bool,
}

impl<T> Clone for PathSet<T> {
    fn clone(&self) -> Self {
        Self { nodes: self.nodes.clone(), is_end: self.is_end }
    }
}

impl<T> std::fmt::Debug for PathSet<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathSet").field("nodes", &self.nodes).field("is_end", &self.is_end).finish()
    }
}

impl<T> Default for PathSet<T> {
    fn default() -> Self {
        Self { nodes: Default::default(), is_end: Default::default() }
    }
}

impl<T> PathSet<T> {
    pub fn insert(&mut self, mut edges: impl Iterator<Item = Interned<T>>) {
        match edges.next() {
            None => {
                self.is_end = true;
            }
            Some(first_edge) => {
                for (edge, next_node) in &mut self.nodes {
                    if edge == &first_edge {
                        return next_node.insert(edges);
                    }
                }
                let mut rest = PathSet::default();
                rest.insert(edges);
                self.nodes.push((first_edge, rest));
            }
        }
    }

    pub fn remove_edge(&mut self, forbidden_edge: Interned<T>) {
        let mut i = 0;
        while i < self.nodes.len() {
            let should_remove = if self.nodes[i].0 == forbidden_edge {
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

    pub fn final_edges_after_prefix(
        &self,
        prefix: &[Interned<T>],
        visit: &mut impl FnMut(Interned<T>),
    ) {
        let [first_edge, remaining_prefix @ ..] = prefix else {
            for node in self.nodes.iter() {
                if node.1.is_end {
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

    pub fn contains_prefix_of_path(&self, path: &[Interned<T>]) -> bool {
        if self.is_end {
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

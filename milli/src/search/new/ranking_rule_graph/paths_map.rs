// What is PathSet used for?
// For the empty_prefixes field in the EmptyPathsCache only :/
// but it could be used for more, like efficient computing of a set of paths

/// A set of [`Path`]
#[derive(Default, Debug, Clone)]
pub struct PathSet {
    nodes: Vec<(u16, PathSet)>,
    is_end: bool,
}
impl PathSet {
    pub fn insert(&mut self, mut edges: impl Iterator<Item = u16>) {
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

    pub fn final_edges_after_prefix(&self, prefix: &[u16], visit: &mut impl FnMut(u16)) {
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

    pub fn contains_prefix_of_path(&self, path: &[u16]) -> bool {
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

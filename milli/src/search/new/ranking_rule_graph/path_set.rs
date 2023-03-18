// What is PathSet used for?
// For the empty_prefixes field in the EmptyPathsCache only :/
// but it could be used for more, like efficient computing of a set of paths

use crate::search::new::{
    interner::{FixedSizeInterner, Interned},
    small_bitmap::SmallBitmap,
};

pub struct DeadEndsCache<T> {
    nodes: Vec<(Interned<T>, Self)>,
    pub forbidden: SmallBitmap<T>,
}
impl<T> DeadEndsCache<T> {
    pub fn new(for_interner: &FixedSizeInterner<T>) -> Self {
        Self { nodes: vec![], forbidden: SmallBitmap::for_interned_values_in(for_interner) }
    }
    pub fn forbid_condition(&mut self, condition: Interned<T>) {
        self.forbidden.insert(condition);
    }
    fn advance(&mut self, condition: Interned<T>) -> Option<&mut Self> {
        for (e, next_node) in &mut self.nodes {
            if condition == *e {
                return Some(next_node);
            }
        }
        None
    }
    pub fn forbidden_conditions_after_prefix(
        &mut self,
        mut prefix: &[Interned<T>],
    ) -> Option<SmallBitmap<T>> {
        let mut cursor = self;
        for c in prefix.iter() {
            if let Some(next) = cursor.advance(*c) {
                cursor = next;
            } else {
                return None;
            }
        }
        Some(cursor.forbidden.clone())
    }
    pub fn forbid_condition_after_prefix(
        &mut self,
        mut prefix: impl Iterator<Item = Interned<T>>,
        forbidden: Interned<T>,
    ) {
        match prefix.next() {
            None => {
                self.forbidden.insert(forbidden);
            }
            Some(first_condition) => {
                for (condition, next_node) in &mut self.nodes {
                    if condition == &first_condition {
                        return next_node.forbid_condition_after_prefix(prefix, forbidden);
                    }
                }
                let mut rest = DeadEndsCache {
                    nodes: vec![],
                    forbidden: SmallBitmap::new(self.forbidden.universe_length()),
                };
                rest.forbid_condition_after_prefix(prefix, forbidden);
                self.nodes.push((first_condition, rest));
            }
        }
    }
}
// /// A set of `Vec<Interned<T>>` implemented as a prefix tree.
// pub struct PathSet<T> {
//     nodes: Vec<(Interned<T>, Self)>,
//     is_end: bool,
// }

// impl<T> Clone for PathSet<T> {
//     fn clone(&self) -> Self {
//         Self { nodes: self.nodes.clone(), is_end: self.is_end }
//     }
// }

// impl<T> std::fmt::Debug for PathSet<T> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("PathSet").field("nodes", &self.nodes).field("is_end", &self.is_end).finish()
//     }
// }

// impl<T> Default for PathSet<T> {
//     fn default() -> Self {
//         Self { nodes: Default::default(), is_end: Default::default() }
//     }
// }

// impl<T> PathSet<T> {
//     pub fn insert(&mut self, mut conditions: impl Iterator<Item = Interned<T>>) {
//         match conditions.next() {
//             None => {
//                 self.is_end = true;
//             }
//             Some(first_condition) => {
//                 for (condition, next_node) in &mut self.nodes {
//                     if condition == &first_condition {
//                         return next_node.insert(conditions);
//                     }
//                 }
//                 let mut rest = PathSet::default();
//                 rest.insert(conditions);
//                 self.nodes.push((first_condition, rest));
//             }
//         }
//     }

//     pub fn remove_condition(&mut self, forbidden_condition: Interned<T>) {
//         let mut i = 0;
//         while i < self.nodes.len() {
//             let should_remove = if self.nodes[i].0 == forbidden_condition {
//                 true
//             } else if !self.nodes[i].1.nodes.is_empty() {
//                 self.nodes[i].1.remove_condition(forbidden_condition);
//                 self.nodes[i].1.nodes.is_empty()
//             } else {
//                 false
//             };
//             if should_remove {
//                 self.nodes.remove(i);
//             } else {
//                 i += 1;
//             }
//         }
//     }

//     pub fn final_conditions_after_prefix(
//         &self,
//         prefix: &[Interned<T>],
//         visit: &mut impl FnMut(Interned<T>),
//     ) {
//         let [first_condition, remaining_prefix @ ..] = prefix else {
//             for node in self.nodes.iter() {
//                 if node.1.is_end {
//                     visit(node.0)
//                 }
//             }
//             return
//         };
//         for (condition, rest) in self.nodes.iter() {
//             if condition == first_condition {
//                 return rest.final_conditions_after_prefix(remaining_prefix, visit);
//             }
//         }
//     }

//     pub fn contains_prefix_of_path(&self, path: &[Interned<T>]) -> bool {
//         if self.is_end {
//             return true;
//         }
//         match path {
//             [] => false,
//             [first_condition, remaining_path @ ..] => {
//                 for (condition, rest) in self.nodes.iter() {
//                     if condition == first_condition {
//                         return rest.contains_prefix_of_path(remaining_path);
//                     }
//                 }
//                 false
//             }
//         }
//     }
// }

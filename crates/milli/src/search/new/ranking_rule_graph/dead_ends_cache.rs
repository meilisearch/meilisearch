use crate::search::new::interner::{FixedSizeInterner, Interned};
use crate::search::new::small_bitmap::SmallBitmap;

pub struct DeadEndsCache<T> {
    // conditions and next could/should be part of the same vector
    conditions: Vec<Interned<T>>,
    next: Vec<Self>,
    pub forbidden: SmallBitmap<T>,
}
impl<T> Clone for DeadEndsCache<T> {
    fn clone(&self) -> Self {
        Self {
            conditions: self.conditions.clone(),
            next: self.next.clone(),
            forbidden: self.forbidden.clone(),
        }
    }
}
impl<T> DeadEndsCache<T> {
    pub fn new(for_interner: &FixedSizeInterner<T>) -> Self {
        Self {
            conditions: vec![],
            next: vec![],
            forbidden: SmallBitmap::for_interned_values_in(for_interner),
        }
    }
    pub fn forbid_condition(&mut self, condition: Interned<T>) {
        self.forbidden.insert(condition);
    }

    fn advance(&mut self, condition: Interned<T>) -> Option<&mut Self> {
        if let Some(idx) = self.conditions.iter().position(|c| *c == condition) {
            Some(&mut self.next[idx])
        } else {
            None
        }
    }
    pub fn forbidden_conditions_for_all_prefixes_up_to(
        &mut self,
        prefix: impl Iterator<Item = Interned<T>>,
    ) -> SmallBitmap<T> {
        let mut forbidden = self.forbidden.clone();
        let mut cursor = self;
        for c in prefix {
            if let Some(next) = cursor.advance(c) {
                cursor = next;
                forbidden.union(&cursor.forbidden);
            } else {
                break;
            }
        }
        forbidden
    }
    pub fn forbidden_conditions_after_prefix(
        &mut self,
        prefix: impl Iterator<Item = Interned<T>>,
    ) -> Option<SmallBitmap<T>> {
        let mut cursor = self;
        for c in prefix {
            if let Some(next) = cursor.advance(c) {
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
                if let Some(idx) = self.conditions.iter().position(|c| *c == first_condition) {
                    return self.next[idx].forbid_condition_after_prefix(prefix, forbidden);
                }
                let mut rest = DeadEndsCache {
                    conditions: vec![],
                    next: vec![],
                    forbidden: SmallBitmap::new(self.forbidden.universe_length()),
                };
                rest.forbid_condition_after_prefix(prefix, forbidden);
                self.conditions.push(first_condition);
                self.next.push(rest);
            }
        }
    }

    // pub fn debug_print(&self, indent: usize) {
    //     println!("{} {:?}", " ".repeat(indent), self.forbidden.iter().collect::<Vec<_>>());
    //     for (condition, next) in self.conditions.iter().zip(self.next.iter()) {
    //         println!("{} {condition}:", " ".repeat(indent));
    //         next.debug_print(indent + 2);
    //     }
    // }
}

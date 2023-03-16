use std::marker::PhantomData;

use super::interner::{FixedSizeInterner, Interned};

/// A compact set of [`Interned<T>`]
pub struct SmallBitmap<T> {
    internal: SmallBitmapInternal,
    _phantom: PhantomData<T>,
}
impl<T> Clone for SmallBitmap<T> {
    fn clone(&self) -> Self {
        Self { internal: self.internal.clone(), _phantom: PhantomData }
    }
}
impl<T> SmallBitmap<T> {
    pub fn for_interned_values_in(interner: &FixedSizeInterner<T>) -> Self {
        Self::new(interner.len())
    }
    pub fn new(universe_length: u16) -> Self {
        if universe_length <= 64 {
            Self { internal: SmallBitmapInternal::Tiny(0), _phantom: PhantomData }
        } else {
            Self {
                internal: SmallBitmapInternal::Small(
                    vec![0; 1 + universe_length as usize / 64].into_boxed_slice(),
                ),
                _phantom: PhantomData,
            }
        }
    }
    pub fn from_iter(
        xs: impl Iterator<Item = Interned<T>>,
        for_interner: &FixedSizeInterner<T>,
    ) -> Self {
        Self {
            internal: SmallBitmapInternal::from_iter(xs.map(|x| x.into_raw()), for_interner.len()),
            _phantom: PhantomData,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.internal.is_empty()
    }
    pub fn clear(&mut self) {
        self.internal.clear()
    }
    pub fn contains(&self, x: Interned<T>) -> bool {
        self.internal.contains(x.into_raw())
    }
    pub fn insert(&mut self, x: Interned<T>) {
        self.internal.insert(x.into_raw())
    }
    pub fn remove(&mut self, x: Interned<T>) {
        self.internal.remove(x.into_raw())
    }

    pub fn intersection(&mut self, other: &Self) {
        self.internal.intersection(&other.internal)
    }
    pub fn union(&mut self, other: &Self) {
        self.internal.union(&other.internal)
    }
    pub fn subtract(&mut self, other: &Self) {
        self.internal.subtract(&other.internal)
    }
    pub fn is_subset(&self, other: &Self) -> bool {
        self.internal.is_subset(&other.internal)
    }
    pub fn intersects(&self, other: &Self) -> bool {
        self.internal.intersects(&other.internal)
    }
    pub fn iter(&self) -> impl Iterator<Item = Interned<T>> + '_ {
        self.internal.iter().map(|x| Interned::from_raw(x))
    }
}
#[derive(Clone)]
pub enum SmallBitmapInternal {
    Tiny(u64),
    Small(Box<[u64]>),
}
impl SmallBitmapInternal {
    fn new(universe_length: u16) -> Self {
        if universe_length <= 64 {
            Self::Tiny(0)
        } else {
            Self::Small(vec![0; 1 + universe_length as usize / 64].into_boxed_slice())
        }
    }
    fn from_iter(xs: impl Iterator<Item = u16>, universe_length: u16) -> Self {
        let mut s = Self::new(universe_length);
        for x in xs {
            s.insert(x);
        }
        s
    }
    pub fn is_empty(&self) -> bool {
        match self {
            SmallBitmapInternal::Tiny(set) => *set == 0,
            SmallBitmapInternal::Small(sets) => {
                for set in sets.iter() {
                    if *set != 0 {
                        return false;
                    }
                }
                true
            }
        }
    }
    pub fn clear(&mut self) {
        match self {
            SmallBitmapInternal::Tiny(set) => *set = 0,
            SmallBitmapInternal::Small(sets) => {
                for set in sets.iter_mut() {
                    *set = 0;
                }
            }
        }
    }
    pub fn contains(&self, mut x: u16) -> bool {
        let set = match self {
            SmallBitmapInternal::Tiny(set) => *set,
            SmallBitmapInternal::Small(set) => {
                let idx = x / 64;
                x %= 64;
                set[idx as usize]
            }
        };
        set & 0b1 << x != 0
    }
    pub fn insert(&mut self, mut x: u16) {
        let set = match self {
            SmallBitmapInternal::Tiny(set) => set,
            SmallBitmapInternal::Small(set) => {
                let idx = x / 64;
                x %= 64;
                &mut set[idx as usize]
            }
        };
        *set |= 0b1 << x;
    }
    pub fn remove(&mut self, mut x: u16) {
        let set = match self {
            SmallBitmapInternal::Tiny(set) => set,
            SmallBitmapInternal::Small(set) => {
                let idx = x / 64;
                x %= 64;
                &mut set[idx as usize]
            }
        };
        *set &= !(0b1 << x);
    }

    pub fn intersection(&mut self, other: &SmallBitmapInternal) {
        self.apply_op(other, |a, b| *a &= b);
    }
    pub fn union(&mut self, other: &SmallBitmapInternal) {
        self.apply_op(other, |a, b| *a |= b);
    }
    pub fn subtract(&mut self, other: &SmallBitmapInternal) {
        self.apply_op(other, |a, b| *a &= !b);
    }

    pub fn apply_op(&mut self, other: &SmallBitmapInternal, op: impl Fn(&mut u64, u64)) {
        match (self, other) {
            (SmallBitmapInternal::Tiny(a), SmallBitmapInternal::Tiny(b)) => op(a, *b),
            (SmallBitmapInternal::Small(a), SmallBitmapInternal::Small(b)) => {
                assert!(a.len() == b.len(),);
                for (a, b) in a.iter_mut().zip(b.iter()) {
                    op(a, *b);
                }
            }
            _ => {
                panic!();
            }
        }
    }
    pub fn all_satisfy_op(
        &self,
        other: &SmallBitmapInternal,
        op: impl Fn(u64, u64) -> bool,
    ) -> bool {
        match (self, other) {
            (SmallBitmapInternal::Tiny(a), SmallBitmapInternal::Tiny(b)) => op(*a, *b),
            (SmallBitmapInternal::Small(a), SmallBitmapInternal::Small(b)) => {
                assert!(a.len() == b.len());
                for (a, b) in a.iter().zip(b.iter()) {
                    if !op(*a, *b) {
                        return false;
                    }
                }
                true
            }
            _ => {
                panic!();
            }
        }
    }
    pub fn any_satisfy_op(
        &self,
        other: &SmallBitmapInternal,
        op: impl Fn(u64, u64) -> bool,
    ) -> bool {
        match (self, other) {
            (SmallBitmapInternal::Tiny(a), SmallBitmapInternal::Tiny(b)) => op(*a, *b),
            (SmallBitmapInternal::Small(a), SmallBitmapInternal::Small(b)) => {
                assert!(a.len() == b.len());
                for (a, b) in a.iter().zip(b.iter()) {
                    if op(*a, *b) {
                        return true;
                    }
                }
                false
            }
            _ => {
                panic!();
            }
        }
    }
    pub fn is_subset(&self, other: &SmallBitmapInternal) -> bool {
        self.all_satisfy_op(other, |a, b| a & !b == 0)
    }
    pub fn intersects(&self, other: &SmallBitmapInternal) -> bool {
        self.any_satisfy_op(other, |a, b| a & b != 0)
    }
    pub fn iter(&self) -> SmallBitmapInternalIter<'_> {
        match self {
            SmallBitmapInternal::Tiny(x) => SmallBitmapInternalIter::Tiny(*x),
            SmallBitmapInternal::Small(xs) => {
                SmallBitmapInternalIter::Small { cur: xs[0], next: &xs[1..], base: 0 }
            }
        }
    }
}

pub enum SmallBitmapInternalIter<'b> {
    Tiny(u64),
    Small { cur: u64, next: &'b [u64], base: u16 },
}
impl<'b> Iterator for SmallBitmapInternalIter<'b> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SmallBitmapInternalIter::Tiny(set) => {
                if *set > 0 {
                    let idx = set.trailing_zeros() as u16;
                    *set &= *set - 1;
                    Some(idx)
                } else {
                    None
                }
            }
            SmallBitmapInternalIter::Small { cur, next, base } => {
                if *cur > 0 {
                    let idx = cur.trailing_zeros() as u16;
                    *cur &= *cur - 1;
                    Some(idx + *base)
                } else if next.is_empty() {
                    return None;
                } else {
                    *base += 64;
                    *cur = next[0];
                    *next = &next[1..];
                    self.next()
                }
            }
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::SmallBitmap;

//     #[test]
//     fn test_small_bitmap() {
//         let mut bitmap1 = SmallBitmap::new(32);
//         for x in 0..16 {
//             bitmap1.insert(x * 2);
//         }
//         let mut bitmap2 = SmallBitmap::new(32);
//         for x in 0..=10 {
//             bitmap2.insert(x * 3);
//         }
//         bitmap1.intersection(&bitmap2);
//         for v in bitmap1.iter() {
//             println!("{v}");
//         }
//     }
// }

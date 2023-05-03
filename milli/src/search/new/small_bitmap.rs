use std::marker::PhantomData;

use super::interner::{FixedSizeInterner, Interned};

/// A compact set of [`Interned<T>`]
///
/// This set optimizes storage by storing the set of values in a bitmap, and further optimizes
/// for bitmaps where the highest possible index (describing the limits of the "universe")
/// is smaller than 64 by storing them as a `u64`.
pub struct SmallBitmap<T> {
    // internals are not typed as they only represent the indexes that are set
    internal: SmallBitmapInternal,
    // restores typing with a tag
    _phantom: PhantomData<T>,
}

// manual implementation for when `T` is not Clone.
impl<T> Clone for SmallBitmap<T> {
    fn clone(&self) -> Self {
        Self { internal: self.internal.clone(), _phantom: PhantomData }
    }
}

impl<T> SmallBitmap<T> {
    /// Constructs a new, **empty**, `SmallBitmap<T>` with an universe large enough to hold all elements
    /// from `interner`.
    ///
    /// The constructed bitmap does not refer to any element in the interner, use [`from_iter`] if there should be
    /// some interned values in the bitmap after construction.
    pub fn for_interned_values_in(interner: &FixedSizeInterner<T>) -> Self {
        Self::new(interner.len())
    }

    /// Constructs a new, **empty**, `SmallBitmap<T>` with an universe at least as large as specified.
    ///
    /// If the passed universe length is not a multiple of 64, it will be rounded up to the next multiple of 64.
    pub fn new(universe_length: u16) -> Self {
        if universe_length <= 64 {
            Self { internal: SmallBitmapInternal::Tiny(0), _phantom: PhantomData }
        } else {
            Self {
                internal: SmallBitmapInternal::Small(
                    vec![0; 1 + (universe_length - 1) as usize / 64].into_boxed_slice(),
                ),
                _phantom: PhantomData,
            }
        }
    }

    /// The highest index that can be set in this bitmap.
    ///
    /// The universe length is always a multiple of 64, and may be higher than the value passed to [`Self::new`].
    pub fn universe_length(&self) -> u16 {
        self.internal.universe_length()
    }

    /// Constructs a new `SmallBitmap<T>` with an universe large enough to hold all elements
    /// from `from_interner`, and containing all the `Interned<T>` produced by `xs`.
    ///
    /// It is a logic error to pass an iterator producing `Interned<T>`s that don't belong to the passed interner.
    ///
    /// # Panics
    ///
    /// - If `xs` produces an element that doesn't fit the universe length obtained from `for_interner`.
    pub fn from_iter(
        xs: impl Iterator<Item = Interned<T>>,
        for_interner: &FixedSizeInterner<T>,
    ) -> Self {
        Self {
            internal: SmallBitmapInternal::from_iter(xs.map(|x| x.into_raw()), for_interner.len()),
            _phantom: PhantomData,
        }
    }

    /// Returns `true` if this bitmap does not contain any `Interned<T>`.
    pub fn is_empty(&self) -> bool {
        self.internal.is_empty()
    }

    /// Removes all `Interned<T>` from this bitmap, such that it [`is_empty`] returns `true` after this call.
    pub fn clear(&mut self) {
        self.internal.clear()
    }

    /// Whether `x` is part of the bitmap.
    ///
    /// It is a logic error to pass an `Interned<T>` from a different interner that the one this bitmap references.
    ///
    /// # Panics
    ///
    /// - if `x` does not fit in [`universe_length`]
    pub fn contains(&self, x: Interned<T>) -> bool {
        self.internal.contains(x.into_raw())
    }

    /// Adds `x` to the bitmap, such that [`contains(x)`] returns `true` after this call.
    ///
    /// It is a logic error to pass an `Interned<T>` from a different interner that the one this bitmap references.
    ///
    /// # Panics
    ///
    /// - if `x` does not fit in [`universe_length`]
    pub fn insert(&mut self, x: Interned<T>) {
        self.internal.insert(x.into_raw())
    }

    /// Removes `x` from the bitmap, such that [`contains(x)`] returns `false` after this call.
    ///
    /// It is a logic error to pass an `Interned<T>` from a different interner that the one this bitmap references.
    ///
    /// # Panics
    ///
    /// - if `x` does not fit in [`universe_length`]
    pub fn remove(&mut self, x: Interned<T>) {
        self.internal.remove(x.into_raw())
    }

    /// Modifies in place this bitmap to retain only the elements that are also present in `other`.
    ///
    /// # Panics
    ///
    /// - if the universe lengths of `self` and `other` differ
    pub fn intersection(&mut self, other: &Self) {
        self.internal.intersection(&other.internal)
    }

    /// Modifies in place this bitmap to add the elements that are present in `other`.
    ///
    /// # Panics
    ///
    /// - if the universe lengths of `self` and `other` differ
    pub fn union(&mut self, other: &Self) {
        self.internal.union(&other.internal)
    }

    /// Modifies in place this bitmap to remove the elements that are also present in `other`.
    ///
    /// # Panics
    ///
    /// - if the universe lengths of `self` and `other` differ
    pub fn subtract(&mut self, other: &Self) {
        self.internal.subtract(&other.internal)
    }

    /// Whether all the elements of `self` are contained in `other`.
    ///
    /// # Panics
    ///
    /// - if the universe lengths of `self` and `other` differ
    pub fn is_subset(&self, other: &Self) -> bool {
        self.internal.is_subset(&other.internal)
    }

    /// Whether any element of `self` is contained in `other`.
    ///
    /// # Panics
    ///
    /// - if the universe lengths of `self` and `other` differ
    pub fn intersects(&self, other: &Self) -> bool {
        self.internal.intersects(&other.internal)
    }

    /// Returns an iterator of the `Interned<T>` that are present in this bitmap.
    pub fn iter(&self) -> impl Iterator<Item = Interned<T>> + '_ {
        self.internal.iter().map(|x| Interned::from_raw(x))
    }
}
#[derive(Clone)]
enum SmallBitmapInternal {
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
    pub fn universe_length(&self) -> u16 {
        match &self {
            SmallBitmapInternal::Tiny(_) => 64,
            SmallBitmapInternal::Small(xs) => 64 * xs.len() as u16,
        }
    }

    fn get_set_index(&self, x: u16) -> (u64, u16) {
        match self {
            SmallBitmapInternal::Tiny(set) => {
                assert!(
                    x < 64,
                    "index out of bounds: the universe length is 64 but the index is {}",
                    x
                );
                (*set, x)
            }
            SmallBitmapInternal::Small(set) => {
                let idx = (x as usize) / 64;
                assert!(
                    idx < set.len(),
                    "index out of bounds: the universe length is {} but the index is {}",
                    self.universe_length(),
                    x
                );
                (set[idx], x % 64)
            }
        }
    }

    fn get_set_index_mut(&mut self, x: u16) -> (&mut u64, u16) {
        match self {
            SmallBitmapInternal::Tiny(set) => {
                assert!(
                    x < 64,
                    "index out of bounds: the universe length is 64 but the index is {}",
                    x
                );
                (set, x)
            }
            SmallBitmapInternal::Small(set) => {
                let idx = (x as usize) / 64;
                assert!(
                    idx < set.len(),
                    "index out of bounds: the universe length is {} but the index is {}",
                    64 * set.len() as u16,
                    x
                );
                (&mut set[idx], x % 64)
            }
        }
    }

    pub fn contains(&self, x: u16) -> bool {
        let (set, x) = self.get_set_index(x);
        set & 0b1 << x != 0
    }

    pub fn insert(&mut self, x: u16) {
        let (set, x) = self.get_set_index_mut(x);
        *set |= 0b1 << x;
    }

    pub fn remove(&mut self, x: u16) {
        let (set, x) = self.get_set_index_mut(x);
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
                assert!(
                    a.len() == b.len(),
                    "universe length mismatch: left is {}, but right is {}",
                    a.len() * 64,
                    other.universe_length()
                );
                for (a, b) in a.iter_mut().zip(b.iter()) {
                    op(a, *b);
                }
            }
            (this, other) => {
                panic!(
                    "universe length mismatch: left is {}, but right is {}",
                    this.universe_length(),
                    other.universe_length()
                );
            }
        }
    }
    fn all_satisfy_op(&self, other: &SmallBitmapInternal, op: impl Fn(u64, u64) -> bool) -> bool {
        match (self, other) {
            (SmallBitmapInternal::Tiny(a), SmallBitmapInternal::Tiny(b)) => op(*a, *b),
            (SmallBitmapInternal::Small(a), SmallBitmapInternal::Small(b)) => {
                assert!(
                    a.len() == b.len(),
                    "universe length mismatch: left is {}, but right is {}",
                    a.len() * 64,
                    other.universe_length()
                );
                for (a, b) in a.iter().zip(b.iter()) {
                    if !op(*a, *b) {
                        return false;
                    }
                }
                true
            }
            _ => {
                panic!(
                    "universe length mismatch: left is {}, but right is {}",
                    self.universe_length(),
                    other.universe_length()
                );
            }
        }
    }
    fn any_satisfy_op(&self, other: &SmallBitmapInternal, op: impl Fn(u64, u64) -> bool) -> bool {
        match (self, other) {
            (SmallBitmapInternal::Tiny(a), SmallBitmapInternal::Tiny(b)) => op(*a, *b),
            (SmallBitmapInternal::Small(a), SmallBitmapInternal::Small(b)) => {
                assert!(
                    a.len() == b.len(),
                    "universe length mismatch: left is {}, but right is {}",
                    a.len() * 64,
                    other.universe_length()
                );
                for (a, b) in a.iter().zip(b.iter()) {
                    if op(*a, *b) {
                        return true;
                    }
                }
                false
            }
            _ => {
                panic!(
                    "universe length mismatch: left is {}, but right is {}",
                    self.universe_length(),
                    other.universe_length()
                );
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

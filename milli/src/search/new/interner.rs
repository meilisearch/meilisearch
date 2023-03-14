use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use fxhash::FxHashMap;

/// An index within a [`Interner<T>`] structure.
pub struct Interned<T> {
    idx: u16,
    _phantom: PhantomData<T>,
}
impl<T> Interned<T> {
    pub fn new(idx: u16) -> Self {
        Self { idx, _phantom: PhantomData }
    }
    pub fn into_inner(self) -> u16 {
        self.idx
    }
}

// TODO: the stable store should be replaced by a bump allocator
// and the interned value should be a pointer wrapper
// then we can get its value with `interned.get()` instead of `interner.get(interned)`
// and as a bonus, its validity is tracked with Rust's lifetime system
// one problem is that we need two lifetimes: one for the bump allocator, one for the
// hashmap
// but that's okay, we can use:
// ```
// struct Interner<'bump> {
//      bump: &'bump Bump,
//      lookup: FxHashMap
// }
// ```

/// An [`Interner`] is used to store a unique copy of a value of type `T`. This value
/// is then identified by a lightweight index of type [`Interned<T>`], which can
/// be copied, compared, and hashed efficiently. An immutable reference to the original value
/// can be retrieved using `self.get(interned)`.
#[derive(Clone)]
pub struct DedupInterner<T> {
    stable_store: Vec<T>,
    lookup: FxHashMap<T, Interned<T>>,
}
impl<T> Default for DedupInterner<T> {
    fn default() -> Self {
        Self { stable_store: Default::default(), lookup: Default::default() }
    }
}
impl<T> DedupInterner<T> {
    pub fn freeze(self) -> FixedSizeInterner<T> {
        FixedSizeInterner { stable_store: self.stable_store }
    }
}

impl<T> DedupInterner<T>
where
    T: Clone + Eq + Hash,
{
    pub fn insert(&mut self, s: T) -> Interned<T> {
        if let Some(interned) = self.lookup.get(&s) {
            *interned
        } else {
            assert!(self.stable_store.len() < u16::MAX as usize);
            self.stable_store.push(s.clone());
            let interned = Interned::new(self.stable_store.len() as u16 - 1);
            self.lookup.insert(s, interned);
            interned
        }
    }
    pub fn get(&self, interned: Interned<T>) -> &T {
        &self.stable_store[interned.idx as usize]
    }
}
#[derive(Clone)]
pub struct Interner<T> {
    stable_store: Vec<T>,
}
impl<T> Default for Interner<T> {
    fn default() -> Self {
        Self { stable_store: Default::default() }
    }
}
impl<T> Interner<T> {
    pub fn freeze(self) -> FixedSizeInterner<T> {
        FixedSizeInterner { stable_store: self.stable_store }
    }
    pub fn push(&mut self, s: T) -> Interned<T> {
        assert!(self.stable_store.len() < u16::MAX as usize);
        self.stable_store.push(s);
        Interned::new(self.stable_store.len() as u16 - 1)
    }
}

#[derive(Clone)]
pub struct FixedSizeInterner<T> {
    stable_store: Vec<T>,
}
impl<T: Clone> FixedSizeInterner<T> {
    pub fn new(length: u16, value: T) -> Self {
        Self { stable_store: vec![value; length as usize] }
    }
}

impl<T> FixedSizeInterner<T> {
    pub fn from_vec(store: Vec<T>) -> Self {
        Self { stable_store: store }
    }
    pub fn get(&self, interned: Interned<T>) -> &T {
        &self.stable_store[interned.idx as usize]
    }
    pub fn get_mut(&mut self, interned: Interned<T>) -> &mut T {
        &mut self.stable_store[interned.idx as usize]
    }

    pub fn len(&self) -> u16 {
        self.stable_store.len() as u16
    }

    pub fn map<U>(&self, map_f: impl Fn(&T) -> U) -> MappedInterner<U, T> {
        MappedInterner {
            stable_store: self.stable_store.iter().map(map_f).collect(),
            _phantom: PhantomData,
        }
    }
    pub fn indexes(&self) -> impl Iterator<Item = Interned<T>> {
        (0..self.stable_store.len()).map(|i| Interned::new(i as u16))
    }
    pub fn iter(&self) -> impl Iterator<Item = (Interned<T>, &T)> {
        self.stable_store.iter().enumerate().map(|(i, x)| (Interned::new(i as u16), x))
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Interned<T>, &mut T)> {
        self.stable_store.iter_mut().enumerate().map(|(i, x)| (Interned::new(i as u16), x))
    }
}
#[derive(Clone)]
pub struct MappedInterner<T, From> {
    stable_store: Vec<T>,
    _phantom: PhantomData<From>,
}

impl<T, From> MappedInterner<T, From> {
    pub fn get(&self, interned: Interned<From>) -> &T {
        &self.stable_store[interned.idx as usize]
    }
    pub fn get_mut(&mut self, interned: Interned<From>) -> &mut T {
        &mut self.stable_store[interned.idx as usize]
    }
    pub fn map<U>(&self, map_f: impl Fn(&T) -> U) -> MappedInterner<U, From> {
        MappedInterner {
            stable_store: self.stable_store.iter().map(map_f).collect(),
            _phantom: PhantomData,
        }
    }
    pub fn iter(&self) -> impl Iterator<Item = (Interned<From>, &T)> {
        self.stable_store.iter().enumerate().map(|(i, x)| (Interned::new(i as u16), x))
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Interned<From>, &mut T)> {
        self.stable_store.iter_mut().enumerate().map(|(i, x)| (Interned::new(i as u16), x))
    }
}
// Interned<T> boilerplate implementations

impl<T> Hash for Interned<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.idx.hash(state);
    }
}

impl<T: Ord> Ord for Interned<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.idx.cmp(&other.idx)
    }
}

impl<T> PartialOrd for Interned<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.idx.partial_cmp(&other.idx)
    }
}

impl<T> Eq for Interned<T> {}

impl<T> PartialEq for Interned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx
    }
}
impl<T> Clone for Interned<T> {
    fn clone(&self) -> Self {
        Self { idx: self.idx, _phantom: PhantomData }
    }
}

impl<T> Copy for Interned<T> {}

impl<T> fmt::Display for Interned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.idx, f)
    }
}
impl<T> fmt::Debug for Interned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.idx, f)
    }
}

use fxhash::FxHashMap;
use std::hash::Hash;
use std::marker::PhantomData;

pub struct Interned<T> {
    idx: u32,
    _phantom: PhantomData<T>,
}

impl<T> Interned<T> {
    fn new(idx: u32) -> Self {
        Self { idx, _phantom: PhantomData }
    }
}

pub struct Interner<T> {
    stable_store: Vec<T>,
    lookup: FxHashMap<T, Interned<T>>,
}
impl<T> Default for Interner<T> {
    fn default() -> Self {
        Self { stable_store: Default::default(), lookup: Default::default() }
    }
}

impl<T> Interner<T>
where
    T: Clone + Eq + Hash,
{
    pub fn insert(&mut self, s: T) -> Interned<T> {
        if let Some(interned) = self.lookup.get(&s) {
            *interned
        } else {
            self.stable_store.push(s.clone());
            let interned = Interned::new(self.stable_store.len() as u32 - 1);
            self.lookup.insert(s, interned);
            interned
        }
    }
    pub fn get(&self, interned: Interned<T>) -> &T {
        &self.stable_store[interned.idx as usize]
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

use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};
use std::iter::repeat_with;
use std::mem;
use std::num::NonZeroUsize;

use hashbrown::hash_map::{DefaultHashBuilder, Entry};
use hashbrown::HashMap;

#[derive(Debug)]
pub struct Lru<K, V, S = DefaultHashBuilder> {
    lookup: HashMap<K, usize, S>,
    storage: FixedSizeList<LruNode<K, V>>,
}

impl<K: Eq + Hash, V> Lru<K, V> {
    /// Creates a new LRU cache that holds at most `capacity` elements.
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self { lookup: HashMap::new(), storage: FixedSizeList::new(capacity.get()) }
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> Lru<K, V, S> {
    /// Creates a new LRU cache that holds at most `capacity` elements
    /// and uses the provided hash builder to hash keys.
    pub fn with_hasher(capacity: NonZeroUsize, hash_builder: S) -> Lru<K, V, S> {
        Self {
            lookup: HashMap::with_hasher(hash_builder),
            storage: FixedSizeList::new(capacity.get()),
        }
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> Lru<K, V, S> {
    /// Returns a mutable reference to the value of the key in the cache or `None` if it is not present in the cache.
    ///
    /// Moves the key to the head of the LRU list if it exists.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let idx = *self.lookup.get(key)?;
        self.storage.move_front(idx).map(|node| &mut node.value)
    }
}

impl<K: Clone + Eq + Hash, V, S: BuildHasher> Lru<K, V, S> {
    pub fn push(&mut self, key: K, value: V) -> Option<(K, V)> {
        match self.lookup.entry(key) {
            Entry::Occupied(occ) => {
                // It's fine to unwrap here because:
                // * the entry already exists
                let node = self.storage.move_front(*occ.get()).unwrap();
                let old_value = mem::replace(&mut node.value, value);
                let old_key = occ.replace_key();
                Some((old_key, old_value))
            }
            Entry::Vacant(vac) => {
                let key = vac.key().clone();
                if self.storage.is_full() {
                    // It's fine to unwrap here because:
                    // * the cache capacity is non zero
                    // * the cache is full
                    let idx = self.storage.back_idx();
                    let node = self.storage.move_front(idx).unwrap();
                    let LruNode { key, value } = mem::replace(node, LruNode { key, value });
                    vac.insert(idx);
                    self.lookup.remove(&key);
                    Some((key, value))
                } else {
                    // It's fine to unwrap here because:
                    // * the cache capacity is non zero
                    // * the cache is not full
                    let (idx, _) = self.storage.push_front(LruNode { key, value }).unwrap();
                    vac.insert(idx);
                    None
                }
            }
        }
    }
}

impl<K, V, S> IntoIterator for Lru<K, V, S> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { lookup_iter: self.lookup.into_iter(), nodes: self.storage.nodes }
    }
}

pub struct IntoIter<K, V> {
    lookup_iter: hashbrown::hash_map::IntoIter<K, usize>,
    nodes: Box<[Option<FixedSizeListNode<LruNode<K, V>>>]>,
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let (_key, idx) = self.lookup_iter.next()?;
        let LruNode { key, value } = self.nodes.get_mut(idx)?.take()?.data;
        Some((key, value))
    }
}

#[derive(Debug)]
struct LruNode<K, V> {
    key: K,
    value: V,
}

#[derive(Debug)]
struct FixedSizeListNode<T> {
    prev: usize,
    next: usize,
    data: T,
}

#[derive(Debug)]
struct FixedSizeList<T> {
    nodes: Box<[Option<FixedSizeListNode<T>>]>,
    /// Also corresponds to the first `None` in the nodes.
    length: usize,
    // TODO Also, we probably do not need one of the front and back cursors.
    front: usize,
    back: usize,
}

impl<T> FixedSizeList<T> {
    fn new(capacity: usize) -> Self {
        Self {
            nodes: repeat_with(|| None).take(capacity).collect::<Vec<_>>().into_boxed_slice(),
            length: 0,
            front: usize::MAX,
            back: usize::MAX,
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    #[inline]
    fn back_idx(&self) -> usize {
        self.back
    }

    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.is_full() {
            None
        } else {
            let current_free = self.length;
            self.length += 1;
            Some(current_free)
        }
    }

    #[inline]
    fn node_mut(&mut self, idx: usize) -> Option<&mut FixedSizeListNode<T>> {
        self.nodes.get_mut(idx).and_then(|node| node.as_mut())
    }

    #[inline]
    fn node_ref(&self, idx: usize) -> Option<&FixedSizeListNode<T>> {
        self.nodes.get(idx).and_then(|node| node.as_ref())
    }

    #[inline]
    fn move_front(&mut self, idx: usize) -> Option<&mut T> {
        let node = self.nodes.get_mut(idx)?.take()?;
        if let Some(prev) = self.node_mut(node.prev) {
            prev.next = node.next;
        } else {
            self.front = node.next;
        }
        if let Some(next) = self.node_mut(node.next) {
            next.prev = node.prev;
        } else {
            self.back = node.prev;
        }

        if let Some(front) = self.node_mut(self.front) {
            front.prev = idx;
        }
        if self.node_ref(self.back).is_none() {
            self.back = idx;
        }

        let node = self.nodes.get_mut(idx).unwrap().insert(FixedSizeListNode {
            prev: usize::MAX,
            next: self.front,
            data: node.data,
        });
        self.front = idx;
        Some(&mut node.data)
    }

    #[inline]
    fn push_front(&mut self, data: T) -> Option<(usize, &mut T)> {
        let idx = self.next()?;
        if let Some(front) = self.node_mut(self.front) {
            front.prev = idx;
        }
        if self.node_ref(self.back).is_none() {
            self.back = idx;
        }
        let node = self.nodes.get_mut(idx).unwrap().insert(FixedSizeListNode {
            prev: usize::MAX,
            next: self.front,
            data,
        });
        self.front = idx;
        Some((idx, &mut node.data))
    }
}

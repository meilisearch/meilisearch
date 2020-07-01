// MIT License
// Copyright (c) 2016 Jerome Froelich

use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::usize;

use std::collections::HashMap;

use crate::FastMap8;

// Struct used to hold a reference to a key
#[doc(hidden)]
pub struct KeyRef<K> {
    k: *const K,
}

impl<K: Hash> Hash for KeyRef<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.k).hash(state) }
    }
}

impl<K: PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &KeyRef<K>) -> bool {
        unsafe { (*self.k).eq(&*other.k) }
    }
}

impl<K: Eq> Eq for KeyRef<K> {}

impl<K> Borrow<K> for KeyRef<K> {
    fn borrow(&self) -> &K {
        unsafe { &*self.k }
    }
}

// Struct used to hold a key value pair. Also contains references to previous and next entries
// so we can maintain the entries in a linked list ordered by their use.
struct LruEntry<K, V> {
    key: mem::MaybeUninit<K>,
    val: mem::MaybeUninit<V>,
    prev: *mut LruEntry<K, V>,
    next: *mut LruEntry<K, V>,
}

impl<K, V> LruEntry<K, V> {
    fn new(key: K, val: V) -> Self {
        LruEntry {
            key: mem::MaybeUninit::new(key),
            val: mem::MaybeUninit::new(val),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }

    fn new_sigil() -> Self {
        LruEntry {
            key: mem::MaybeUninit::uninit(),
            val: mem::MaybeUninit::uninit(),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

/// An LRU Cache.
pub struct LruCache<K, V> {
    map: FastMap8<KeyRef<K>, Box<LruEntry<K, V>>>,
    cap: usize,

    // head and tail are sigil nodes to faciliate inserting entries
    head: *mut LruEntry<K, V>,
    tail: *mut LruEntry<K, V>,
}

impl<K: Hash + Eq, V> LruCache<K, V> {
    /// Creates a new LRU Cache that holds at most `cap` items.
    pub fn new(cap: usize) -> LruCache<K, V> {
        let mut map = FastMap8::default();
        map.reserve(cap);
        LruCache::construct(cap, map)
    }

    /// Creates a new LRU Cache that never automatically evicts items.
    pub fn unbounded() -> LruCache<K, V> {
        LruCache::construct(usize::MAX, HashMap::default())
    }
}

impl<K: Hash + Eq, V> LruCache<K, V> {
    /// Creates a new LRU Cache with the given capacity.
    fn construct(cap: usize, map: FastMap8<KeyRef<K>, Box<LruEntry<K, V>>>) -> LruCache<K, V> {
        // NB: The compiler warns that cache does not need to be marked as mutable if we
        // declare it as such since we only mutate it inside the unsafe block.
        let cache = LruCache {
            map,
            cap,
            head: Box::into_raw(Box::new(LruEntry::new_sigil())),
            tail: Box::into_raw(Box::new(LruEntry::new_sigil())),
        };

        unsafe {
            (*cache.head).next = cache.tail;
            (*cache.tail).prev = cache.head;
        }

        cache
    }

    /// Puts a key-value pair into cache. If the capacity is reached the evicted entry is returned.
    pub fn insert(&mut self, k: K, mut v: V) -> Option<(K, V)> {
        let node_ptr = self.map.get_mut(&KeyRef { k: &k }).map(|node| {
            let node_ptr: *mut LruEntry<K, V> = &mut **node;
            node_ptr
        });

        match node_ptr {
            Some(node_ptr) => {
                // if the key is already in the cache just update its value and move it to the
                // front of the list
                unsafe { mem::swap(&mut v, &mut (*(*node_ptr).val.as_mut_ptr()) as &mut V) }
                self.detach(node_ptr);
                self.attach(node_ptr);
                None
            }
            None => {
                let (mut node, old_entry) = if self.len() == self.cap() {
                    // if the cache is full, remove the last entry so we can use it for the new key
                    let old_key = KeyRef {
                        k: unsafe { &(*(*(*self.tail).prev).key.as_ptr()) },
                    };
                    let mut old_node = self.map.remove(&old_key).unwrap();

                    // drop the node's current key and val so we can overwrite them
                    let old_entry = unsafe { (old_node.key.assume_init(), old_node.val.assume_init()) };

                    old_node.key = mem::MaybeUninit::new(k);
                    old_node.val = mem::MaybeUninit::new(v);

                    let node_ptr: *mut LruEntry<K, V> = &mut *old_node;
                    self.detach(node_ptr);

                    (old_node, Some(old_entry))
                } else {
                    // if the cache is not full allocate a new LruEntry
                    (Box::new(LruEntry::new(k, v)), None)
                };

                let node_ptr: *mut LruEntry<K, V> = &mut *node;
                self.attach(node_ptr);

                let keyref = unsafe { (*node_ptr).key.as_ptr() };
                self.map.insert(KeyRef { k: keyref }, node);

                old_entry
            }
        }
    }

    /// Returns a mutable reference to the value of the key in the cache or `None` if it
    /// is not present in the cache. Moves the key to the head of the LRU list if it exists.
    pub fn get_mut<'a, Q>(&'a mut self, k: &Q) -> Option<&'a mut V>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(node) = self.map.get_mut(k) {
            let node_ptr: *mut LruEntry<K, V> = &mut **node;

            self.detach(node_ptr);
            self.attach(node_ptr);

            Some(unsafe { &mut (*(*node_ptr).val.as_mut_ptr()) as &mut V })
        } else {
            None
        }
    }

    /// Returns a bool indicating whether the given key is in the cache. Does not update the
    /// LRU list.
    pub fn contains_key<Q>(&self, k: &Q) -> bool
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.contains_key(k)
    }

    /// Removes and returns the value corresponding to the key from the cache or
    /// `None` if it does not exist.
    pub fn remove<Q>(&mut self, k: &Q) -> Option<V>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.map.remove(&k) {
            None => None,
            Some(mut old_node) => {
                let node_ptr: *mut LruEntry<K, V> = &mut *old_node;
                self.detach(node_ptr);
                unsafe { Some(old_node.val.assume_init()) }
            }
        }
    }

    /// Removes and returns the key and value corresponding to the least recently
    /// used item or `None` if the cache is empty.
    pub fn remove_lru(&mut self) -> Option<(K, V)> {
        let node = self.remove_last()?;
        // N.B.: Can't destructure directly because of https://github.com/rust-lang/rust/issues/28536
        let node = *node;
        let LruEntry { key, val, .. } = node;
        unsafe { Some((key.assume_init(), val.assume_init())) }
    }

    /// Returns the number of key-value pairs that are currently in the the cache.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns a bool indicating whether the cache is empty or not.
    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }

    /// Returns the maximum number of key-value pairs the cache can hold.
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Resizes the cache. If the new capacity is smaller than the size of the current
    /// cache any entries past the new capacity are discarded.
    pub fn resize(&mut self, cap: usize) {
        // return early if capacity doesn't change
        if cap == self.cap {
            return;
        }

        while self.map.len() > cap {
            self.remove_last();
        }
        self.map.shrink_to_fit();

        self.cap = cap;
    }

    /// Clears the contents of the cache.
    pub fn clear(&mut self) {
        loop {
            match self.remove_last() {
                Some(_) => (),
                None => break,
            }
        }
    }

    /// An iterator visiting all entries in order. The iterator element type is `(&'a K, &'a V)`.
    pub fn iter<'a>(&'a self) -> Iter<'a, K, V> {
        Iter {
            len: self.len(),
            ptr: unsafe { (*self.head).next },
            phantom: PhantomData,
        }
    }

    fn remove_last(&mut self) -> Option<Box<LruEntry<K, V>>> {
        let prev;
        unsafe { prev = (*self.tail).prev }
        if prev != self.head {
            let old_key = KeyRef {
                k: unsafe { &(*(*(*self.tail).prev).key.as_ptr()) },
            };
            let mut old_node = self.map.remove(&old_key).unwrap();
            let node_ptr: *mut LruEntry<K, V> = &mut *old_node;
            self.detach(node_ptr);
            Some(old_node)
        } else {
            None
        }
    }

    fn detach(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            (*(*node).prev).next = (*node).next;
            (*(*node).next).prev = (*node).prev;
        }
    }

    fn attach(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            (*node).next = (*self.head).next;
            (*node).prev = self.head;
            (*self.head).next = node;
            (*(*node).next).prev = node;
        }
    }
}

impl<K, V> Drop for LruCache<K, V> {
    fn drop(&mut self) {
        self.map.values_mut().for_each(|e| unsafe {
            ptr::drop_in_place(e.key.as_mut_ptr());
            ptr::drop_in_place(e.val.as_mut_ptr());
        });
        // We rebox the head/tail, and because these are maybe-uninit
        // they do not have the absent k/v dropped.
        unsafe {
            let _head = *Box::from_raw(self.head);
            let _tail = *Box::from_raw(self.tail);
        }
    }
}

impl<'a, K: Hash + Eq, V> IntoIterator for &'a LruCache<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

// The compiler does not automatically derive Send and Sync for LruCache because it contains
// raw pointers. The raw pointers are safely encapsulated by LruCache though so we can
// implement Send and Sync for it below.
unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for LruCache<K, V> {}

impl<K: Hash + Eq, V> fmt::Debug for LruCache<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LruCache")
            .field("len", &self.len())
            .field("cap", &self.cap())
            .finish()
    }
}

/// An iterator over the entries of a `LruCache`.
pub struct Iter<'a, K: 'a, V: 'a> {
    len: usize,
    ptr: *const LruEntry<K, V>,
    phantom: PhantomData<&'a K>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        if self.len == 0 {
            return None;
        }

        let key = unsafe { &(*(*self.ptr).key.as_ptr()) as &K };
        let val = unsafe { &(*(*self.ptr).val.as_ptr()) as &V };

        self.len -= 1;
        self.ptr = unsafe { (*self.ptr).next };

        Some((key, val))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }

    fn count(self) -> usize {
        self.len
    }
}

impl<'a, K, V> ExactSizeIterator for Iter<'a, K, V> {}
impl<'a, K, V> FusedIterator for Iter<'a, K, V> {}

// The compiler does not automatically derive Send and Sync for Iter because it contains
// raw pointers.
unsafe impl<'a, K: Send, V: Send> Send for Iter<'a, K, V> {}
unsafe impl<'a, K: Sync, V: Sync> Sync for Iter<'a, K, V> {}

pub struct ArcCache<K, V>
where
    K: Eq + Hash,
{
    recent_set: LruCache<K, V>,
    recent_evicted: LruCache<K, ()>,
    frequent_set: LruCache<K, V>,
    frequent_evicted: LruCache<K, ()>,
    capacity: usize,
    p: usize,
}

impl<K, V> ArcCache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new(capacity: usize) -> ArcCache<K, V> {
        assert_ne!(capacity, 0, "cache length cannot be zero");
        ArcCache {
            recent_set: LruCache::new(capacity),
            recent_evicted: LruCache::new(capacity),
            frequent_set: LruCache::new(capacity),
            frequent_evicted: LruCache::new(capacity),
            capacity: capacity,
            p: 0,
        }
    }

    pub fn insert<F>(&mut self, key: K, value: V, mut merge: F) -> Vec<(K, V)>
    where F: FnMut(V, V) -> V
    {
        let mut evicted = Vec::new();
        if self.frequent_set.contains_key(&key) {
            evicted.extend(self.frequent_set.insert(key, value));
            return evicted;
        }
        if let Some(prev_value) = self.recent_set.remove(&key) {
            let value = (merge)(prev_value, value);
            evicted.extend(self.frequent_set.insert(key, value));
            return evicted;
        }
        if self.frequent_evicted.contains_key(&key) {
            let recent_evicted_len = self.recent_evicted.len();
            let frequent_evicted_len = self.frequent_evicted.len();
            let delta = if recent_evicted_len > frequent_evicted_len {
                recent_evicted_len / frequent_evicted_len
            } else {
                1
            };
            if delta < self.p {
                self.p -= delta;
            } else {
                self.p = 0
            }
            if self.recent_set.len() + self.frequent_set.len() >= self.capacity {
                evicted.extend(self.replace(true));
            }
            self.frequent_evicted.remove(&key);
            evicted.extend(self.frequent_set.insert(key, value));
            return evicted;
        }
        if self.recent_evicted.contains_key(&key) {
            let recent_evicted_len = self.recent_evicted.len();
            let frequent_evicted_len = self.frequent_evicted.len();
            let delta = if frequent_evicted_len > recent_evicted_len {
                frequent_evicted_len / recent_evicted_len
            } else {
                1
            };
            if delta <= self.capacity - self.p {
                self.p += delta;
            } else {
                self.p = self.capacity;
            }
            if self.recent_set.len() + self.frequent_set.len() >= self.capacity {
                evicted.extend(self.replace(false));
            }
            self.recent_evicted.remove(&key);
            evicted.extend(self.frequent_set.insert(key, value));
            return evicted;
        }
        let mut evicted = Vec::with_capacity(2);
        if self.recent_set.len() + self.frequent_set.len() >= self.capacity {
            evicted.extend(self.replace(false));
        }
        if self.recent_evicted.len() > self.capacity - self.p {
            self.recent_evicted.remove_lru();
        }
        if self.frequent_evicted.len() > self.p {
            self.frequent_evicted.remove_lru();
        }
        evicted.extend(self.recent_set.insert(key, value));
        evicted
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V>
    where
        K: Clone + Hash + Eq,
    {
        if let Some(value) = self.recent_set.remove(key) {
            self.frequent_set.insert((*key).clone(), value);
        }
        self.frequent_set.get_mut(key)
    }

    fn replace(&mut self, frequent_evicted_contains_key: bool) -> Option<(K, V)> {
        let recent_set_len = self.recent_set.len();
        if recent_set_len > 0
            && (recent_set_len > self.p
                || (recent_set_len == self.p && frequent_evicted_contains_key))
        {
            if let Some((old_key, old_val)) = self.recent_set.remove_lru() {
                self.recent_evicted.insert(old_key.clone(), ());
                return Some((old_key, old_val));
            }
        } else {
            if let Some((old_key, old_val)) = self.frequent_set.remove_lru() {
                self.frequent_evicted.insert(old_key.clone(), ());
                return Some((old_key, old_val));
            }
        }
        None
    }
}

impl<'a, K: 'a + Eq + Hash, V: 'a> IntoIterator for &'a ArcCache<K, V>{
    type Item = (&'a K, &'a V);
    type IntoIter = std::iter::Chain<Iter<'a, K, V>, Iter<'a, K, V>>;

    fn into_iter(self) -> Self::IntoIter {
        self.recent_set.iter().chain(&self.frequent_set)
    }
}

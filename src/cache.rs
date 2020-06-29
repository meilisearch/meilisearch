// Copyright 2015 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{Hash, BuildHasher};
use std::iter::FromIterator;

use linked_hash_map::LinkedHashMap;

/// An LRU cache.
#[derive(Clone)]
pub struct LruCache<K: Eq + Hash, V, S: BuildHasher = RandomState> {
    map: LinkedHashMap<K, V, S>,
    max_size: usize,
}

impl<K: Eq + Hash, V> LruCache<K, V> {
    /// Creates an empty cache that can hold at most `capacity` items.
    pub fn new(capacity: usize) -> Self {
        LruCache {
            map: LinkedHashMap::new(),
            max_size: capacity,
        }
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> LruCache<K, V, S> {
    /// Creates an empty cache that can hold at most `capacity` items with the given hash builder.
    pub fn with_hasher(capacity: usize, hash_builder: S) -> Self {
        LruCache { map: LinkedHashMap::with_hasher(hash_builder), max_size: capacity }
    }

    /// Checks if the map contains the given key.
    pub fn contains_key<Q: ?Sized>(&mut self, key: &Q) -> bool
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.get_mut(key).is_some()
    }

    /// Inserts a key-value pair into the cache. If the maximum size is reached the LRU is returned.
    pub fn insert(&mut self, k: K, v: V) -> Option<(K, V)> {
        self.map.insert(k, v);
        if self.len() > self.capacity() {
            self.remove_lru()
        } else {
            None
        }
    }

    /// Returns a mutable reference to the value corresponding to the given key in the cache, if
    /// any.
    pub fn get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.map.get_refresh(k)
    }

    pub fn peek_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.map.get_mut(k)
    }

    /// Removes the given key from the cache and returns its corresponding value.
    pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<V>
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.map.remove(k)
    }

    /// Returns the maximum number of key-value pairs the cache can hold.
    pub fn capacity(&self) -> usize {
        self.max_size
    }

    /// Sets the number of key-value pairs the cache can hold. Removes
    /// least-recently-used key-value pairs if necessary.
    pub fn set_capacity(&mut self, capacity: usize) {
        for _ in capacity..self.len() {
            self.remove_lru();
        }
        self.max_size = capacity;
    }

    /// Removes and returns the least recently used key-value pair as a tuple.
    #[inline]
    pub fn remove_lru(&mut self) -> Option<(K, V)> {
        self.map.pop_front()
    }

    /// Returns the number of key-value pairs in the cache.
    pub fn len(&self) -> usize { self.map.len() }

    /// Returns `true` if the cache contains no key-value pairs.
    pub fn is_empty(&self) -> bool { self.map.is_empty() }

    /// Removes all key-value pairs from the cache.
    pub fn clear(&mut self) { self.map.clear(); }
}

impl<K: Eq + Hash, V, S: BuildHasher> IntoIterator for LruCache<K, V, S> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        IntoIter(self.map.into_iter())
    }
}

#[derive(Clone)]
pub struct IntoIter<K, V>(linked_hash_map::IntoIter<K, V>);

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K, V> DoubleEndedIterator for IntoIter<K, V> {
    fn next_back(&mut self) -> Option<(K, V)> {
        self.0.next_back()
    }
}

impl<K, V> ExactSizeIterator for IntoIter<K, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

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

    pub fn insert(&mut self, key: K, value: V) -> Vec<(K, V)> {
        let mut evicted = Vec::new();
        if self.frequent_set.contains_key(&key) {
            evicted.extend(self.frequent_set.insert(key, value));
            return evicted;
        }
        if self.recent_set.contains_key(&key) {
            self.recent_set.remove(&key);
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
            return Vec::from_iter(self.frequent_set.insert(key, value));
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
        if let Some(value) = self.recent_set.remove(&key) {
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

impl<K: Eq + Hash, V> IntoIterator for ArcCache<K, V>{
    type Item = (K, V);
    type IntoIter = std::iter::Chain<IntoIter<K, V>, IntoIter<K, V>>;

    fn into_iter(self) -> Self::IntoIter {
        self.recent_set.into_iter().chain(self.frequent_set)
    }
}

//! Thread-safe `Vec`-backend LRU cache using [`std::sync::atomic::AtomicU64`] for synchronization.

use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe `Vec`-backend LRU cache
#[derive(Debug)]
pub struct Lru<T> {
    data: Vec<(AtomicU64, T)>,
    generation: AtomicU64,
    cap: usize,
}

impl<T> Lru<T> {
    /// Creates a new LRU cache with the specified capacity.
    ///
    /// The capacity is allocated up-front, and will never change through a [`Self::put`] operation.
    ///
    /// # Panics
    ///
    /// - If the capacity is 0.
    /// - If the capacity exceeds `isize::MAX` bytes.
    pub fn new(cap: usize) -> Self {
        assert_ne!(cap, 0, "The capacity of a cache cannot be 0");
        Self {
            // Note: since the element of the vector contains an AtomicU64, it is definitely not zero-sized so cap will never be usize::MAX.
            data: Vec::with_capacity(cap),
            generation: AtomicU64::new(0),
            cap,
        }
    }

    /// The capacity of this LRU cache, that is the maximum number of elements it can hold before evicting elements from the cache.
    ///
    /// The cache will contain at most this number of elements at any given time.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    fn next_generation(&self) -> u64 {
        // Acquire so this "happens-before" any potential store to a data cell (with Release ordering)
        let generation = self.generation.fetch_add(1, Ordering::Acquire);
        generation + 1
    }

    fn next_generation_mut(&mut self) -> u64 {
        let generation = self.generation.get_mut();
        *generation += 1;
        *generation
    }

    /// Add a value in the cache, evicting an older value if necessary.
    ///
    /// If a value was evicted from the cache, it is returned.
    ///
    /// # Complexity
    ///
    /// - If the cache is full, then linear in the capacity.
    /// - Otherwise constant.
    pub fn put(&mut self, value: T) -> Option<T> {
        // no need for a memory fence: we assume that whichever mechanism provides us synchronization
        // (very probably, a RwLock) takes care of fencing for us.

        let next_generation = self.next_generation_mut();
        let evicted = if self.is_full() { self.pop() } else { None };
        self.data.push((AtomicU64::new(next_generation), value));
        evicted
    }

    /// Evict the oldest value from the cache.
    ///
    /// If the cache is empty, `None` will be returned.
    ///
    /// # Complexity
    ///
    /// - Linear in the capacity of the cache.
    pub fn pop(&mut self) -> Option<T> {
        // Iterator::min_by_key provides shared references to its elements,
        // but we need (and can afford!) an exclusive one, so let's make an explicit loop
        let mut min_generation_index = None;
        for (index, (generation, _)) in self.data.iter_mut().enumerate() {
            let generation = *generation.get_mut();
            if let Some((_, min_generation)) = min_generation_index {
                if min_generation > generation {
                    min_generation_index = Some((index, generation));
                }
            } else {
                min_generation_index = Some((index, generation))
            }
        }
        min_generation_index.map(|(min_index, _)| self.data.swap_remove(min_index).1)
    }

    /// The current number of elements in the cache.
    ///
    /// This value is guaranteed to be less than or equal to [`Self::capacity`].
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if putting any additional element in the cache would cause the eviction of an element.
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }
}

pub struct LruMap<K, V>(Lru<(K, V)>);

impl<K, V> LruMap<K, V>
where
    K: Eq,
{
    /// Creates a new LRU cache map with the specified capacity.
    ///
    /// The capacity is allocated up-front, and will never change through a [`Self::insert`] operation.
    ///
    /// # Panics
    ///
    /// - If the capacity is 0.
    /// - If the capacity exceeds `isize::MAX` bytes.
    pub fn new(cap: usize) -> Self {
        Self(Lru::new(cap))
    }

    /// Gets a value in the cache map by its key.
    ///
    /// If no value matches, `None` will be returned.
    ///
    /// # Complexity
    ///
    /// - Linear in the capacity of the cache.
    pub fn get(&self, key: &K) -> Option<&V> {
        for (generation, (candidate, value)) in self.0.data.iter() {
            if key == candidate {
                generation.store(self.0.next_generation(), Ordering::Release);
                return Some(value);
            }
        }
        None
    }

    /// Gets a value in the cache map by its key.
    ///
    /// If no value matches, `None` will be returned.
    ///
    /// # Complexity
    ///
    /// - Linear in the capacity of the cache.
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let next_generation = self.0.next_generation_mut();
        for (generation, (candidate, value)) in self.0.data.iter_mut() {
            if key == candidate {
                *generation.get_mut() = next_generation;
                return Some(value);
            }
        }
        None
    }

    /// Inserts a value in the cache map by its key, replacing any existing value and returning any evicted value.
    ///
    /// # Complexity
    ///
    /// - Linear in the capacity of the cache.
    pub fn insert(&mut self, key: K, mut value: V) -> InsertionOutcome<K, V> {
        match self.get_mut(&key) {
            Some(old_value) => {
                std::mem::swap(old_value, &mut value);
                InsertionOutcome::Replaced(value)
            }
            None => match self.0.put((key, value)) {
                Some((key, value)) => InsertionOutcome::Evicted(key, value),
                None => InsertionOutcome::InsertedNew,
            },
        }
    }

    /// Removes an element from the cache map by its key, returning its value.
    ///
    /// Returns `None` if there was no element with this key in the cache.
    ///
    /// # Complexity
    ///
    /// - Linear in the capacity of the cache.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        for (index, (_, (candidate, _))) in self.0.data.iter_mut().enumerate() {
            if key == candidate {
                return Some(self.0.data.swap_remove(index).1 .1);
            }
        }
        None
    }
}

/// The result of an insertion in a LRU map.
pub enum InsertionOutcome<K, V> {
    /// The key was not in the cache, the key-value pair has been inserted.
    InsertedNew,
    /// The key was not in the cache and an old key-value pair was evicted from the cache to make room for its insertions.
    Evicted(K, V),
    /// The key was already in the cache map, its value has been updated.
    Replaced(V),
}

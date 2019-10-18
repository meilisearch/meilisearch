use hashbrown::HashMap;
use std::hash::Hash;

pub struct DistinctMap<K> {
    inner: HashMap<K, usize>,
    limit: usize,
    len: usize,
}

impl<K: Hash + Eq> DistinctMap<K> {
    pub fn new(limit: usize) -> Self {
        DistinctMap {
            inner: HashMap::new(),
            limit,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

pub struct BufferedDistinctMap<'a, K> {
    internal: &'a mut DistinctMap<K>,
    inner: HashMap<K, usize>,
    len: usize,
}

impl<'a, K: Hash + Eq> BufferedDistinctMap<'a, K> {
    pub fn new(internal: &'a mut DistinctMap<K>) -> BufferedDistinctMap<'a, K> {
        BufferedDistinctMap {
            internal,
            inner: HashMap::new(),
            len: 0,
        }
    }

    pub fn register(&mut self, key: K) -> bool {
        let internal_seen = self.internal.inner.get(&key).unwrap_or(&0);
        let inner_seen = self.inner.entry(key).or_insert(0);
        let seen = *internal_seen + *inner_seen;

        if seen < self.internal.limit {
            *inner_seen += 1;
            self.len += 1;
            true
        } else {
            false
        }
    }

    pub fn register_without_key(&mut self) -> bool {
        self.len += 1;
        true
    }

    pub fn transfert_to_internal(&mut self) {
        for (k, v) in self.inner.drain() {
            let value = self.internal.inner.entry(k).or_insert(0);
            *value += v;
        }

        self.internal.len += self.len;
        self.len = 0;
    }

    pub fn len(&self) -> usize {
        self.internal.len() + self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn easy_distinct_map() {
        let mut map = DistinctMap::new(2);
        let mut buffered = BufferedDistinctMap::new(&mut map);

        for x in &[1, 1, 1, 2, 3, 4, 5, 6, 6, 6, 6, 6] {
            buffered.register(x);
        }
        buffered.transfert_to_internal();
        assert_eq!(map.len(), 8);

        let mut map = DistinctMap::new(2);
        let mut buffered = BufferedDistinctMap::new(&mut map);
        assert_eq!(buffered.register(1), true);
        assert_eq!(buffered.register(1), true);
        assert_eq!(buffered.register(1), false);
        assert_eq!(buffered.register(1), false);

        assert_eq!(buffered.register(2), true);
        assert_eq!(buffered.register(3), true);
        assert_eq!(buffered.register(2), true);
        assert_eq!(buffered.register(2), false);

        buffered.transfert_to_internal();
        assert_eq!(map.len(), 5);
    }
}

use std::hash::Hash;

use hashbrown::HashMap;

pub struct DistinctMap<K> {
    inner: HashMap<K, usize>,
    limit: usize,
    len: usize,
}

impl<K: Hash + Eq> DistinctMap<K> {
    pub fn new(limit: usize) -> Self {
        DistinctMap {
            inner: HashMap::new(),
            limit: limit,
            len: 0,
        }
    }

    pub fn digest(&mut self, key: K) -> bool {
        let seen = self.inner.entry(key).or_insert(0);
        if *seen < self.limit {
            *seen += 1;
            self.len += 1;
            true
        } else {
            false
        }
    }

    pub fn accept_without_key(&mut self) -> bool {
        self.len += 1;
        true
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn easy_distinct_map() {
        let mut map = DistinctMap::new(2);
        for x in &[1, 1, 1, 2, 3, 4, 5, 6, 6, 6, 6, 6] {
            map.digest(x);
        }
        assert_eq!(map.len(), 8);

        let mut map = DistinctMap::new(2);
        assert_eq!(map.digest(1), true);
        assert_eq!(map.digest(1), true);
        assert_eq!(map.digest(1), false);
        assert_eq!(map.digest(1), false);

        assert_eq!(map.digest(2), true);
        assert_eq!(map.digest(3), true);
        assert_eq!(map.digest(2), true);
        assert_eq!(map.digest(2), false);

        assert_eq!(map.len(), 5);
    }
}

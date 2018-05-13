use std::collections::BTreeMap;

#[derive(Debug)]
pub struct CappedBTreeMap<K, V> {
    inner: BTreeMap<K, V>,
    capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Insertion<V> {
    OldValue(V),
    Evicted(V), // FIXME give (key *and* value)
    Nothing,
}

impl<K: Ord, V> CappedBTreeMap<K, V> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity != 0, "It is invalid to set the capacity to zero.");
        Self {
            inner: BTreeMap::new(),
            capacity: capacity,
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear()
    }

    /// This method insert the `key`, `value` pair in the tree *but* will
    /// remove the _smallest_ one, if the capacity is already reached,
    /// before insertion.
    ///
    /// The _smallest_ `value` is not removed if the `key` inserted is already
    /// present in the tree, in this case, the old replaced `value` is returned.
    ///
    /// ```
    /// # extern crate raptor;
    /// use raptor::{CappedBTreeMap, Insertion};
    ///
    /// let mut tree = CappedBTreeMap::new(3);
    ///
    /// let res = tree.insert(1, "a");
    /// assert_eq!(res, Insertion::Nothing);
    ///
    /// tree.insert(2, "b");
    /// tree.insert(3, "c");
    ///
    /// assert_eq!(tree.insert(4, "d"), Insertion::Evicted("c"));
    ///
    /// assert_eq!(tree.insert(1, "d"), Insertion::OldValue("a"));
    /// ```
    ///
    pub fn insert(&mut self, key: K, value: V) -> Insertion<V>
    where K: Clone,
    {
        if self.len() == self.capacity {
            if self.inner.contains_key(&key) {
                let value = self.inner.insert(key, value).unwrap();
                Insertion::OldValue(value)
            }
            else {
                let evicted_value = {
                    // it is not possible to panic because we have reached
                    // the capacity and the capacity cannot be set to zero.

                    // FIXME remove this clone, find a way to remove
                    //       the smallest key/value avoid borrowing problems
                    let key = self.inner.keys().next_back().unwrap().clone();
                    self.inner.remove(&key).unwrap()
                };

                self.inner.insert(key, value);
                Insertion::Evicted(evicted_value)
            }
        }
        else {
            self.inner.insert(key, value);
            Insertion::Nothing
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

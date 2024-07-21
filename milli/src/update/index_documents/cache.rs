use std::borrow::{Borrow, Cow};
use std::hash::Hash;
use std::iter::Chain;
use std::mem;
use std::num::NonZeroUsize;

use lru::{IntoIter, LruCache};
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::update::del_add::{DelAdd, KvWriterDelAdd};
use crate::CboRoaringBitmapCodec;

const ENABLED: bool = true;

pub struct SorterCacheDelAddCboRoaringBitmap<const N: usize, MF> {
    cache: ArcCache<SmallVec<[u8; N]>, DelAddRoaringBitmap>,
    prefix: &'static [u8; 3],
    sorter: grenad::Sorter<MF>,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
    conn: sled::Db,
}

impl<const N: usize, MF> SorterCacheDelAddCboRoaringBitmap<N, MF> {
    pub fn new(
        cap: NonZeroUsize,
        sorter: grenad::Sorter<MF>,
        prefix: &'static [u8; 3],
        conn: sled::Db,
    ) -> Self {
        SorterCacheDelAddCboRoaringBitmap {
            cache: ArcCache::new(cap),
            prefix,
            sorter,
            deladd_buffer: Vec::new(),
            cbo_buffer: Vec::new(),
            conn,
        }
    }
}

impl<const N: usize, MF, U> SorterCacheDelAddCboRoaringBitmap<N, MF>
where
    MF: for<'a> Fn(&[u8], &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, U>,
{
    pub fn insert_del_u32(&mut self, key: &[u8], n: u32) -> Result<(), grenad::Error<U>> {
        if !ENABLED {
            return self.write_entry_to_sorter(key, DelAddRoaringBitmap::new_del_u32(n));
        }

        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                del.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_del_u32(n);
                for (key, deladd) in self.cache.push(key.into(), value) {
                    self.write_entry_to_sorter(key, deladd)?;
                }
            }
        }

        match evicted {
            Some((key, value)) => self.write_entry_to_sorter(key, value),
            None => Ok(()),
        }
    }

    pub fn insert_del(
        &mut self,
        key: &[u8],
        bitmap: RoaringBitmap,
    ) -> Result<(), grenad::Error<U>> {
        if !ENABLED {
            return self.write_entry_to_sorter(key, DelAddRoaringBitmap::new_del(bitmap));
        }

        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                *del.get_or_insert_with(RoaringBitmap::new) |= bitmap;
            }
            None => {
                let value = DelAddRoaringBitmap::new_del(bitmap);
                for (key, deladd) in self.cache.push(key.into(), value) {
                    self.write_entry_to_sorter(key, deladd)?;
                }
            }
        }

        match evicted {
            Some((key, value)) => self.write_entry_to_sorter(key, value),
            None => Ok(()),
        }
    }

    pub fn insert_add_u32(&mut self, key: &[u8], n: u32) -> Result<(), grenad::Error<U>> {
        if !ENABLED {
            return self.write_entry_to_sorter(key, DelAddRoaringBitmap::new_add_u32(n));
        }

        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                add.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_add_u32(n);
                for (key, deladd) in self.cache.push(key.into(), value) {
                    self.write_entry_to_sorter(key, deladd)?;
                }
            }
        }

        match evicted {
            Some((key, value)) => self.write_entry_to_sorter(key, value),
            None => Ok(()),
        }
    }

    pub fn insert_add(
        &mut self,
        key: &[u8],
        bitmap: RoaringBitmap,
    ) -> Result<(), grenad::Error<U>> {
        if !ENABLED {
            return self.write_entry_to_sorter(key, DelAddRoaringBitmap::new_add(bitmap));
        }

        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                *add.get_or_insert_with(RoaringBitmap::new) |= bitmap;
            }
            None => {
                let value = DelAddRoaringBitmap::new_add(bitmap);
                for (key, deladd) in self.cache.push(key.into(), value) {
                    self.write_entry_to_sorter(key, deladd)?;
                }
            }
        }

        match evicted {
            Some((key, value)) => self.write_entry_to_sorter(key, value),
            None => Ok(()),
        }
    }

    pub fn insert_del_add_u32(&mut self, key: &[u8], n: u32) -> Result<(), grenad::Error<U>> {
        if !ENABLED {
            return self.write_entry_to_sorter(key, DelAddRoaringBitmap::new_del_add_u32(n));
        }

        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del, add }) => {
                del.get_or_insert_with(RoaringBitmap::new).insert(n);
                add.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_del_add_u32(n);
                for (key, deladd) in self.cache.push(key.into(), value) {
                    self.write_entry_to_sorter(key, deladd)?;
                }
            }
        }

        match evicted {
            Some((key, value)) => self.write_entry_to_sorter(key, value),
            None => Ok(()),
        }
    }

    fn write_entry_to_sorter<A: AsRef<[u8]>>(
        &mut self,
        key: A,
        deladd: DelAddRoaringBitmap,
    ) -> Result<(), grenad::Error<U>> {
        self.deladd_buffer.clear();
        let mut value_writer = KvWriterDelAdd::new(&mut self.deladd_buffer);
        match deladd {
            DelAddRoaringBitmap { del: Some(del), add: None } => {
                self.cbo_buffer.clear();
                CboRoaringBitmapCodec::serialize_into(&del, &mut self.cbo_buffer);
                value_writer.insert(DelAdd::Deletion, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: None, add: Some(add) } => {
                self.cbo_buffer.clear();
                CboRoaringBitmapCodec::serialize_into(&add, &mut self.cbo_buffer);
                value_writer.insert(DelAdd::Addition, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: Some(del), add: Some(add) } => {
                self.cbo_buffer.clear();
                CboRoaringBitmapCodec::serialize_into(&del, &mut self.cbo_buffer);
                value_writer.insert(DelAdd::Deletion, &self.cbo_buffer)?;

                self.cbo_buffer.clear();
                CboRoaringBitmapCodec::serialize_into(&add, &mut self.cbo_buffer);
                value_writer.insert(DelAdd::Addition, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: None, add: None } => return Ok(()),
        }
        self.cbo_buffer.clear();
        self.cbo_buffer.extend_from_slice(self.prefix);
        self.cbo_buffer.extend_from_slice(key.as_ref());
        self.conn.merge(&self.cbo_buffer, 1u32.to_ne_bytes()).unwrap();
        self.sorter.insert(key, value_writer.into_inner().unwrap())
    }

    pub fn direct_insert(&mut self, key: &[u8], val: &[u8]) -> Result<(), grenad::Error<U>> {
        self.cbo_buffer.clear();
        self.cbo_buffer.extend_from_slice(self.prefix);
        self.cbo_buffer.extend_from_slice(key);
        self.conn.merge(&self.cbo_buffer, 1u32.to_ne_bytes()).unwrap();
        self.sorter.insert(key, val)
    }

    pub fn into_sorter(mut self) -> Result<grenad::Sorter<MF>, grenad::Error<U>> {
        let default_arc = ArcCache::new(NonZeroUsize::MIN);
        for (key, deladd) in mem::replace(&mut self.cache, default_arc) {
            self.write_entry_to_sorter(key, deladd)?;
        }
        Ok(self.sorter)
    }
}

pub struct DelAddRoaringBitmap {
    pub del: Option<RoaringBitmap>,
    pub add: Option<RoaringBitmap>,
}

impl DelAddRoaringBitmap {
    fn new_del_add_u32(n: u32) -> Self {
        DelAddRoaringBitmap {
            del: Some(RoaringBitmap::from([n])),
            add: Some(RoaringBitmap::from([n])),
        }
    }

    fn new_del(bitmap: RoaringBitmap) -> Self {
        DelAddRoaringBitmap { del: Some(bitmap), add: None }
    }

    fn new_del_u32(n: u32) -> Self {
        DelAddRoaringBitmap { del: Some(RoaringBitmap::from([n])), add: None }
    }

    fn new_add(bitmap: RoaringBitmap) -> Self {
        DelAddRoaringBitmap { del: None, add: Some(bitmap) }
    }

    fn new_add_u32(n: u32) -> Self {
        DelAddRoaringBitmap { del: None, add: Some(RoaringBitmap::from([n])) }
    }
}

// TODO support custom State (3rd param S of LruCache)
pub struct ArcCache<K, V> {
    recent_set: LruCache<K, V>,
    recent_evicted: LruCache<K, ()>,
    frequent_set: LruCache<K, V>,
    frequent_evicted: LruCache<K, ()>,
    capacity: NonZeroUsize,
    // negative means shrinking recent and increasing frequent
    // positive means shrinking frequent and increasing recent
    p: isize,
}

impl<K: Eq + Hash, V> ArcCache<K, V> {
    pub fn new(cap: NonZeroUsize) -> Self {
        ArcCache {
            recent_set: LruCache::new(cap),
            recent_evicted: LruCache::new(cap),
            frequent_set: LruCache::new(cap),
            frequent_evicted: LruCache::new(cap),
            capacity: cap,
            p: 0,
        }
    }
}

impl<K: Eq + Hash + Clone, V> ArcCache<K, V> {
    pub fn get_mut<Q>(&mut self, k: &Q) -> (Option<&mut V>, Option<(K, V)>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let evicted = match self.recent_set.pop_entry(k) {
            Some((key, value)) => self.frequent_set.push(key, value),
            None => None,
        };

        (self.frequent_set.get_mut(k), evicted)
    }

    pub fn push(&mut self, k: K, v: V) -> SmallVec<[(K, V); 2]> {
        let mut evicted = SmallVec::new();

        if self.frequent_set.contains(&k) {
            if let Some(evicted_entry) = self.frequent_set.push(k, v) {
                evicted.push(evicted_entry);
            }
        } else if let Some((k, v)) = self.recent_set.pop_entry(&k) {
            if let Some(evicted_entry) = self.frequent_set.push(k, v) {
                evicted.push(evicted_entry);
            }
        } else if self.frequent_evicted.contains(&k) {
            let delta = if self.recent_evicted.len() > self.frequent_evicted.len() {
                self.recent_evicted.len() / self.frequent_evicted.len()
            } else {
                1
            };

            if (delta as isize) < self.p {
                self.p = self.p.saturating_sub(delta as isize);
            } else {
                self.p = 0;
            }

            if self.recent_set.len() + self.frequent_set.len() >= self.capacity.get() {
                if let Some(evicted_entry) = self.replace(true) {
                    evicted.push(evicted_entry);
                }
            }

            self.frequent_evicted.pop(&k);
            if let Some(evicted_entry) = self.frequent_set.push(k, v) {
                evicted.push(evicted_entry);
            }
        } else if self.recent_evicted.contains(&k) {
            let delta = if self.frequent_evicted.len() > self.recent_evicted.len() {
                self.frequent_evicted.len() / self.recent_evicted.len()
            } else {
                1
            };

            if (delta as isize) <= (self.capacity.get() as isize) - self.p {
                self.p = self.p.saturating_add(delta as isize);
            } else {
                self.p = self.capacity.get() as isize;
            }

            if self.recent_set.len() + self.frequent_set.len() >= self.capacity.get() {
                if let Some(evicted_entry) = self.replace(false) {
                    evicted.push(evicted_entry);
                }
            }

            self.recent_evicted.pop(&k);
            if let Some(evicted_entry) = self.frequent_set.push(k, v) {
                evicted.push(evicted_entry)
            }
        } else if self.recent_set.len() + self.frequent_set.len() >= self.capacity.get() {
            if let Some(evicted_entry) = self.replace(false) {
                evicted.push(evicted_entry);
            }
        } else if let Some(evicted_entry) = self.recent_set.push(k, v) {
            evicted.push(evicted_entry);
        }

        evicted
    }

    fn replace(&mut self, frequent_evicted_contains_key: bool) -> Option<(K, V)> {
        if !self.recent_set.is_empty()
            && (self.recent_set.len() as isize > self.p
                || (self.recent_set.len() as isize == self.p && frequent_evicted_contains_key))
        {
            match self.recent_set.pop_lru() {
                Some((key, value)) => {
                    self.recent_evicted.push(key.clone(), ());
                    Some((key, value))
                }
                None => None,
            }
        } else if let Some((key, value)) = self.frequent_set.pop_lru() {
            self.frequent_evicted.push(key.clone(), ());
            Some((key, value))
        } else {
            None
        }
    }
}

impl<K: Hash + Eq, V> IntoIterator for ArcCache<K, V> {
    type Item = (K, V);
    type IntoIter = Chain<IntoIter<K, V>, IntoIter<K, V>>;

    fn into_iter(self) -> Self::IntoIter {
        self.recent_set.into_iter().chain(self.frequent_set)
    }
}

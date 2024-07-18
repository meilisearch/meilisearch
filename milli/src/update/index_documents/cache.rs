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

pub struct SorterCacheDelAddCboRoaringBitmap<const N: usize, MF> {
    cache: ArcCache<SmallVec<[u8; N]>, DelAddRoaringBitmap>,
    prefix: &'static [u8; 3],
    sorter: grenad::Sorter<MF>,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
    conn: redis::Connection,
}

impl<const N: usize, MF> SorterCacheDelAddCboRoaringBitmap<N, MF> {
    pub fn new(
        cap: NonZeroUsize,
        sorter: grenad::Sorter<MF>,
        prefix: &'static [u8; 3],
        conn: redis::Connection,
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
        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                del.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_del_u32(n);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
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
        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                *del.get_or_insert_with(RoaringBitmap::new) |= bitmap;
            }
            None => {
                let value = DelAddRoaringBitmap::new_del(bitmap);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
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
        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                add.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_add_u32(n);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
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
        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                *add.get_or_insert_with(RoaringBitmap::new) |= bitmap;
            }
            None => {
                let value = DelAddRoaringBitmap::new_add(bitmap);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
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
        let (cache, evicted) = self.cache.get_mut(key);
        match cache {
            Some(DelAddRoaringBitmap { del, add }) => {
                del.get_or_insert_with(RoaringBitmap::new).insert(n);
                add.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_del_add_u32(n);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry_to_sorter(key, deladd)?;
                }
            }
        }

        match evicted {
            Some((key, value)) => self.write_entry_to_sorter(key, value),
            None => Ok(()),
        }
    }

    fn write_entry_to_sorter(
        &mut self,
        key: SmallVec<[u8; N]>,
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
        self.cbo_buffer.extend_from_slice(&key);
        redis::cmd("INCR").arg(&self.cbo_buffer).query::<usize>(&mut self.conn).unwrap();
        self.sorter.insert(key, value_writer.into_inner().unwrap())
    }

    pub fn direct_insert(&mut self, key: &[u8], val: &[u8]) -> Result<(), grenad::Error<U>> {
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
    // recent_evicted: LruCache<K, ()>,
    frequent_set: LruCache<K, V>,
    // frequent_evicted: LruCache<K, ()>,
    // capacity: NonZeroUsize,
    // negative means shrinking recent and increasing frequent
    // positive means shrinking frequent and increasing recent
    // target: isize,
}

impl<K: Eq + Hash, V> ArcCache<K, V> {
    pub fn new(cap: NonZeroUsize) -> Self {
        ArcCache {
            recent_set: LruCache::new(cap),
            // recent_evicted: LruCache::new(cap),
            frequent_set: LruCache::new(cap),
            // frequent_evicted: LruCache::new(cap),
            // capacity: cap,
            // target: 0,
        }
    }
}

impl<K: Eq + Hash + Clone, V> ArcCache<K, V> {
    pub fn get_mut<'a, Q>(&'a mut self, k: &Q) -> (Option<&'a mut V>, Option<(K, V)>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // Rust is too dumb to let me get_mut directly...
        if self.frequent_set.contains(k) {
            return (self.frequent_set.get_mut(k), None);
        }

        if let Some((key, value)) = self.recent_set.pop_entry(k) {
            let evicted = self.frequent_set.push(key, value);
            let inserted = self.frequent_set.get_mut(k).unwrap();
            // if let Some((evicted_key, _)) = evicted.as_ref() {
            //     self.frequent_evicted.push(evicted_key.clone(), ());
            // }
            return (Some(inserted), evicted);
        }

        // TODO implement live resize of LRUs

        (None, None)
    }

    pub fn push(&mut self, k: K, v: V) -> Option<(K, V)> {
        self.frequent_set.push(k, v)
    }
}

impl<K: Hash + Eq, V> IntoIterator for ArcCache<K, V> {
    type Item = (K, V);
    type IntoIter = Chain<IntoIter<K, V>, IntoIter<K, V>>;

    fn into_iter(self) -> Self::IntoIter {
        self.recent_set.into_iter().chain(self.frequent_set)
    }
}

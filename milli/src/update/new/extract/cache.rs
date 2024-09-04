use std::mem;
use std::num::NonZeroUsize;

use grenad::{MergeFunction, Sorter};
use lru::LruCache;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::update::del_add::{DelAdd, KvWriterDelAdd};
use crate::CboRoaringBitmapCodec;

#[derive(Debug)]
pub struct CboCachedSorter<MF> {
    cache: lru::LruCache<SmallVec<[u8; 20]>, DelAddRoaringBitmap>,
    sorter: Sorter<MF>,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
}

impl<MF> CboCachedSorter<MF> {
    pub fn new(cap: NonZeroUsize, sorter: Sorter<MF>) -> Self {
        CboCachedSorter {
            cache: lru::LruCache::new(cap),
            sorter,
            deladd_buffer: Vec::new(),
            cbo_buffer: Vec::new(),
        }
    }
}

impl<MF: MergeFunction> CboCachedSorter<MF> {
    pub fn insert_del_u32(&mut self, key: &[u8], n: u32) -> grenad::Result<(), MF::Error> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                del.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_del_u32(n);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    pub fn insert_del(
        &mut self,
        key: &[u8],
        bitmap: RoaringBitmap,
    ) -> grenad::Result<(), MF::Error> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                *del.get_or_insert_with(RoaringBitmap::new) |= bitmap;
            }
            None => {
                let value = DelAddRoaringBitmap::new_del(bitmap);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    pub fn insert_add_u32(&mut self, key: &[u8], n: u32) -> grenad::Result<(), MF::Error> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                add.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_add_u32(n);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    pub fn insert_add(
        &mut self,
        key: &[u8],
        bitmap: RoaringBitmap,
    ) -> grenad::Result<(), MF::Error> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                *add.get_or_insert_with(RoaringBitmap::new) |= bitmap;
            }
            None => {
                let value = DelAddRoaringBitmap::new_add(bitmap);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    pub fn insert_del_add_u32(&mut self, key: &[u8], n: u32) -> grenad::Result<(), MF::Error> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add }) => {
                del.get_or_insert_with(RoaringBitmap::new).insert(n);
                add.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_del_add_u32(n);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    fn write_entry<A: AsRef<[u8]>>(
        &mut self,
        key: A,
        deladd: DelAddRoaringBitmap,
    ) -> grenad::Result<(), MF::Error> {
        /// TODO we must create a serialization trait to correctly serialize bitmaps
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
        let bytes = value_writer.into_inner().unwrap();
        self.sorter.insert(key, bytes)
    }

    pub fn direct_insert(&mut self, key: &[u8], val: &[u8]) -> grenad::Result<(), MF::Error> {
        self.sorter.insert(key, val)
    }

    pub fn into_sorter(mut self) -> grenad::Result<Sorter<MF>, MF::Error> {
        let default_arc = LruCache::new(NonZeroUsize::MIN);
        for (key, deladd) in mem::replace(&mut self.cache, default_arc) {
            self.write_entry(key, deladd)?;
        }
        Ok(self.sorter)
    }
}

#[derive(Debug, Clone)]
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

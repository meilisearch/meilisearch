use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::{io, mem};

use grenad2::{MergeFunction, Sorter};
use lru::LruCache;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};

#[derive(Debug)]
pub struct CachedSorter<MF> {
    cache: lru::LruCache<SmallVec<[u8; 20]>, DelAddRoaringBitmap>,
    sorter: Sorter<MF>,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
}

impl<MF> CachedSorter<MF> {
    pub fn new(cap: NonZeroUsize, sorter: Sorter<MF>) -> Self {
        CachedSorter {
            cache: lru::LruCache::new(cap),
            sorter,
            deladd_buffer: Vec::new(),
            cbo_buffer: Vec::new(),
        }
    }
}

impl<MF: MergeFunction> CachedSorter<MF> {
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
        self.deladd_buffer.clear();
        let mut value_writer = KvWriterDelAdd::new(&mut self.deladd_buffer);
        match deladd {
            DelAddRoaringBitmap { del: Some(del), add: None } => {
                self.cbo_buffer.clear();
                RoaringBitmap::serialize_into(&del, &mut self.cbo_buffer)?;
                value_writer.insert(DelAdd::Deletion, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: None, add: Some(add) } => {
                self.cbo_buffer.clear();
                RoaringBitmap::serialize_into(&add, &mut self.cbo_buffer)?;
                value_writer.insert(DelAdd::Addition, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: Some(del), add: Some(add) } => {
                self.cbo_buffer.clear();
                RoaringBitmap::serialize_into(&del, &mut self.cbo_buffer)?;
                value_writer.insert(DelAdd::Deletion, &self.cbo_buffer)?;

                self.cbo_buffer.clear();
                RoaringBitmap::serialize_into(&add, &mut self.cbo_buffer)?;
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

/// Do a union of CboRoaringBitmaps on both sides of a DelAdd obkv
/// separately and outputs a new DelAdd with both unions.
pub struct DelAddRoaringBitmapMerger;

impl MergeFunction for DelAddRoaringBitmapMerger {
    type Error = io::Error;

    fn merge<'a>(
        &self,
        _key: &[u8],
        values: &[Cow<'a, [u8]>],
    ) -> std::result::Result<Cow<'a, [u8]>, Self::Error> {
        if values.len() == 1 {
            Ok(values[0].clone())
        } else {
            // Retrieve the bitmaps from both sides
            let mut del_bitmaps_bytes = Vec::new();
            let mut add_bitmaps_bytes = Vec::new();
            for value in values {
                let obkv: &KvReaderDelAdd = value.as_ref().into();
                if let Some(bitmap_bytes) = obkv.get(DelAdd::Deletion) {
                    del_bitmaps_bytes.push(bitmap_bytes);
                }
                if let Some(bitmap_bytes) = obkv.get(DelAdd::Addition) {
                    add_bitmaps_bytes.push(bitmap_bytes);
                }
            }

            let mut output_deladd_obkv = KvWriterDelAdd::memory();

            // Deletion
            let mut buffer = Vec::new();
            let mut merged = RoaringBitmap::new();
            for bytes in del_bitmaps_bytes {
                merged |= RoaringBitmap::deserialize_unchecked_from(bytes)?;
            }
            merged.serialize_into(&mut buffer)?;
            output_deladd_obkv.insert(DelAdd::Deletion, &buffer)?;

            // Addition
            buffer.clear();
            merged.clear();
            for bytes in add_bitmaps_bytes {
                merged |= RoaringBitmap::deserialize_unchecked_from(bytes)?;
            }
            output_deladd_obkv.insert(DelAdd::Addition, &buffer)?;

            output_deladd_obkv.into_inner().map(Cow::from).map_err(Into::into)
        }
    }
}

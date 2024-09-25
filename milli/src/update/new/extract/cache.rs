use std::mem;
use std::num::NonZeroUsize;

use grenad::{MergeFunction, Sorter};
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use super::lru::Lru;
use crate::update::del_add::{DelAdd, KvWriterDelAdd};
use crate::CboRoaringBitmapCodec;

const KEY_SIZE: usize = 12;

#[derive(Debug)]
pub struct CboCachedSorter<MF> {
    cache: Lru<SmallVec<[u8; KEY_SIZE]>, DelAddRoaringBitmap>,
    sorter: Sorter<MF>,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
    total_insertions: usize,
    fitted_in_key: usize,
}

impl<MF> CboCachedSorter<MF> {
    pub fn new(cap: NonZeroUsize, sorter: Sorter<MF>) -> Self {
        CboCachedSorter {
            cache: Lru::new(cap),
            sorter,
            deladd_buffer: Vec::new(),
            cbo_buffer: Vec::new(),
            total_insertions: 0,
            fitted_in_key: 0,
        }
    }
}

impl<MF: MergeFunction> CboCachedSorter<MF> {
    pub fn insert_del_u32(&mut self, key: &[u8], n: u32) -> grenad::Result<(), MF::Error> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                del.get_or_insert_with(PushOptimizedBitmap::default).insert(n);
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
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
                del.get_or_insert_with(PushOptimizedBitmap::default).union_with_bitmap(bitmap);
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
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
                add.get_or_insert_with(PushOptimizedBitmap::default).insert(n);
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
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
                add.get_or_insert_with(PushOptimizedBitmap::default).union_with_bitmap(bitmap);
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
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
                del.get_or_insert_with(PushOptimizedBitmap::default).insert(n);
                add.get_or_insert_with(PushOptimizedBitmap::default).insert(n);
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
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
                CboRoaringBitmapCodec::serialize_into(&del.bitmap, &mut self.cbo_buffer);
                value_writer.insert(DelAdd::Deletion, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: None, add: Some(add) } => {
                self.cbo_buffer.clear();
                CboRoaringBitmapCodec::serialize_into(&add.bitmap, &mut self.cbo_buffer);
                value_writer.insert(DelAdd::Addition, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: Some(del), add: Some(add) } => {
                self.cbo_buffer.clear();
                CboRoaringBitmapCodec::serialize_into(&del.bitmap, &mut self.cbo_buffer);
                value_writer.insert(DelAdd::Deletion, &self.cbo_buffer)?;

                self.cbo_buffer.clear();
                CboRoaringBitmapCodec::serialize_into(&add.bitmap, &mut self.cbo_buffer);
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
        let default_arc = Lru::new(NonZeroUsize::MIN);
        for (key, deladd) in mem::replace(&mut self.cache, default_arc) {
            self.write_entry(key, deladd)?;
        }

        eprintln!(
            "LruCache stats: {} <= {KEY_SIZE} bytes ({}%) on a total of {} insertions",
            self.fitted_in_key,
            (self.fitted_in_key as f32 / self.total_insertions as f32) * 100.0,
            self.total_insertions,
        );

        Ok(self.sorter)
    }
}

#[derive(Debug, Clone)]
pub struct DelAddRoaringBitmap {
    pub(crate) del: Option<PushOptimizedBitmap>,
    pub(crate) add: Option<PushOptimizedBitmap>,
}

impl DelAddRoaringBitmap {
    fn new_del_add_u32(n: u32) -> Self {
        DelAddRoaringBitmap {
            del: Some(PushOptimizedBitmap::from_single(n)),
            add: Some(PushOptimizedBitmap::from_single(n)),
        }
    }

    fn new_del(bitmap: RoaringBitmap) -> Self {
        DelAddRoaringBitmap { del: Some(PushOptimizedBitmap::from_bitmap(bitmap)), add: None }
    }

    fn new_del_u32(n: u32) -> Self {
        DelAddRoaringBitmap { del: Some(PushOptimizedBitmap::from_single(n)), add: None }
    }

    fn new_add(bitmap: RoaringBitmap) -> Self {
        DelAddRoaringBitmap { del: None, add: Some(PushOptimizedBitmap::from_bitmap(bitmap)) }
    }

    fn new_add_u32(n: u32) -> Self {
        DelAddRoaringBitmap { del: None, add: Some(PushOptimizedBitmap::from_single(n)) }
    }
}

#[derive(Debug, Clone, Default)]
struct PushOptimizedBitmap {
    bitmap: RoaringBitmap,
}

impl PushOptimizedBitmap {
    #[inline]
    fn from_bitmap(bitmap: RoaringBitmap) -> PushOptimizedBitmap {
        PushOptimizedBitmap { bitmap }
    }

    #[inline]
    fn from_single(single: u32) -> PushOptimizedBitmap {
        PushOptimizedBitmap { bitmap: RoaringBitmap::from([single]) }
    }

    #[inline]
    fn insert(&mut self, n: u32) {
        if !self.bitmap.push(n) {
            self.bitmap.insert(n);
        }
    }

    #[inline]
    fn union_with_bitmap(&mut self, bitmap: RoaringBitmap) {
        self.bitmap |= bitmap;
    }
}

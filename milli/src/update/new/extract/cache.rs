use std::collections::HashMap;

use roaring::RoaringBitmap;
use smallvec::SmallVec;

pub const KEY_SIZE: usize = 12;

#[derive(Debug)]
pub struct CboCachedSorter {
    cache: HashMap<SmallVec<[u8; KEY_SIZE]>, DelAddRoaringBitmap>,
    total_insertions: usize,
    fitted_in_key: usize,
}

impl CboCachedSorter {
    pub fn new() -> Self {
        CboCachedSorter { cache: HashMap::new(), total_insertions: 0, fitted_in_key: 0 }
    }
}

impl CboCachedSorter {
    pub fn insert_del_u32(&mut self, key: &[u8], n: u32) {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                del.get_or_insert_with(RoaringBitmap::default).insert(n);
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let value = DelAddRoaringBitmap::new_del_u32(n);
                assert!(self.cache.insert(key.into(), value).is_none());
            }
        }
    }

    pub fn insert_del(&mut self, key: &[u8], bitmap: RoaringBitmap) {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                *del.get_or_insert_with(RoaringBitmap::default) |= bitmap;
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let value = DelAddRoaringBitmap::new_del(bitmap);
                assert!(self.cache.insert(key.into(), value).is_none());
            }
        }
    }

    pub fn insert_add_u32(&mut self, key: &[u8], n: u32) {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                add.get_or_insert_with(RoaringBitmap::default).insert(n);
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let value = DelAddRoaringBitmap::new_add_u32(n);
                assert!(self.cache.insert(key.into(), value).is_none());
            }
        }
    }

    pub fn insert_add(&mut self, key: &[u8], bitmap: RoaringBitmap) {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                *add.get_or_insert_with(RoaringBitmap::default) |= bitmap;
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let value = DelAddRoaringBitmap::new_add(bitmap);
                assert!(self.cache.insert(key.into(), value).is_none());
            }
        }
    }

    pub fn insert_del_add_u32(&mut self, key: &[u8], n: u32) {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add }) => {
                del.get_or_insert_with(RoaringBitmap::default).insert(n);
                add.get_or_insert_with(RoaringBitmap::default).insert(n);
            }
            None => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let value = DelAddRoaringBitmap::new_del_add_u32(n);
                assert!(self.cache.insert(key.into(), value).is_none());
            }
        }
    }

    pub fn into_sorter(self) -> HashMap<SmallVec<[u8; KEY_SIZE]>, DelAddRoaringBitmap> {
        eprintln!(
            "LruCache stats: {} <= {KEY_SIZE} bytes ({}%) on a total of {} insertions",
            self.fitted_in_key,
            (self.fitted_in_key as f32 / self.total_insertions as f32) * 100.0,
            self.total_insertions,
        );

        self.cache
    }
}

#[derive(Debug, Clone, Default)]
pub struct DelAddRoaringBitmap {
    pub(crate) del: Option<RoaringBitmap>,
    pub(crate) add: Option<RoaringBitmap>,
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

    pub fn merge_with(&mut self, other: &DelAddRoaringBitmap) {
        self.del = match (&self.del, &other.del) {
            (None, None) => None,
            (None, Some(other)) => Some(other.clone()),
            (Some(this), None) => Some(this.clone()),
            (Some(this), Some(other)) => Some(this | other),
        };
        self.add = match (&self.add, &other.add) {
            (None, None) => None,
            (None, Some(other)) => Some(other.clone()),
            (Some(this), None) => Some(this.clone()),
            (Some(this), Some(other)) => Some(this | other),
        };
    }
}

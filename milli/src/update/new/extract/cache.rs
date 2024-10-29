use std::fmt::Write as _;
use std::mem;
use std::num::NonZeroUsize;

use grenad::{MergeFunction, Sorter};
use roaring::bitmap::Statistics;
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
                del.get_or_insert_with(RoaringBitmap::default).insert(n);
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
                *del.get_or_insert_with(RoaringBitmap::default) |= bitmap;
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
                add.get_or_insert_with(RoaringBitmap::default).insert(n);
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
                *add.get_or_insert_with(RoaringBitmap::default) |= bitmap;
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
                del.get_or_insert_with(RoaringBitmap::default).insert(n);
                add.get_or_insert_with(RoaringBitmap::default).insert(n);
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
        let mut all_n_containers = Vec::new();
        let mut all_n_array_containers = Vec::new();
        let mut all_n_bitset_containers = Vec::new();
        let mut all_n_values_array_containers = Vec::new();
        let mut all_n_values_bitset_containers = Vec::new();
        let mut all_cardinality = Vec::new();

        let default_arc = Lru::new(NonZeroUsize::MIN);
        for (key, deladd) in mem::replace(&mut self.cache, default_arc) {
            for bitmap in [&deladd.del, &deladd.add].into_iter().flatten() {
                let Statistics {
                    n_containers,
                    n_array_containers,
                    n_bitset_containers,
                    n_values_array_containers,
                    n_values_bitset_containers,
                    cardinality,
                    ..
                } = bitmap.statistics();
                all_n_containers.push(n_containers);
                all_n_array_containers.push(n_array_containers);
                all_n_bitset_containers.push(n_bitset_containers);
                all_n_values_array_containers.push(n_values_array_containers);
                all_n_values_bitset_containers.push(n_values_bitset_containers);
                all_cardinality.push(cardinality as u32);
            }

            self.write_entry(key, deladd)?;
        }

        let mut output = String::new();

        for (name, mut slice) in [
            ("n_containers", all_n_containers),
            ("n_array_containers", all_n_array_containers),
            ("n_bitset_containers", all_n_bitset_containers),
            ("n_values_array_containers", all_n_values_array_containers),
            ("n_values_bitset_containers", all_n_values_bitset_containers),
            ("cardinality", all_cardinality),
        ] {
            let _ = writeln!(&mut output, "{name} (p100) {:?}", Stats::from_slice(&mut slice));
            // let _ = writeln!(&mut output, "{name} (p99)  {:?}", Stats::from_slice_p99(&mut slice));
        }

        let _ = writeln!(
            &mut output,
            "LruCache stats: {} <= {KEY_SIZE} bytes ({}%) on a total of {} insertions",
            self.fitted_in_key,
            (self.fitted_in_key as f32 / self.total_insertions as f32) * 100.0,
            self.total_insertions,
        );

        eprintln!("{output}");

        Ok(self.sorter)
    }
}

#[derive(Default, Debug)]
struct Stats {
    pub len: usize,
    pub average: f32,
    pub mean: u32,
    pub min: u32,
    pub max: u32,
}

impl Stats {
    fn from_slice(slice: &mut [u32]) -> Stats {
        slice.sort_unstable();
        Self::from_sorted_slice(slice)
    }

    fn from_slice_p99(slice: &mut [u32]) -> Stats {
        slice.sort_unstable();
        let new_len = slice.len() - (slice.len() as f32 / 100.0) as usize;
        match slice.get(..new_len) {
            Some(slice) => Self::from_sorted_slice(slice),
            None => Stats::default(),
        }
    }

    fn from_sorted_slice(slice: &[u32]) -> Stats {
        let sum: f64 = slice.iter().map(|i| *i as f64).sum();
        let average = (sum / slice.len() as f64) as f32;
        let mean = *slice.len().checked_div(2).and_then(|middle| slice.get(middle)).unwrap_or(&0);
        let min = *slice.first().unwrap_or(&0);
        let max = *slice.last().unwrap_or(&0);
        Stats { len: slice.len(), average, mean, min, max }
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
}

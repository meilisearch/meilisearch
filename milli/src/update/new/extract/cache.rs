use std::cell::RefCell;
use std::fmt::Write as _;

use bumpalo::Bump;
use grenad::{MergeFunction, Sorter};
use hashbrown::hash_map::RawEntryMut;
use raw_collections::alloc::{RefBump, RefBytes};
use roaring::bitmap::Statistics;
use roaring::RoaringBitmap;

use crate::update::del_add::{DelAdd, KvWriterDelAdd};
use crate::CboRoaringBitmapCodec;

const KEY_SIZE: usize = 12;

// #[derive(Debug)]
pub struct CboCachedSorter<'extractor, MF> {
    cache: hashbrown::HashMap<
        // TODO check the size of it
        RefBytes<'extractor>,
        DelAddRoaringBitmap,
        hashbrown::DefaultHashBuilder,
        RefBump<'extractor>,
    >,
    alloc: RefBump<'extractor>,
    sorter: Sorter<MF>,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
    total_insertions: usize,
    fitted_in_key: usize,
}

impl<'extractor, MF> CboCachedSorter<'extractor, MF> {
    /// TODO may add the capacity
    pub fn new_in(sorter: Sorter<MF>, alloc: RefBump<'extractor>) -> Self {
        CboCachedSorter {
            cache: hashbrown::HashMap::new_in(RefBump::clone(&alloc)),
            alloc,
            sorter,
            deladd_buffer: Vec::new(),
            cbo_buffer: Vec::new(),
            total_insertions: 0,
            fitted_in_key: 0,
        }
    }
}

impl<'extractor, MF: MergeFunction> CboCachedSorter<'extractor, MF> {
    pub fn insert_del_u32(&mut self, key: &[u8], n: u32) {
        match self.cache.raw_entry_mut().from_key(key) {
            RawEntryMut::Occupied(mut entry) => {
                let DelAddRoaringBitmap { del, add: _ } = entry.get_mut();
                del.get_or_insert_with(RoaringBitmap::default).insert(n);
            }
            RawEntryMut::Vacant(entry) => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let alloc = RefBump::clone(&self.alloc);
                let key = RefBump::map(alloc, |b| b.alloc_slice_copy(key));
                entry.insert(RefBytes(key), DelAddRoaringBitmap::new_del_u32(n));
            }
        }
    }

    pub fn insert_del(&mut self, key: &[u8], bitmap: RoaringBitmap) {
        match self.cache.raw_entry_mut().from_key(key) {
            RawEntryMut::Occupied(mut entry) => {
                let DelAddRoaringBitmap { del, add: _ } = entry.get_mut();
                *del.get_or_insert_with(RoaringBitmap::default) |= bitmap;
            }
            RawEntryMut::Vacant(entry) => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let alloc = RefBump::clone(&self.alloc);
                let key = RefBump::map(alloc, |b| b.alloc_slice_copy(key));
                entry.insert(RefBytes(key), DelAddRoaringBitmap::new_del(bitmap));
            }
        }
    }

    pub fn insert_add_u32(&mut self, key: &[u8], n: u32) {
        match self.cache.raw_entry_mut().from_key(key) {
            RawEntryMut::Occupied(mut entry) => {
                let DelAddRoaringBitmap { del: _, add } = entry.get_mut();
                add.get_or_insert_with(RoaringBitmap::default).insert(n);
            }
            RawEntryMut::Vacant(entry) => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let alloc = RefBump::clone(&self.alloc);
                let key = RefBump::map(alloc, |b| b.alloc_slice_copy(key));
                entry.insert(RefBytes(key), DelAddRoaringBitmap::new_add_u32(n));
            }
        }
    }

    pub fn insert_add(&mut self, key: &[u8], bitmap: RoaringBitmap) {
        match self.cache.raw_entry_mut().from_key(key) {
            RawEntryMut::Occupied(mut entry) => {
                let DelAddRoaringBitmap { del: _, add } = entry.get_mut();
                *add.get_or_insert_with(RoaringBitmap::default) |= bitmap;
            }
            RawEntryMut::Vacant(entry) => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let alloc = RefBump::clone(&self.alloc);
                let key = RefBump::map(alloc, |b| b.alloc_slice_copy(key));
                entry.insert(RefBytes(key), DelAddRoaringBitmap::new_add(bitmap));
            }
        }
    }

    pub fn insert_del_add_u32(&mut self, key: &[u8], n: u32) {
        match self.cache.raw_entry_mut().from_key(key) {
            RawEntryMut::Occupied(mut entry) => {
                let DelAddRoaringBitmap { del, add } = entry.get_mut();
                del.get_or_insert_with(RoaringBitmap::default).insert(n);
                add.get_or_insert_with(RoaringBitmap::default).insert(n);
            }
            RawEntryMut::Vacant(entry) => {
                self.total_insertions += 1;
                self.fitted_in_key += (key.len() <= KEY_SIZE) as usize;
                let alloc = RefBump::clone(&self.alloc);
                let key = RefBump::map(alloc, |b| b.alloc_slice_copy(key));
                entry.insert(RefBytes(key), DelAddRoaringBitmap::new_del_add_u32(n));
            }
        }
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

    pub fn spill_to_disk(self) -> std::io::Result<SpilledCache<MF>> {
        let Self {
            cache,
            alloc: _,
            sorter,
            deladd_buffer,
            cbo_buffer,
            total_insertions,
            fitted_in_key,
        } = self;

        /// I want to spill to disk for real
        drop(cache);

        Ok(SpilledCache { sorter, deladd_buffer, cbo_buffer, total_insertions, fitted_in_key })
    }

    pub fn into_sorter(self) -> grenad::Result<Sorter<MF>, MF::Error> {
        let Self { cache, sorter, total_insertions, fitted_in_key, .. } = self;

        let mut all_n_containers = Vec::new();
        let mut all_n_array_containers = Vec::new();
        let mut all_n_bitset_containers = Vec::new();
        let mut all_n_values_array_containers = Vec::new();
        let mut all_n_values_bitset_containers = Vec::new();
        let mut all_cardinality = Vec::new();

        for (_key, deladd) in &cache {
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
        }

        for (key, deladd) in cache {
            // self.write_entry(key, deladd)?;
            todo!("spill into the sorter")
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
            fitted_in_key,
            (fitted_in_key as f32 / total_insertions as f32) * 100.0,
            total_insertions,
        );

        eprintln!("{output}");

        Ok(sorter)
    }
}

pub struct SpilledCache<MF> {
    sorter: Sorter<MF>,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
    total_insertions: usize,
    fitted_in_key: usize,
}

impl<MF> SpilledCache<MF> {
    pub fn reconstruct<'extractor>(
        self,
        alloc: RefBump<'extractor>,
    ) -> CboCachedSorter<'extractor, MF> {
        let SpilledCache { sorter, deladd_buffer, cbo_buffer, total_insertions, fitted_in_key } =
            self;

        CboCachedSorter {
            cache: hashbrown::HashMap::new_in(RefBump::clone(&alloc)),
            alloc,
            sorter,
            deladd_buffer,
            cbo_buffer,
            total_insertions,
            fitted_in_key,
        }
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

#[derive(Debug, Clone)]
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

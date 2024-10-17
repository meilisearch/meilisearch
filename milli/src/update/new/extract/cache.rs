use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read as _, Seek, Write as _};
use std::vec;

use hashbrown::hash_map::RawEntryMut;
use raw_collections::alloc::{RefBump, RefBytes};
use roaring::RoaringBitmap;
use tempfile::tempfile;

use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::new::indexer::document_changes::MostlySend;
use crate::CboRoaringBitmapCodec;

const KEY_SIZE: usize = 12;

// #[derive(Debug)]
pub struct CboCachedSorter<'extractor> {
    cache: hashbrown::HashMap<
        RefBytes<'extractor>,
        DelAddRoaringBitmap,
        hashbrown::DefaultHashBuilder,
        RefBump<'extractor>,
    >,
    alloc: RefBump<'extractor>,
    spilled_entries: UnorderedEntries,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
}

// # How the Merge Algorithm works
//
//  - Collect all hashmaps to the main thread
//  - Iterator over all the hashmaps in the different threads
//    - Each thread must take care of its own keys (regarding a hash number)
//    - Also read the spilled content which are inside
//  - Each thread must populate a local hashmap with the entries
//  - Every thread send the merged content to the main writing thread
//
// ## Next Step
//
//  - Define the size of the buckets in advance to make sure everything fits in memory.
// ```
// let total_buckets = 32;
// (0..total_buckets).par_iter().for_each(|n| {
//   let hash = todo!();
//   if hash % total_bucket == n {
//     // take care of this key
//   }
// });
// ```

impl<'extractor> CboCachedSorter<'extractor> {
    /// TODO may add the capacity
    pub fn new_in(alloc: RefBump<'extractor>) -> io::Result<Self> {
        Ok(CboCachedSorter {
            cache: hashbrown::HashMap::new_in(RefBump::clone(&alloc)),
            alloc,
            spilled_entries: tempfile().map(UnorderedEntries::new)?,
            deladd_buffer: Vec::new(),
            cbo_buffer: Vec::new(),
        })
    }
}

impl<'extractor> CboCachedSorter<'extractor> {
    pub fn insert_del_u32(&mut self, key: &[u8], n: u32) {
        match self.cache.raw_entry_mut().from_key(key) {
            RawEntryMut::Occupied(mut entry) => {
                let DelAddRoaringBitmap { del, add: _ } = entry.get_mut();
                del.get_or_insert_with(RoaringBitmap::default).insert(n);
            }
            RawEntryMut::Vacant(entry) => {
                let alloc = RefBump::clone(&self.alloc);
                let key = RefBump::map(alloc, |b| b.alloc_slice_copy(key));
                entry.insert(RefBytes(key), DelAddRoaringBitmap::new_del_u32(n));
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
                let alloc = RefBump::clone(&self.alloc);
                let key = RefBump::map(alloc, |b| b.alloc_slice_copy(key));
                entry.insert(RefBytes(key), DelAddRoaringBitmap::new_add_u32(n));
            }
        }
    }

    pub fn spill_to_disk(self) -> io::Result<SpilledCache> {
        let Self { cache, alloc: _, mut spilled_entries, mut deladd_buffer, mut cbo_buffer } = self;

        for (key, deladd) in cache {
            spill_entry_to_disk(
                &mut spilled_entries,
                &mut deladd_buffer,
                &mut cbo_buffer,
                &key,
                deladd,
            )?;
        }

        Ok(SpilledCache { spilled_entries, deladd_buffer, cbo_buffer })
    }

    // TODO Do not spill to disk if not necessary
    pub fn into_unordered_entries(self) -> io::Result<UnorderedEntriesIntoIter> {
        let Self { cache, alloc: _, mut spilled_entries, mut cbo_buffer, mut deladd_buffer } = self;

        for (key, deladd) in cache {
            spill_entry_to_disk(
                &mut spilled_entries,
                &mut deladd_buffer,
                &mut cbo_buffer,
                &key,
                deladd,
            )?;
        }

        spilled_entries.into_iter_bitmap()
    }
}

fn spill_entry_to_disk(
    spilled_entries: &mut UnorderedEntries,
    deladd_buffer: &mut Vec<u8>,
    cbo_buffer: &mut Vec<u8>,
    key: &[u8],
    deladd: DelAddRoaringBitmap,
) -> io::Result<()> {
    deladd_buffer.clear();
    let mut value_writer = KvWriterDelAdd::new(deladd_buffer);
    match deladd {
        DelAddRoaringBitmap { del: Some(del), add: None } => {
            cbo_buffer.clear();
            CboRoaringBitmapCodec::serialize_into(&del, cbo_buffer);
            value_writer.insert(DelAdd::Deletion, &cbo_buffer)?;
        }
        DelAddRoaringBitmap { del: None, add: Some(add) } => {
            cbo_buffer.clear();
            CboRoaringBitmapCodec::serialize_into(&add, cbo_buffer);
            value_writer.insert(DelAdd::Addition, &cbo_buffer)?;
        }
        DelAddRoaringBitmap { del: Some(del), add: Some(add) } => {
            cbo_buffer.clear();
            CboRoaringBitmapCodec::serialize_into(&del, cbo_buffer);
            value_writer.insert(DelAdd::Deletion, &cbo_buffer)?;

            cbo_buffer.clear();
            CboRoaringBitmapCodec::serialize_into(&add, cbo_buffer);
            value_writer.insert(DelAdd::Addition, &cbo_buffer)?;
        }
        DelAddRoaringBitmap { del: None, add: None } => return Ok(()),
    }
    let bytes = value_writer.into_inner().unwrap();
    spilled_entries.push(key, bytes)
}

pub struct SpilledCache {
    spilled_entries: UnorderedEntries,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
}

impl SpilledCache {
    pub fn reconstruct(self, alloc: RefBump<'_>) -> CboCachedSorter<'_> {
        let SpilledCache { spilled_entries, deladd_buffer, cbo_buffer } = self;
        CboCachedSorter {
            cache: hashbrown::HashMap::new_in(RefBump::clone(&alloc)),
            alloc,
            spilled_entries,
            deladd_buffer,
            cbo_buffer,
        }
    }
}

unsafe impl<'extractor> MostlySend for CboCachedSorter<'extractor> {}

pub struct UnorderedEntries {
    entry_sizes: Vec<(u32, u32)>,
    file: BufWriter<File>,
}

impl UnorderedEntries {
    fn new(file: File) -> Self {
        UnorderedEntries { entry_sizes: Vec::new(), file: BufWriter::new(file) }
    }

    /// Pushes a new tuple of key/value into a file.
    ///
    /// If the function fails you must not continue to use this struct and rather drop it.
    ///
    /// # Panics
    ///
    /// - Panics if the key or value length is larger than 2^32 bytes.
    fn push(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let key_len = key.len().try_into().unwrap();
        let value_len = value.len().try_into().unwrap();

        self.file.write_all(key)?;
        self.file.write_all(value)?;

        self.entry_sizes.push((key_len, value_len));

        Ok(())
    }

    fn into_iter_bitmap(self) -> io::Result<UnorderedEntriesIntoIter> {
        let Self { entry_sizes, file } = self;

        let mut file = file.into_inner().map_err(|e| e.into_error())?;
        file.rewind()?;

        Ok(UnorderedEntriesIntoIter {
            entry_sizes: entry_sizes.into_iter(),
            file: BufReader::new(file),
            buffer: Vec::new(),
        })
    }
}

pub struct UnorderedEntriesIntoIter {
    entry_sizes: vec::IntoIter<(u32, u32)>,
    file: BufReader<File>,
    buffer: Vec<u8>,
}

impl UnorderedEntriesIntoIter {
    fn next_ref(&mut self) -> io::Result<Option<(&[u8], &[u8])>> {
        match self.entry_sizes.next() {
            Some((key_len, value_len)) => {
                let key_len = key_len as usize;
                let value_len = value_len as usize;
                let total_len = key_len + value_len;

                self.buffer.resize(total_len, 0);
                let buffer = &mut self.buffer[..total_len];

                self.file.read_exact(buffer)?;
                let buffer = &self.buffer[..total_len];

                Ok(Some(buffer.split_at(key_len)))
            }
            None => Ok(None),
        }
    }

    pub fn next_deladd_bitmap(&mut self) -> io::Result<Option<(&[u8], DelAddRoaringBitmap)>> {
        match self.next_ref()? {
            Some((key, value_bytes)) => {
                let reader = KvReaderDelAdd::from_slice(value_bytes);
                let del = match reader.get(DelAdd::Deletion) {
                    Some(del_bytes) => Some(CboRoaringBitmapCodec::deserialize_from(del_bytes)?),
                    None => None,
                };
                let add = match reader.get(DelAdd::Addition) {
                    Some(add_bytes) => Some(CboRoaringBitmapCodec::deserialize_from(add_bytes)?),
                    None => None,
                };
                Ok(Some((key, DelAddRoaringBitmap { del, add })))
            }
            None => Ok(None),
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

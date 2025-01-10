//! # How the Merge Algorithm works
//!
//! Each extractor create #Threads caches and balances the entries
//! based on the hash of the keys. To do that we can use the
//! hashbrown::hash_map::RawEntryBuilderMut::from_key_hashed_nocheck.
//! This way we can compute the hash on our own, decide on the cache to
//! target, and insert it into the right HashMap.
//!
//! #Thread  -> caches
//! t1       -> [t1c1, t1c2, t1c3]
//! t2       -> [t2c1, t2c2, t2c3]
//! t3       -> [t3c1, t3c2, t3c3]
//!
//! When the extractors are done filling the caches, we want to merge
//! the content of all the caches. We do a transpose and each thread is
//! assigned the associated cache. By doing that we know that every key
//! is put in a known cache and will collide with keys in the other
//! caches of the other threads.
//!
//! #Thread  -> caches
//! t1       -> [t1c1, t2c1, t3c1]
//! t2       -> [t1c2, t2c2, t3c2]
//! t3       -> [t1c3, t2c3, t3c3]
//!
//! When we encountered a miss in the other caches we must still try
//! to find it in the spilled entries. This is the reason why we use
//! a grenad sorter/reader so that we can seek "efficiently" for a key.
//!
//! ## More Detailled Algorithm
//!
//! Each sub-cache has an in-memory HashMap and some spilled
//! lexicographically ordered entries on disk (grenad). We first iterate
//! over the spilled entries of all the caches at once by using a merge
//! join algorithm. This algorithm will merge the entries by using its
//! merge function.
//!
//! Everytime a merged entry is emited by the merge join algorithm we also
//! fetch the value from the other in-memory caches (HashMaps) to finish
//! the merge. Everytime we retrieve an entry from the in-memory caches
//! we mark them with a tombstone for later.
//!
//! Once we are done with the spilled entries we iterate over the in-memory
//! HashMaps. We iterate over the first one, retrieve the content from the
//! other onces and mark them with a tombstone again. We also make sure
//! to ignore the dead (tombstoned) ones.
//!
//! ## Memory Control
//!
//! We can detect that there are no more memory available when the
//! bump allocator reaches a threshold. When this is the case we
//! freeze the cache. There is one bump allocator by thread and the
//! memory must be well balanced as we manage one type of extraction
//! at a time with well-balanced documents.
//!
//! It means that the unknown new keys added to the
//! cache are directly spilled to disk: basically a key followed by a
//! del/add bitmap. For the known keys we can keep modifying them in
//! the materialized version in the cache: update the del/add bitmaps.
//!
//! For now we can use a grenad sorter for spilling even thought I think
//! it's not the most efficient way (too many files open, sorting entries).

use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::fs::File;
use std::hash::BuildHasher;
use std::io::BufReader;
use std::{io, iter, mem};

use bumpalo::Bump;
use bumparaw_collections::bbbul::{BitPacker, BitPacker4x};
use bumparaw_collections::map::FrozenMap;
use bumparaw_collections::{Bbbul, FrozenBbbul};
use grenad::ReaderCursor;
use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;
use roaring::RoaringBitmap;
use rustc_hash::FxBuildHasher;

use crate::update::del_add::{DelAdd, KvWriterDelAdd};
use crate::update::new::thread_local::MostlySend;
use crate::update::new::KvReaderDelAdd;
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{CboRoaringBitmapCodec, Result};

/// A cache that stores bytes keys associated to CboDelAddRoaringBitmaps.
///
/// Internally balances the content over `N` buckets for future merging.
pub struct BalancedCaches<'extractor> {
    hasher: FxBuildHasher,
    alloc: &'extractor Bump,
    max_memory: Option<usize>,
    caches: InnerCaches<'extractor>,
}

enum InnerCaches<'extractor> {
    Normal(NormalCaches<'extractor>),
    Spilling(SpillingCaches<'extractor>),
}

impl<'extractor> BalancedCaches<'extractor> {
    pub fn new_in(buckets: usize, max_memory: Option<usize>, alloc: &'extractor Bump) -> Self {
        Self {
            hasher: FxBuildHasher,
            max_memory,
            caches: InnerCaches::Normal(NormalCaches {
                caches: iter::repeat_with(|| HashMap::with_hasher_in(FxBuildHasher, alloc))
                    .take(buckets)
                    .collect(),
            }),
            alloc,
        }
    }

    fn buckets(&self) -> usize {
        match &self.caches {
            InnerCaches::Normal(caches) => caches.caches.len(),
            InnerCaches::Spilling(caches) => caches.caches.len(),
        }
    }

    pub fn insert_del_u32(&mut self, key: &[u8], n: u32) -> Result<()> {
        if self.max_memory.map_or(false, |mm| self.alloc.allocated_bytes() >= mm) {
            self.start_spilling()?;
        }

        let buckets = self.buckets();
        match &mut self.caches {
            InnerCaches::Normal(normal) => {
                normal.insert_del_u32(&self.hasher, self.alloc, buckets, key, n);
                Ok(())
            }
            InnerCaches::Spilling(spilling) => {
                spilling.insert_del_u32(&self.hasher, self.alloc, buckets, key, n)
            }
        }
    }

    pub fn insert_add_u32(&mut self, key: &[u8], n: u32) -> Result<()> {
        if self.max_memory.map_or(false, |mm| self.alloc.allocated_bytes() >= mm) {
            self.start_spilling()?;
        }

        let buckets = self.buckets();
        match &mut self.caches {
            InnerCaches::Normal(normal) => {
                normal.insert_add_u32(&self.hasher, self.alloc, buckets, key, n);
                Ok(())
            }
            InnerCaches::Spilling(spilling) => {
                spilling.insert_add_u32(&self.hasher, self.alloc, buckets, key, n)
            }
        }
    }

    /// Make sure the cache is no longer allocating data
    /// and writes every new and unknow entry to disk.
    fn start_spilling(&mut self) -> Result<()> {
        let BalancedCaches { hasher: _, alloc, max_memory: _, caches } = self;

        if let InnerCaches::Normal(normal_caches) = caches {
            tracing::trace!(
                "We are spilling after we allocated {} bytes on thread #{}",
                alloc.allocated_bytes(),
                rayon::current_thread_index().unwrap_or(0)
            );

            let allocated: usize = normal_caches.caches.iter().map(|m| m.allocation_size()).sum();
            tracing::trace!("The last allocated HashMap took {allocated} bytes");

            let dummy = NormalCaches { caches: Vec::new() };
            let NormalCaches { caches: cache_maps } = mem::replace(normal_caches, dummy);
            *caches = InnerCaches::Spilling(SpillingCaches::from_cache_maps(cache_maps));
        }

        Ok(())
    }

    pub fn freeze(&mut self, source_id: usize) -> Result<Vec<FrozenCache<'_, 'extractor>>> {
        match &mut self.caches {
            InnerCaches::Normal(NormalCaches { caches }) => caches
                .iter_mut()
                .enumerate()
                .map(|(bucket_id, map)| {
                    // safety: we are transmuting the Bbbul into a FrozenBbbul
                    //         that are the same size.
                    let map = unsafe {
                        std::mem::transmute::<
                            &mut HashMap<
                                &[u8],
                                DelAddBbbul<BitPacker4x>, // from this
                                FxBuildHasher,
                                &Bump,
                            >,
                            &mut HashMap<
                                &[u8],
                                FrozenDelAddBbbul<BitPacker4x>, // to that
                                FxBuildHasher,
                                &Bump,
                            >,
                        >(map)
                    };
                    Ok(FrozenCache {
                        source_id,
                        bucket_id,
                        cache: FrozenMap::new(map),
                        spilled: Vec::new(),
                    })
                })
                .collect(),
            InnerCaches::Spilling(SpillingCaches { caches, spilled_entries, .. }) => caches
                .iter_mut()
                .zip(mem::take(spilled_entries))
                .enumerate()
                .map(|(bucket_id, (map, sorter))| {
                    let spilled = sorter
                        .into_reader_cursors()?
                        .into_iter()
                        .map(ReaderCursor::into_inner)
                        .map(BufReader::new)
                        .map(|bufreader| grenad::Reader::new(bufreader).map_err(Into::into))
                        .collect::<Result<_>>()?;
                    // safety: we are transmuting the Bbbul into a FrozenBbbul
                    //         that are the same size.
                    let map = unsafe {
                        std::mem::transmute::<
                            &mut HashMap<
                                &[u8],
                                DelAddBbbul<BitPacker4x>, // from this
                                FxBuildHasher,
                                &Bump,
                            >,
                            &mut HashMap<
                                &[u8],
                                FrozenDelAddBbbul<BitPacker4x>, // to that
                                FxBuildHasher,
                                &Bump,
                            >,
                        >(map)
                    };
                    Ok(FrozenCache { source_id, bucket_id, cache: FrozenMap::new(map), spilled })
                })
                .collect(),
        }
    }
}

/// SAFETY: No Thread-Local inside
unsafe impl MostlySend for BalancedCaches<'_> {}

struct NormalCaches<'extractor> {
    caches: Vec<
        HashMap<
            &'extractor [u8],
            DelAddBbbul<'extractor, BitPacker4x>,
            FxBuildHasher,
            &'extractor Bump,
        >,
    >,
}

impl<'extractor> NormalCaches<'extractor> {
    pub fn insert_del_u32(
        &mut self,
        hasher: &FxBuildHasher,
        alloc: &'extractor Bump,
        buckets: usize,
        key: &[u8],
        n: u32,
    ) {
        let hash = hasher.hash_one(key);
        let bucket = compute_bucket_from_hash(buckets, hash);

        match self.caches[bucket].raw_entry_mut().from_hash(hash, |&k| k == key) {
            RawEntryMut::Occupied(mut entry) => {
                entry.get_mut().del.get_or_insert_with(|| Bbbul::new_in(alloc)).insert(n);
            }
            RawEntryMut::Vacant(entry) => {
                entry.insert_hashed_nocheck(
                    hash,
                    alloc.alloc_slice_copy(key),
                    DelAddBbbul::new_del_u32_in(n, alloc),
                );
            }
        }
    }

    pub fn insert_add_u32(
        &mut self,
        hasher: &FxBuildHasher,
        alloc: &'extractor Bump,
        buckets: usize,
        key: &[u8],
        n: u32,
    ) {
        let hash = hasher.hash_one(key);
        let bucket = compute_bucket_from_hash(buckets, hash);
        match self.caches[bucket].raw_entry_mut().from_hash(hash, |&k| k == key) {
            RawEntryMut::Occupied(mut entry) => {
                entry.get_mut().add.get_or_insert_with(|| Bbbul::new_in(alloc)).insert(n);
            }
            RawEntryMut::Vacant(entry) => {
                entry.insert_hashed_nocheck(
                    hash,
                    alloc.alloc_slice_copy(key),
                    DelAddBbbul::new_add_u32_in(n, alloc),
                );
            }
        }
    }
}

struct SpillingCaches<'extractor> {
    caches: Vec<
        HashMap<
            &'extractor [u8],
            DelAddBbbul<'extractor, BitPacker4x>,
            FxBuildHasher,
            &'extractor Bump,
        >,
    >,
    spilled_entries: Vec<grenad::Sorter<MergeDeladdCboRoaringBitmaps>>,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
}

impl<'extractor> SpillingCaches<'extractor> {
    fn from_cache_maps(
        caches: Vec<
            HashMap<
                &'extractor [u8],
                DelAddBbbul<'extractor, BitPacker4x>,
                FxBuildHasher,
                &'extractor Bump,
            >,
        >,
    ) -> SpillingCaches<'extractor> {
        SpillingCaches {
            spilled_entries: iter::repeat_with(|| {
                let mut builder = grenad::SorterBuilder::new(MergeDeladdCboRoaringBitmaps);
                builder.dump_threshold(0);
                builder.allow_realloc(false);
                builder.build()
            })
            .take(caches.len())
            .collect(),
            caches,
            deladd_buffer: Vec::new(),
            cbo_buffer: Vec::new(),
        }
    }

    pub fn insert_del_u32(
        &mut self,
        hasher: &FxBuildHasher,
        alloc: &'extractor Bump,
        buckets: usize,
        key: &[u8],
        n: u32,
    ) -> Result<()> {
        let hash = hasher.hash_one(key);
        let bucket = compute_bucket_from_hash(buckets, hash);
        match self.caches[bucket].raw_entry_mut().from_hash(hash, |&k| k == key) {
            RawEntryMut::Occupied(mut entry) => {
                entry.get_mut().del.get_or_insert_with(|| Bbbul::new_in(alloc)).insert(n);
                Ok(())
            }
            RawEntryMut::Vacant(_entry) => spill_entry_to_sorter(
                &mut self.spilled_entries[bucket],
                &mut self.deladd_buffer,
                &mut self.cbo_buffer,
                key,
                DelAddRoaringBitmap::new_del_u32(n),
            ),
        }
    }

    pub fn insert_add_u32(
        &mut self,
        hasher: &FxBuildHasher,
        alloc: &'extractor Bump,
        buckets: usize,
        key: &[u8],
        n: u32,
    ) -> Result<()> {
        let hash = hasher.hash_one(key);
        let bucket = compute_bucket_from_hash(buckets, hash);
        match self.caches[bucket].raw_entry_mut().from_hash(hash, |&k| k == key) {
            RawEntryMut::Occupied(mut entry) => {
                entry.get_mut().add.get_or_insert_with(|| Bbbul::new_in(alloc)).insert(n);
                Ok(())
            }
            RawEntryMut::Vacant(_entry) => spill_entry_to_sorter(
                &mut self.spilled_entries[bucket],
                &mut self.deladd_buffer,
                &mut self.cbo_buffer,
                key,
                DelAddRoaringBitmap::new_add_u32(n),
            ),
        }
    }
}

#[inline]
fn compute_bucket_from_hash(buckets: usize, hash: u64) -> usize {
    hash as usize % buckets
}

fn spill_entry_to_sorter(
    spilled_entries: &mut grenad::Sorter<MergeDeladdCboRoaringBitmaps>,
    deladd_buffer: &mut Vec<u8>,
    cbo_buffer: &mut Vec<u8>,
    key: &[u8],
    deladd: DelAddRoaringBitmap,
) -> Result<()> {
    deladd_buffer.clear();
    let mut value_writer = KvWriterDelAdd::new(deladd_buffer);

    match deladd {
        DelAddRoaringBitmap { del: Some(del), add: None } => {
            cbo_buffer.clear();
            CboRoaringBitmapCodec::serialize_into_vec(&del, cbo_buffer);
            value_writer.insert(DelAdd::Deletion, &cbo_buffer)?;
        }
        DelAddRoaringBitmap { del: None, add: Some(add) } => {
            cbo_buffer.clear();
            CboRoaringBitmapCodec::serialize_into_vec(&add, cbo_buffer);
            value_writer.insert(DelAdd::Addition, &cbo_buffer)?;
        }
        DelAddRoaringBitmap { del: Some(del), add: Some(add) } => {
            cbo_buffer.clear();
            CboRoaringBitmapCodec::serialize_into_vec(&del, cbo_buffer);
            value_writer.insert(DelAdd::Deletion, &cbo_buffer)?;

            cbo_buffer.clear();
            CboRoaringBitmapCodec::serialize_into_vec(&add, cbo_buffer);
            value_writer.insert(DelAdd::Addition, &cbo_buffer)?;
        }
        DelAddRoaringBitmap { del: None, add: None } => return Ok(()),
    }

    let bytes = value_writer.into_inner().unwrap();
    spilled_entries.insert(key, bytes).map_err(Into::into)
}

pub struct FrozenCache<'a, 'extractor> {
    bucket_id: usize,
    source_id: usize,
    cache: FrozenMap<
        'a,
        'extractor,
        &'extractor [u8],
        FrozenDelAddBbbul<'extractor, BitPacker4x>,
        FxBuildHasher,
    >,
    spilled: Vec<grenad::Reader<BufReader<File>>>,
}

pub fn transpose_and_freeze_caches<'a, 'extractor>(
    caches: &'a mut [BalancedCaches<'extractor>],
) -> Result<Vec<Vec<FrozenCache<'a, 'extractor>>>> {
    let width = caches.first().map(BalancedCaches::buckets).unwrap_or(0);
    let mut bucket_caches: Vec<_> = iter::repeat_with(Vec::new).take(width).collect();

    for (thread_index, thread_cache) in caches.iter_mut().enumerate() {
        for frozen in thread_cache.freeze(thread_index)? {
            bucket_caches[frozen.bucket_id].push(frozen);
        }
    }

    Ok(bucket_caches)
}

/// Merges the caches that must be all associated to the same bucket
/// but make sure to sort the different buckets before performing the merges.
///
/// # Panics
///
/// - If the bucket IDs in these frozen caches are not exactly the same.
pub fn merge_caches_sorted<F>(frozen: Vec<FrozenCache>, mut f: F) -> Result<()>
where
    F: for<'a> FnMut(&'a [u8], DelAddRoaringBitmap) -> Result<()>,
{
    let mut maps = Vec::new();
    let mut heap = BinaryHeap::new();
    let mut current_bucket = None;
    for FrozenCache { source_id, bucket_id, cache, spilled } in frozen {
        assert_eq!(*current_bucket.get_or_insert(bucket_id), bucket_id);
        maps.push((source_id, cache));
        for reader in spilled {
            let mut cursor = reader.into_cursor()?;
            if cursor.move_on_next()?.is_some() {
                heap.push(Entry { cursor, source_id });
            }
        }
    }

    loop {
        let mut first_entry = match heap.pop() {
            Some(entry) => entry,
            None => break,
        };

        let (first_key, first_value) = match first_entry.cursor.current() {
            Some((key, value)) => (key, value),
            None => break,
        };

        let mut output = DelAddRoaringBitmap::from_bytes(first_value)?;
        while let Some(mut entry) = heap.peek_mut() {
            if let Some((key, value)) = entry.cursor.current() {
                if first_key != key {
                    break;
                }

                let new = DelAddRoaringBitmap::from_bytes(value)?;
                output = output.merge(new);
                // When we are done we the current value of this entry move make
                // it move forward and let the heap reorganize itself (on drop)
                if entry.cursor.move_on_next()?.is_none() {
                    PeekMut::pop(entry);
                }
            }
        }

        // Once we merged all of the spilled bitmaps we must also
        // fetch the entries from the non-spilled entries (the HashMaps).
        for (source_id, map) in maps.iter_mut() {
            debug_assert!(
                !(map.get(first_key).is_some() && first_entry.source_id == *source_id),
                "A thread should not have spiled a key that has been inserted in the cache"
            );
            if first_entry.source_id != *source_id {
                if let Some(new) = map.get_mut(first_key) {
                    output.union_and_clear_bbbul(new);
                }
            }
        }

        // We send the merged entry outside.
        (f)(first_key, output)?;

        // Don't forget to put the first entry back into the heap.
        if first_entry.cursor.move_on_next()?.is_some() {
            heap.push(first_entry);
        }
    }

    // Then manage the content on the HashMap entries that weren't taken (mem::take).
    while let Some((_, mut map)) = maps.pop() {
        // Make sure we don't try to work with entries already managed by the spilled
        let mut ordered_entries: Vec<_> =
            map.iter_mut().filter(|(_, bbbul)| !bbbul.is_empty()).collect();
        ordered_entries.sort_unstable_by_key(|(key, _)| *key);

        for (key, bbbul) in ordered_entries {
            let mut output = DelAddRoaringBitmap::empty();
            output.union_and_clear_bbbul(bbbul);

            for (_, rhs) in maps.iter_mut() {
                if let Some(new) = rhs.get_mut(key) {
                    output.union_and_clear_bbbul(new);
                }
            }

            // We send the merged entry outside.
            (f)(key, output)?;
        }
    }

    Ok(())
}

struct Entry<R> {
    cursor: ReaderCursor<R>,
    source_id: usize,
}

impl<R> Ord for Entry<R> {
    fn cmp(&self, other: &Entry<R>) -> Ordering {
        let skey = self.cursor.current().map(|(k, _)| k);
        let okey = other.cursor.current().map(|(k, _)| k);
        skey.cmp(&okey).then(self.source_id.cmp(&other.source_id)).reverse()
    }
}

impl<R> Eq for Entry<R> {}

impl<R> PartialEq for Entry<R> {
    fn eq(&self, other: &Entry<R>) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<R> PartialOrd for Entry<R> {
    fn partial_cmp(&self, other: &Entry<R>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct DelAddBbbul<'bump, B> {
    pub del: Option<Bbbul<'bump, B>>,
    pub add: Option<Bbbul<'bump, B>>,
}

impl<'bump, B: BitPacker> DelAddBbbul<'bump, B> {
    pub fn new_del_u32_in(n: u32, bump: &'bump Bump) -> Self {
        let mut bbbul = Bbbul::new_in(bump);
        bbbul.insert(n);
        DelAddBbbul { del: Some(bbbul), add: None }
    }

    pub fn new_add_u32_in(n: u32, bump: &'bump Bump) -> Self {
        let mut bbbul = Bbbul::new_in(bump);
        bbbul.insert(n);
        DelAddBbbul { del: None, add: Some(bbbul) }
    }
}

pub struct FrozenDelAddBbbul<'bump, B> {
    pub del: Option<FrozenBbbul<'bump, B>>,
    pub add: Option<FrozenBbbul<'bump, B>>,
}

impl<'bump, B> FrozenDelAddBbbul<'bump, B> {
    fn is_empty(&self) -> bool {
        self.del.is_none() && self.add.is_none()
    }
}

#[derive(Debug, Default, Clone)]
pub struct DelAddRoaringBitmap {
    pub del: Option<RoaringBitmap>,
    pub add: Option<RoaringBitmap>,
}

impl DelAddRoaringBitmap {
    fn from_bytes(bytes: &[u8]) -> io::Result<DelAddRoaringBitmap> {
        let reader = KvReaderDelAdd::from_slice(bytes);

        let del = match reader.get(DelAdd::Deletion) {
            Some(bytes) => CboRoaringBitmapCodec::deserialize_from(bytes).map(Some)?,
            None => None,
        };

        let add = match reader.get(DelAdd::Addition) {
            Some(bytes) => CboRoaringBitmapCodec::deserialize_from(bytes).map(Some)?,
            None => None,
        };

        Ok(DelAddRoaringBitmap { del, add })
    }

    pub fn empty() -> DelAddRoaringBitmap {
        DelAddRoaringBitmap { del: None, add: None }
    }

    pub fn insert_del_u32(&mut self, n: u32) {
        self.del.get_or_insert_with(RoaringBitmap::new).insert(n);
    }

    pub fn insert_add_u32(&mut self, n: u32) {
        self.add.get_or_insert_with(RoaringBitmap::new).insert(n);
    }

    pub fn new_del_u32(n: u32) -> Self {
        DelAddRoaringBitmap { del: Some(RoaringBitmap::from([n])), add: None }
    }

    pub fn new_add_u32(n: u32) -> Self {
        DelAddRoaringBitmap { del: None, add: Some(RoaringBitmap::from([n])) }
    }

    pub fn union_and_clear_bbbul<B: BitPacker>(&mut self, bbbul: &mut FrozenDelAddBbbul<'_, B>) {
        let FrozenDelAddBbbul { del, add } = bbbul;

        if let Some(ref mut bbbul) = del.take() {
            let del = self.del.get_or_insert_with(RoaringBitmap::new);
            let mut iter = bbbul.iter_and_clear();
            while let Some(block) = iter.next_block() {
                del.extend(block);
            }
        }

        if let Some(ref mut bbbul) = add.take() {
            let add = self.add.get_or_insert_with(RoaringBitmap::new);
            let mut iter = bbbul.iter_and_clear();
            while let Some(block) = iter.next_block() {
                add.extend(block);
            }
        }
    }

    pub fn merge(self, rhs: DelAddRoaringBitmap) -> DelAddRoaringBitmap {
        let DelAddRoaringBitmap { del, add } = self;
        let DelAddRoaringBitmap { del: ndel, add: nadd } = rhs;

        let del = match (del, ndel) {
            (None, None) => None,
            (None, Some(del)) | (Some(del), None) => Some(del),
            (Some(del), Some(ndel)) => Some(del | ndel),
        };

        let add = match (add, nadd) {
            (None, None) => None,
            (None, Some(add)) | (Some(add), None) => Some(add),
            (Some(add), Some(nadd)) => Some(add | nadd),
        };

        DelAddRoaringBitmap { del, add }
    }

    pub fn apply_to(&self, documents_ids: &mut RoaringBitmap) {
        let DelAddRoaringBitmap { del, add } = self;

        if let Some(del) = del {
            *documents_ids -= del;
        }

        if let Some(add) = add {
            *documents_ids |= add;
        }
    }
}

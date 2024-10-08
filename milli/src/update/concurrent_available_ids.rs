use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use roaring::RoaringBitmap;

/// A concurrent ID generate that will never return the same ID twice.
#[derive(Debug)]
pub struct ConcurrentAvailableIds {
    /// The current tree node ID we should use if there is no other IDs available.
    current: AtomicU32,
    /// The total number of tree node IDs used.
    used: AtomicU64,

    /// A list of IDs to exhaust before picking IDs from `current`.
    available: RoaringBitmap,
    /// The current Nth ID to select in the bitmap.
    select_in_bitmap: AtomicU32,
    /// Tells if you should look in the roaring bitmap or if all the IDs are already exhausted.
    look_into_bitmap: AtomicBool,
}

impl ConcurrentAvailableIds {
    /// Creates an ID generator returning unique IDs, avoiding the specified used IDs.
    pub fn new(used: RoaringBitmap) -> ConcurrentAvailableIds {
        let last_id = used.max().map_or(0, |id| id + 1);
        let used_ids = used.len();
        let available = RoaringBitmap::from_sorted_iter(0..last_id).unwrap() - used;

        ConcurrentAvailableIds {
            current: AtomicU32::new(last_id),
            used: AtomicU64::new(used_ids),
            select_in_bitmap: AtomicU32::new(0),
            look_into_bitmap: AtomicBool::new(!available.is_empty()),
            available,
        }
    }

    /// Returns a new unique ID and increase the count of IDs used.
    pub fn next(&self) -> Option<u32> {
        if self.used.fetch_add(1, Ordering::Relaxed) > u32::MAX as u64 {
            None
        } else if self.look_into_bitmap.load(Ordering::Relaxed) {
            let current = self.select_in_bitmap.fetch_add(1, Ordering::Relaxed);
            match self.available.select(current) {
                Some(id) => Some(id),
                None => {
                    self.look_into_bitmap.store(false, Ordering::Relaxed);
                    Some(self.current.fetch_add(1, Ordering::Relaxed))
                }
            }
        } else {
            Some(self.current.fetch_add(1, Ordering::Relaxed))
        }
    }

    /// Returns the number of used ids in total.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }
}

use std::iter::{Chain, FromIterator};
use std::ops::RangeInclusive;

use roaring::bitmap::{IntoIter, RoaringBitmap};

pub struct AvailableIds {
    iter: Chain<IntoIter, RangeInclusive<u32>>,
}

impl AvailableIds {
    pub fn new(docids: &RoaringBitmap) -> AvailableIds {
        match docids.max() {
            Some(last_id) => {
                let mut available = RoaringBitmap::from_iter(0..last_id);
                available -= docids;

                let iter = match last_id.checked_add(1) {
                    Some(id) => id..=u32::MAX,
                    #[allow(clippy::reversed_empty_ranges)]
                    None => 1..=0, // empty range iterator
                };

                AvailableIds { iter: available.into_iter().chain(iter) }
            }
            None => {
                let empty = RoaringBitmap::new().into_iter();
                AvailableIds { iter: empty.chain(0..=u32::MAX) }
            }
        }
    }
}

impl Iterator for AvailableIds {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let base = RoaringBitmap::new();
        let left = AvailableIds::new(&base);
        let right = 0..=u32::MAX;
        left.zip(right).take(500).for_each(|(l, r)| assert_eq!(l, r));
    }

    #[test]
    fn scattered() {
        let mut base = RoaringBitmap::new();
        base.insert(0);
        base.insert(10);
        base.insert(100);
        base.insert(405);

        let left = AvailableIds::new(&base);
        let right = (0..=u32::MAX).filter(|&n| n != 0 && n != 10 && n != 100 && n != 405);
        left.zip(right).take(500).for_each(|(l, r)| assert_eq!(l, r));
    }
}

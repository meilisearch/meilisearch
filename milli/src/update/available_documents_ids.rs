use std::iter::{Chain, FromIterator};
use std::ops::RangeInclusive;

use roaring::bitmap::{IntoIter, RoaringBitmap};

pub struct AvailableDocumentsIds {
    iter: Chain<IntoIter, RangeInclusive<u32>>,
}

impl AvailableDocumentsIds {
    pub fn from_documents_ids(
        docids: &RoaringBitmap,
        soft_deleted_docids: &RoaringBitmap,
    ) -> AvailableDocumentsIds {
        let used_docids = docids | soft_deleted_docids;

        match used_docids.max() {
            Some(last_id) => {
                let mut available = RoaringBitmap::from_iter(0..last_id);
                available -= used_docids;

                let iter = match last_id.checked_add(1) {
                    Some(id) => id..=u32::max_value(),
                    #[allow(clippy::reversed_empty_ranges)]
                    None => 1..=0, // empty range iterator
                };

                AvailableDocumentsIds { iter: available.into_iter().chain(iter) }
            }
            None => {
                let empty = RoaringBitmap::new().into_iter();
                AvailableDocumentsIds { iter: empty.chain(0..=u32::max_value()) }
            }
        }
    }
}

impl Iterator for AvailableDocumentsIds {
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
        let left = AvailableDocumentsIds::from_documents_ids(&base, &RoaringBitmap::new());
        let right = 0..=u32::max_value();
        left.zip(right).take(500).for_each(|(l, r)| assert_eq!(l, r));
    }

    #[test]
    fn scattered() {
        let mut base = RoaringBitmap::new();
        base.insert(0);
        base.insert(10);
        base.insert(100);
        base.insert(405);

        let left = AvailableDocumentsIds::from_documents_ids(&base, &RoaringBitmap::new());
        let right = (0..=u32::max_value()).filter(|&n| n != 0 && n != 10 && n != 100 && n != 405);
        left.zip(right).take(500).for_each(|(l, r)| assert_eq!(l, r));
    }

    #[test]
    fn soft_deleted() {
        let mut base = RoaringBitmap::new();
        base.insert(0);
        base.insert(10);
        base.insert(100);
        base.insert(405);

        let mut soft_deleted = RoaringBitmap::new();
        soft_deleted.insert(1);
        soft_deleted.insert(11);
        soft_deleted.insert(101);
        soft_deleted.insert(406);

        let left = AvailableDocumentsIds::from_documents_ids(&base, &soft_deleted);
        let right =
            (0..=u32::max_value()).filter(|&n| ![0, 1, 10, 11, 100, 101, 405, 406].contains(&n));
        left.zip(right).take(500).for_each(|(l, r)| assert_eq!(l, r));
    }
}

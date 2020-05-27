use std::borrow::Cow;

use heed::{BytesDecode, BytesEncode};
use sdset::Set;

use crate::DocumentId;
use super::cow_set::CowSet;

pub struct DocumentsIds;

impl BytesEncode<'_> for DocumentsIds {
    type EItem = Set<DocumentId>;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        CowSet::bytes_encode(item)
    }
}

impl<'a> BytesDecode<'a> for DocumentsIds {
    type DItem = Cow<'a, Set<DocumentId>>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        CowSet::bytes_decode(bytes)
    }
}

pub struct DiscoverIds<'a> {
    ids_iter: std::slice::Iter<'a, DocumentId>,
    left_id: Option<u32>,
    right_id: Option<u32>,
    available_range: std::ops::Range<u32>,
}

impl DiscoverIds<'_> {
    pub fn new(ids: &Set<DocumentId>) -> DiscoverIds {
        let mut ids_iter = ids.iter();
        let right_id = ids_iter.next().map(|id| id.0);
        let available_range = 0..right_id.unwrap_or(u32::max_value());
        DiscoverIds { ids_iter, left_id: None, right_id, available_range }
    }
}

impl Iterator for DiscoverIds<'_> {
    type Item = DocumentId;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.available_range.next() {
                // The available range gives us a new id, we return it.
                Some(id) => return Some(DocumentId(id)),
                // The available range is exhausted, we need to find the next one.
                None if self.available_range.end == u32::max_value() => return None,
                None => loop {
                    self.left_id = self.right_id.take();
                    self.right_id = self.ids_iter.next().map(|id| id.0);
                    match (self.left_id, self.right_id) {
                        // We found a gap in the used ids, we can yield all ids
                        // until the end of the gap
                        (Some(l), Some(r)) => if l.saturating_add(1) != r {
                            self.available_range = (l + 1)..r;
                            break;
                        },
                        // The last used id has been reached, we can use all ids
                        // until u32 MAX
                        (Some(l), None) => {
                            self.available_range = l.saturating_add(1)..u32::max_value();
                            break;
                        },
                        _ => (),
                    }
                },
            }
        }
    }
}

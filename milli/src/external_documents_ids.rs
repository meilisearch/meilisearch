use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::{fmt, str};

use fst::map::IndexedValue;
use fst::{IntoStreamer, Streamer};
use roaring::RoaringBitmap;

const DELETED_ID: u64 = u64::MAX;

pub struct ExternalDocumentsIds<'a> {
    pub(crate) hard: fst::Map<Cow<'a, [u8]>>,
    pub(crate) soft: fst::Map<Cow<'a, [u8]>>,
    soft_deleted_docids: RoaringBitmap,
}

impl<'a> ExternalDocumentsIds<'a> {
    pub fn new(
        hard: fst::Map<Cow<'a, [u8]>>,
        soft: fst::Map<Cow<'a, [u8]>>,
        soft_deleted_docids: RoaringBitmap,
    ) -> ExternalDocumentsIds<'a> {
        ExternalDocumentsIds { hard, soft, soft_deleted_docids }
    }

    pub fn into_static(self) -> ExternalDocumentsIds<'static> {
        ExternalDocumentsIds {
            hard: self.hard.map_data(|c| Cow::Owned(c.into_owned())).unwrap(),
            soft: self.soft.map_data(|c| Cow::Owned(c.into_owned())).unwrap(),
            soft_deleted_docids: self.soft_deleted_docids,
        }
    }

    /// Returns `true` if hard and soft external documents lists are empty.
    pub fn is_empty(&self) -> bool {
        self.hard.is_empty() && self.soft.is_empty()
    }

    pub fn get<A: AsRef<[u8]>>(&self, external_id: A) -> Option<u32> {
        let external_id = external_id.as_ref();
        match self.soft.get(external_id).or_else(|| self.hard.get(external_id)) {
            Some(id) if id != DELETED_ID && !self.soft_deleted_docids.contains(id as u32) => {
                Some(id.try_into().unwrap())
            }
            _otherwise => None,
        }
    }

    pub fn delete_ids<A: AsRef<[u8]>>(&mut self, other: fst::Set<A>) -> fst::Result<()> {
        let other = fst::Map::from(other.into_fst());
        let union_op = self.soft.op().add(&other).r#union();

        let mut iter = union_op.into_stream();
        let mut new_soft_builder = fst::MapBuilder::memory();
        while let Some((external_id, docids)) = iter.next() {
            if docids.iter().any(|v| v.index == 1) {
                // If the `other` set returns a value here it means
                // that it must be marked as deleted.
                new_soft_builder.insert(external_id, DELETED_ID)?;
            } else {
                let value = docids.iter().find(|v| v.index == 0).unwrap().value;
                new_soft_builder.insert(external_id, value)?;
            }
        }

        drop(iter);

        // We save this new map as the new soft map.
        self.soft = new_soft_builder.into_map().map_data(Cow::Owned)?;
        self.merge_soft_into_hard()
    }

    /// Rebuild the internal FSTs in the ExternalDocumentsIds structure such that they
    /// don't contain any soft deleted document id.
    pub fn delete_soft_deleted_documents_ids_from_fsts(&mut self) -> fst::Result<()> {
        let mut new_hard_builder = fst::MapBuilder::memory();

        let union_op = self.hard.op().add(&self.soft).r#union();
        let mut iter = union_op.into_stream();
        while let Some((external_id, docids)) = iter.next() {
            // prefer selecting the ids from soft, always
            let id = indexed_last_value(docids).unwrap();
            if id != DELETED_ID && !self.soft_deleted_docids.contains(id as u32) {
                new_hard_builder.insert(external_id, id)?;
            }
        }
        drop(iter);

        // Delete soft map completely
        self.soft = fst::Map::default().map_data(Cow::Owned)?;
        // We save the new map as the new hard map.
        self.hard = new_hard_builder.into_map().map_data(Cow::Owned)?;

        Ok(())
    }

    pub fn insert_ids<A: AsRef<[u8]>>(&mut self, other: &fst::Map<A>) -> fst::Result<()> {
        let union_op = self.soft.op().add(other).r#union();

        let mut new_soft_builder = fst::MapBuilder::memory();
        let mut iter = union_op.into_stream();
        while let Some((external_id, marked_docids)) = iter.next() {
            let id = indexed_last_value(marked_docids).unwrap();
            new_soft_builder.insert(external_id, id)?;
        }

        drop(iter);

        // We save the new map as the new soft map.
        self.soft = new_soft_builder.into_map().map_data(Cow::Owned)?;
        self.merge_soft_into_hard()
    }

    /// An helper function to debug this type, returns an `HashMap` of both,
    /// soft and hard fst maps, combined.
    pub fn to_hash_map(&self) -> HashMap<String, u32> {
        let mut map = HashMap::new();

        let union_op = self.hard.op().add(&self.soft).r#union();
        let mut iter = union_op.into_stream();
        while let Some((external_id, marked_docids)) = iter.next() {
            let id = indexed_last_value(marked_docids).unwrap();
            if id != DELETED_ID {
                let external_id = str::from_utf8(external_id).unwrap();
                map.insert(external_id.to_owned(), id.try_into().unwrap());
            }
        }

        map
    }

    fn merge_soft_into_hard(&mut self) -> fst::Result<()> {
        if self.soft.len() >= self.hard.len() / 2 {
            let union_op = self.hard.op().add(&self.soft).r#union();

            let mut iter = union_op.into_stream();
            let mut new_hard_builder = fst::MapBuilder::memory();
            while let Some((external_id, marked_docids)) = iter.next() {
                let value = indexed_last_value(marked_docids).unwrap();
                if value != DELETED_ID {
                    new_hard_builder.insert(external_id, value)?;
                }
            }

            drop(iter);

            self.hard = new_hard_builder.into_map().map_data(Cow::Owned)?;
            self.soft = fst::Map::default().map_data(Cow::Owned)?;
        }

        Ok(())
    }
}

impl fmt::Debug for ExternalDocumentsIds<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("ExternalDocumentsIds").field(&self.to_hash_map()).finish()
    }
}

impl Default for ExternalDocumentsIds<'static> {
    fn default() -> Self {
        ExternalDocumentsIds {
            hard: fst::Map::default().map_data(Cow::Owned).unwrap(),
            soft: fst::Map::default().map_data(Cow::Owned).unwrap(),
            soft_deleted_docids: RoaringBitmap::new(),
        }
    }
}

/// Returns the value of the `IndexedValue` with the highest _index_.
fn indexed_last_value(indexed_values: &[IndexedValue]) -> Option<u64> {
    indexed_values.iter().copied().max_by_key(|iv| iv.index).map(|iv| iv.value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_insert_delete_ids() {
        let mut external_documents_ids = ExternalDocumentsIds::default();

        let new_ids = fst::Map::from_iter(vec![("a", 1), ("b", 2), ("c", 3), ("d", 4)]).unwrap();
        external_documents_ids.insert_ids(&new_ids).unwrap();

        assert_eq!(external_documents_ids.get("a"), Some(1));
        assert_eq!(external_documents_ids.get("b"), Some(2));
        assert_eq!(external_documents_ids.get("c"), Some(3));
        assert_eq!(external_documents_ids.get("d"), Some(4));

        let new_ids = fst::Map::from_iter(vec![("e", 5), ("f", 6), ("g", 7)]).unwrap();
        external_documents_ids.insert_ids(&new_ids).unwrap();

        assert_eq!(external_documents_ids.get("a"), Some(1));
        assert_eq!(external_documents_ids.get("b"), Some(2));
        assert_eq!(external_documents_ids.get("c"), Some(3));
        assert_eq!(external_documents_ids.get("d"), Some(4));
        assert_eq!(external_documents_ids.get("e"), Some(5));
        assert_eq!(external_documents_ids.get("f"), Some(6));
        assert_eq!(external_documents_ids.get("g"), Some(7));

        let del_ids = fst::Set::from_iter(vec!["a", "c", "f"]).unwrap();
        external_documents_ids.delete_ids(del_ids).unwrap();

        assert_eq!(external_documents_ids.get("a"), None);
        assert_eq!(external_documents_ids.get("b"), Some(2));
        assert_eq!(external_documents_ids.get("c"), None);
        assert_eq!(external_documents_ids.get("d"), Some(4));
        assert_eq!(external_documents_ids.get("e"), Some(5));
        assert_eq!(external_documents_ids.get("f"), None);
        assert_eq!(external_documents_ids.get("g"), Some(7));

        let new_ids = fst::Map::from_iter(vec![("a", 5), ("b", 6), ("h", 8)]).unwrap();
        external_documents_ids.insert_ids(&new_ids).unwrap();

        assert_eq!(external_documents_ids.get("a"), Some(5));
        assert_eq!(external_documents_ids.get("b"), Some(6));
        assert_eq!(external_documents_ids.get("c"), None);
        assert_eq!(external_documents_ids.get("d"), Some(4));
        assert_eq!(external_documents_ids.get("e"), Some(5));
        assert_eq!(external_documents_ids.get("f"), None);
        assert_eq!(external_documents_ids.get("g"), Some(7));
        assert_eq!(external_documents_ids.get("h"), Some(8));
    }

    #[test]
    fn strange_delete_insert_ids() {
        let mut external_documents_ids = ExternalDocumentsIds::default();

        let new_ids =
            fst::Map::from_iter(vec![("1", 0), ("123", 1), ("30", 2), ("456", 3)]).unwrap();
        external_documents_ids.insert_ids(&new_ids).unwrap();
        assert_eq!(external_documents_ids.get("1"), Some(0));
        assert_eq!(external_documents_ids.get("123"), Some(1));
        assert_eq!(external_documents_ids.get("30"), Some(2));
        assert_eq!(external_documents_ids.get("456"), Some(3));

        let deleted_ids = fst::Set::from_iter(vec!["30"]).unwrap();
        external_documents_ids.delete_ids(deleted_ids).unwrap();
        assert_eq!(external_documents_ids.get("30"), None);

        let new_ids = fst::Map::from_iter(vec![("30", 2)]).unwrap();
        external_documents_ids.insert_ids(&new_ids).unwrap();
        assert_eq!(external_documents_ids.get("30"), Some(2));
    }
}

use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::{fmt, str};

use fst::{IntoStreamer, Streamer};

const DELETED_ID: u64 = u64::MAX;

pub struct ExternalDocumentsIds<'a> {
    pub(crate) hard: fst::Map<Cow<'a, [u8]>>,
    pub(crate) soft: fst::Map<Cow<'a, [u8]>>,
}

impl<'a> ExternalDocumentsIds<'a> {
    pub fn new(
        hard: fst::Map<Cow<'a, [u8]>>,
        soft: fst::Map<Cow<'a, [u8]>>,
    ) -> ExternalDocumentsIds<'a> {
        ExternalDocumentsIds { hard, soft }
    }

    pub fn into_static(self) -> ExternalDocumentsIds<'static> {
        ExternalDocumentsIds {
            hard: self.hard.map_data(|c| Cow::Owned(c.into_owned())).unwrap(),
            soft: self.soft.map_data(|c| Cow::Owned(c.into_owned())).unwrap(),
        }
    }

    /// Returns `true` if hard and soft external documents lists are empty.
    pub fn is_empty(&self) -> bool {
        self.hard.is_empty() && self.soft.is_empty()
    }

    pub fn get<A: AsRef<[u8]>>(&self, external_id: A) -> Option<u32> {
        let external_id = external_id.as_ref();
        match self.soft.get(external_id).or_else(|| self.hard.get(external_id)) {
            // u64 MAX means deleted in the soft fst map
            Some(id) if id != DELETED_ID => Some(id.try_into().unwrap()),
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
                new_soft_builder.insert(external_id, docids[0].value)?;
            }
        }

        drop(iter);

        // We save this new map as the new soft map.
        self.soft = new_soft_builder.into_map().map_data(Cow::Owned)?;
        self.merge_soft_into_hard()
    }

    pub fn insert_ids<A: AsRef<[u8]>>(&mut self, other: &fst::Map<A>) -> fst::Result<()> {
        let union_op = self.soft.op().add(other).r#union();

        let mut new_soft_builder = fst::MapBuilder::memory();
        let mut iter = union_op.into_stream();
        while let Some((external_id, docids)) = iter.next() {
            let id = docids.last().unwrap().value;
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
            let id = marked_docids.last().unwrap().value;
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
            while let Some((external_id, docids)) = iter.next() {
                if docids.len() == 2 {
                    if docids[1].value != DELETED_ID {
                        new_hard_builder.insert(external_id, docids[1].value)?;
                    }
                } else {
                    new_hard_builder.insert(external_id, docids[0].value)?;
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
        }
    }
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
}

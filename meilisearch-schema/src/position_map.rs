use std::collections::BTreeMap;

use crate::{FieldId, IndexedPos};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PositionMap {
    pos_to_field: Vec<FieldId>,
    field_to_pos: BTreeMap<FieldId, IndexedPos>,
}

impl PositionMap {
    /// insert `id` at the specified `position` updating the other position if a shift is caused by
    /// the operation. If `id` is already present in the position map, it is moved to the requested
    /// `position`, potentially causing shifts.
    pub fn insert(&mut self, id: FieldId, position: IndexedPos) -> IndexedPos {
        let mut upos = position.0 as usize;
        let mut must_rebuild_map = false;

        if let Some(old_pos) = self.field_to_pos.get(&id) {
            let uold_pos = old_pos.0 as usize;
            self.pos_to_field.remove(uold_pos);
            must_rebuild_map = true;
        }

        if upos < self.pos_to_field.len() {
            self.pos_to_field.insert(upos, id);
            must_rebuild_map = true;
        } else {
            upos = self.pos_to_field.len();
            self.pos_to_field.push(id);
        }

        // we only need to update all the positions if there have been a shift a some point. In
        // most cases we only did a push, so we don't need to rebuild the `field_to_pos` map.
        if must_rebuild_map {
            self.field_to_pos.clear();
            self.field_to_pos.extend(
                self.pos_to_field
                .iter()
                .enumerate()
                .map(|(p, f)| (*f, IndexedPos(p as u16))),
            );
        } else {
            self.field_to_pos.insert(id, IndexedPos(upos as u16));
        }
        IndexedPos(upos as u16)
    }

    /// Pushes `id` in last position
    pub fn push(&mut self, id: FieldId) -> IndexedPos {
        let pos = self.len();
        self.insert(id, IndexedPos(pos as u16))
    }

    pub fn len(&self) -> usize {
        self.pos_to_field.len()
    }

    pub fn field_to_pos(&self, id: FieldId) -> Option<IndexedPos> {
        self.field_to_pos.get(&id).cloned()
    }

    pub fn pos_to_field(&self, pos: IndexedPos) -> Option<FieldId> {
        let pos = pos.0 as usize;
        self.pos_to_field.get(pos).cloned()
    }

    pub fn field_pos(&self) -> impl Iterator<Item = (FieldId, IndexedPos)> + '_ {
        self.pos_to_field
            .iter()
            .enumerate()
            .map(|(i, f)| (*f, IndexedPos(i as u16)))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_default() {
        assert_eq!(
            format!("{:?}", PositionMap::default()),
            r##"PositionMap { pos_to_field: [], field_to_pos: {} }"##
        );
    }

    #[test]
    fn test_insert() {
        let mut map = PositionMap::default();
        // changing position removes from old position
        map.insert(0.into(), 0.into());
        map.insert(1.into(), 1.into());
        assert_eq!(
            format!("{:?}", map),
            r##"PositionMap { pos_to_field: [FieldId(0), FieldId(1)], field_to_pos: {FieldId(0): IndexedPos(0), FieldId(1): IndexedPos(1)} }"##
        );
        map.insert(0.into(), 1.into());
        assert_eq!(
            format!("{:?}", map),
            r##"PositionMap { pos_to_field: [FieldId(1), FieldId(0)], field_to_pos: {FieldId(0): IndexedPos(1), FieldId(1): IndexedPos(0)} }"##
        );
        map.insert(2.into(), 1.into());
        assert_eq!(
            format!("{:?}", map),
            r##"PositionMap { pos_to_field: [FieldId(1), FieldId(2), FieldId(0)], field_to_pos: {FieldId(0): IndexedPos(2), FieldId(1): IndexedPos(0), FieldId(2): IndexedPos(1)} }"##
        );
    }

    #[test]
    fn test_push() {
        let mut map = PositionMap::default();
        map.push(0.into());
        map.push(2.into());
        assert_eq!(map.len(), 2);
        assert_eq!(
            format!("{:?}", map),
            r##"PositionMap { pos_to_field: [FieldId(0), FieldId(2)], field_to_pos: {FieldId(0): IndexedPos(0), FieldId(2): IndexedPos(1)} }"##
        );
    }

    #[test]
    fn test_field_to_pos() {
        let mut map = PositionMap::default();
        map.push(0.into());
        map.push(2.into());
        assert_eq!(map.field_to_pos(2.into()), Some(1.into()));
        assert_eq!(map.field_to_pos(0.into()), Some(0.into()));
        assert_eq!(map.field_to_pos(4.into()), None);
    }

    #[test]
    fn test_pos_to_field() {
        let mut map = PositionMap::default();
        map.push(0.into());
        map.push(2.into());
        map.push(3.into());
        map.push(4.into());
        assert_eq!(
            format!("{:?}", map),
            r##"PositionMap { pos_to_field: [FieldId(0), FieldId(2), FieldId(3), FieldId(4)], field_to_pos: {FieldId(0): IndexedPos(0), FieldId(2): IndexedPos(1), FieldId(3): IndexedPos(2), FieldId(4): IndexedPos(3)} }"##
        );
        assert_eq!(map.pos_to_field(0.into()), Some(0.into()));
        assert_eq!(map.pos_to_field(1.into()), Some(2.into()));
        assert_eq!(map.pos_to_field(2.into()), Some(3.into()));
        assert_eq!(map.pos_to_field(3.into()), Some(4.into()));
        assert_eq!(map.pos_to_field(4.into()), None);
    }

    #[test]
    fn test_field_pos() {
        let mut map = PositionMap::default();
        map.push(0.into());
        map.push(2.into());
        let mut iter = map.field_pos();
        assert_eq!(iter.next(), Some((0.into(), 0.into())));
        assert_eq!(iter.next(), Some((2.into(), 1.into())));
        assert_eq!(iter.next(), None);
    }
}

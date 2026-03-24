use std::{collections::BTreeMap, ops::BitOr};

use either::Either;
use itertools::{EitherOrBoth, Itertools};

use crate::{FieldId, SmallVec8};

#[derive(Default, Debug)]
pub struct WordDelta {
    pub added: BTreeMap<String, SmallVec8<FieldId>>,
    pub modified: BTreeMap<String, SmallVec8<FieldId>>,
    pub deleted: BTreeMap<String, SmallVec8<FieldId>>,
}

impl WordDelta {
    pub fn is_empty(&self) -> bool {
        let Self { added, modified, deleted } = self;
        added.is_empty() && modified.is_empty() && deleted.is_empty()
    }

    pub fn added_or_modified_words(&self) -> impl Iterator<Item = &str> + '_ {
        itertools::merge_join_by(self.added.keys(), self.modified.keys(), |a, b| a.cmp(b))
            .map(EitherOrBoth::into_left)
            .dedup()
            .map(|ss| ss.as_str())
    }

    pub fn added_or_deleted_words(&self) -> impl Iterator<Item = Either<&str, &str>> + '_ {
        itertools::merge_join_by(self.added.keys(), self.modified.keys(), |a, b| a.cmp(b))
            .filter_map(|eob| match eob {
                EitherOrBoth::Both(_, _) => None,
                EitherOrBoth::Left(added) => Some(Either::Left(added.as_str())),
                EitherOrBoth::Right(deleted) => Some(Either::Right(deleted.as_str())),
            })
            .dedup()
    }

    pub fn deleted_words(&self) -> impl Iterator<Item = &str> + '_ {
        self.deleted.keys().dedup().map(|ss| ss.as_str())
    }

    pub fn insert_added(&mut self, word: String, fid: FieldId) {
        insert_ordered(self.added.entry(word).or_default(), fid)
    }

    pub fn insert_modified(&mut self, word: String, fid: FieldId) {
        insert_ordered(self.added.entry(word).or_default(), fid)
    }

    pub fn insert_deleted(&mut self, word: String, fid: FieldId) {
        insert_ordered(self.added.entry(word).or_default(), fid)
    }
}

fn insert_ordered(vec: &mut SmallVec8<FieldId>, fid: FieldId) {
    if let Err(index) = vec.binary_search(&fid) {
        vec.insert(index, fid);
    }
}

impl BitOr for WordDelta {
    type Output = Self;

    fn bitor(mut self, rhs: Self) -> Self::Output {
        use itertools::{merge_join_by, EitherOrBoth};
        use std::mem::take;

        let Self { added, modified, deleted } = &mut self;
        let Self { added: rhs_added, modified: rhs_modified, deleted: rhs_deleted } = rhs;
        let maps = [(added, rhs_added), (modified, rhs_modified), (deleted, rhs_deleted)];

        for (lhs, rhs) in maps {
            for (word, rhs_fields) in rhs {
                let lhs_fields = lhs.entry(word).or_default();
                *lhs_fields = merge_join_by(take(lhs_fields), rhs_fields, |a, b| a.cmp(b))
                    .map(EitherOrBoth::into_left) // or into_right
                    .collect();
            }
        }

        self
    }
}

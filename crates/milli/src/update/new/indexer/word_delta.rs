use std::{collections::BTreeSet, ops::BitOr};

use either::Either;
use itertools::{EitherOrBoth, Itertools};

#[derive(Default, Debug)]
pub struct WordDelta {
    pub added: BTreeSet<String>,
    pub modified: BTreeSet<String>,
    pub deleted: BTreeSet<String>,
}

impl WordDelta {
    pub fn is_empty(&self) -> bool {
        let Self { added, modified, deleted } = self;
        added.is_empty() && modified.is_empty() && deleted.is_empty()
    }

    pub fn added_or_modified_words(&self) -> impl Iterator<Item = &str> + '_ {
        itertools::merge_join_by(self.added.iter(), self.modified.iter(), |a, b| a.cmp(b))
            .map(EitherOrBoth::into_left)
            .dedup()
            .map(|s| s.as_str())
    }

    pub fn added_or_deleted_words(&self) -> impl Iterator<Item = Either<&str, &str>> + '_ {
        itertools::merge_join_by(self.added.iter(), self.modified.iter(), |a, b| a.cmp(b))
            .filter_map(|eob| match eob {
                EitherOrBoth::Both(_, _) => None,
                EitherOrBoth::Left(added) => Some(Either::Left(added.as_str())),
                EitherOrBoth::Right(deleted) => Some(Either::Right(deleted.as_str())),
            })
            .dedup()
    }

    pub fn deleted_words(&self) -> impl Iterator<Item = &str> + '_ {
        self.deleted.iter().dedup().map(|s| s.as_str())
    }

    pub fn insert_added(&mut self, word: String) {
        self.added.insert(word);
    }

    pub fn insert_modified(&mut self, word: String) {
        self.modified.insert(word);
    }

    pub fn insert_deleted(&mut self, word: String) {
        self.deleted.insert(word);
    }
}

impl BitOr for WordDelta {
    type Output = Self;

    fn bitor(mut self, rhs: Self) -> Self::Output {
        let Self { added, modified, deleted } = &mut self;
        let Self { added: rhs_added, modified: rhs_modified, deleted: rhs_deleted } = rhs;

        added.extend(rhs_added);
        modified.extend(rhs_modified);
        deleted.extend(rhs_deleted);

        self
    }
}

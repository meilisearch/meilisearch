pub mod criterion;
mod query_builder;
mod distinct_map;

use std::slice::Windows;

use sdset::SetBuf;
use group_by::GroupBy;

use crate::{Match, DocumentId};

pub use self::query_builder::{FilterFunc, QueryBuilder, DistinctQueryBuilder};

#[inline]
fn match_query_index(a: &Match, b: &Match) -> bool {
    a.query_index == b.query_index
}

#[derive(Debug, Clone)]
pub struct Document {
    pub id: DocumentId,
    pub matches: Matches,
}

impl Document {
    pub fn new(doc: DocumentId, match_: Match) -> Self {
        let matches = SetBuf::new_unchecked(vec![match_]);
        Self::from_matches(doc, matches)
    }

    pub fn from_matches(id: DocumentId, matches: SetBuf<Match>) -> Self {
        let mut last = 0;
        let mut slices = vec![0];
        for group in GroupBy::new(&matches, match_query_index) {
            let index = last + group.len();
            slices.push(index);
            last = index;
        }

        let matches = Matches { matches, slices };
        Self { id, matches }
    }

    pub fn from_unsorted_matches(doc: DocumentId, mut matches: Vec<Match>) -> Self {
        matches.sort_unstable();
        let matches = SetBuf::new_unchecked(matches);
        Self::from_matches(doc, matches)
    }
}

#[derive(Debug, Clone)]
pub struct Matches {
    matches: SetBuf<Match>,
    slices: Vec<usize>,
}

impl Matches {
    pub fn query_index_groups(&self) -> QueryIndexGroups {
        QueryIndexGroups {
            matches: &self.matches,
            windows: self.slices.windows(2),
        }
    }
}

pub struct QueryIndexGroups<'a, 'b> {
    matches: &'a [Match],
    windows: Windows<'b, usize>,
}

impl<'a, 'b> Iterator for QueryIndexGroups<'a, 'b> {
    type Item = &'a [Match];

    fn next(&mut self) -> Option<Self::Item> {
        self.windows.next().map(|range| {
            match *range {
                [left, right] => &self.matches[left..right],
                _             => unreachable!()
            }
        })
    }
}

// impl ExactSizeIterator for QueryIndexGroups<'_, '_> {
//     fn len(&self) -> usize {
//         self.windows.len() // FIXME (+1) ?
//     }
// }

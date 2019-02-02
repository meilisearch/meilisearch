pub mod criterion;
mod query_builder;
mod distinct_map;

use std::sync::Arc;

use slice_group_by::GroupBy;
use rayon::slice::ParallelSliceMut;

use crate::{Match, DocumentId};

pub use self::query_builder::{FilterFunc, QueryBuilder, DistinctQueryBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Document {
    pub id: DocumentId,
    pub matches: Vec<Match>,
}

impl Document {
    pub fn from_raw(raw: &RawDocument) -> Document {
        let len = raw.matches.range.len();
        let mut matches = Vec::with_capacity(len);

        let query_index = raw.query_index();
        let distance = raw.distance();
        let attribute = raw.attribute();
        let word_index = raw.word_index();
        let is_exact = raw.is_exact();
        let char_index = raw.char_index();
        let char_length = raw.char_length();

        for i in 0..len {
            let match_ = Match {
                query_index: query_index[i],
                distance: distance[i],
                attribute: attribute[i],
                word_index: word_index[i],
                is_exact: is_exact[i],
                char_index: char_index[i],
                char_length: char_length[i],
            };
            matches.push(match_);
        }

        Document { id: raw.id, matches }
    }
}

#[derive(Clone)]
pub struct RawDocument {
    pub id: DocumentId,
    pub matches: SharedMatches,
}

impl RawDocument {
    fn new(id: DocumentId, range: Range, matches: Arc<Matches>) -> RawDocument {
        RawDocument { id, matches: SharedMatches { range, matches } }
    }

    pub fn query_index(&self) -> &[u32] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.query_index.get_unchecked(r.start..r.end) }
    }

    pub fn distance(&self) -> &[u8] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.distance.get_unchecked(r.start..r.end) }
    }

    pub fn attribute(&self) -> &[u16] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.attribute.get_unchecked(r.start..r.end) }
    }

    pub fn word_index(&self) -> &[u32] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.word_index.get_unchecked(r.start..r.end) }
    }

    pub fn is_exact(&self) -> &[bool] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.is_exact.get_unchecked(r.start..r.end) }
    }

    pub fn char_index(&self) -> &[u32] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.char_index.get_unchecked(r.start..r.end) }
    }

    pub fn char_length(&self) -> &[u16] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.char_length.get_unchecked(r.start..r.end) }
    }
}

pub fn raw_documents_from_matches(mut matches: Vec<(DocumentId, Match)>) -> Vec<RawDocument> {
    let mut docs_ranges = Vec::<(DocumentId, Range)>::new();
    let mut matches2 = Matches::with_capacity(matches.len());

    matches.par_sort_unstable();

    for group in matches.linear_group_by(|(a, _), (b, _)| a == b) {
        let id = group[0].0;
        let start = docs_ranges.last().map(|(_, r)| r.end).unwrap_or(0);
        let end = start + group.len();
        docs_ranges.push((id, Range { start, end }));

        matches2.extend_from_slice(group);
    }

    let matches = Arc::new(matches2);
    docs_ranges.into_iter().map(|(i, r)| RawDocument::new(i, r, matches.clone())).collect()
}

#[derive(Debug, Copy, Clone)]
struct Range {
    start: usize,
    end: usize,
}

impl Range {
    fn len(self) -> usize {
        self.end - self.start
    }
}

#[derive(Clone)]
pub struct SharedMatches {
    range: Range,
    matches: Arc<Matches>,
}

#[derive(Clone)]
struct Matches {
    query_index: Vec<u32>,
    distance: Vec<u8>,
    attribute: Vec<u16>,
    word_index: Vec<u32>,
    is_exact: Vec<bool>,
    char_index: Vec<u32>,
    char_length: Vec<u16>,
}

impl Matches {
    fn with_capacity(cap: usize) -> Matches {
        Matches {
            query_index: Vec::with_capacity(cap),
            distance: Vec::with_capacity(cap),
            attribute: Vec::with_capacity(cap),
            word_index: Vec::with_capacity(cap),
            is_exact: Vec::with_capacity(cap),
            char_index: Vec::with_capacity(cap),
            char_length: Vec::with_capacity(cap),
        }
    }

    fn extend_from_slice(&mut self, matches: &[(DocumentId, Match)]) {
        for (_, match_) in matches {
            self.query_index.push(match_.query_index);
            self.distance.push(match_.distance);
            self.attribute.push(match_.attribute);
            self.word_index.push(match_.word_index);
            self.is_exact.push(match_.is_exact);
            self.char_index.push(match_.char_index);
            self.char_length.push(match_.char_length);
        }
    }
}

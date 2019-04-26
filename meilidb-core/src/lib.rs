pub mod criterion;
mod index;
mod automaton;
mod query_builder;
mod distinct_map;

use std::sync::Arc;
use serde::{Serialize, Deserialize};

use slice_group_by::GroupBy;
use rayon::slice::ParallelSliceMut;
use zerocopy::{AsBytes, FromBytes};

pub use self::index::{Index, Store};
pub use self::query_builder::{QueryBuilder, DistinctQueryBuilder};

/// Represent an internally generated document unique identifier.
///
/// It is used to inform the database the document you want to deserialize.
/// Helpful for custom ranking.
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[derive(Serialize, Deserialize)]
#[derive(AsBytes, FromBytes)]
#[repr(C)]
pub struct DocumentId(pub u64);

/// This structure represent the position of a word
/// in a document and its attributes.
///
/// This is stored in the map, generated at index time,
/// extracted and interpreted at search time.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[derive(AsBytes, FromBytes)]
#[repr(C)]
pub struct DocIndex {
    /// The document identifier where the word was found.
    pub document_id: DocumentId,

    /// The attribute in the document where the word was found
    /// along with the index in it.
    pub attribute: u16,
    pub word_index: u16,

    /// The position in bytes where the word was found
    /// along with the length of it.
    ///
    /// It informs on the original word area in the text indexed
    /// without needing to run the tokenizer again.
    pub char_index: u16,
    pub char_length: u16,
}

/// This structure represent a matching word with informations
/// on the location of the word in the document.
///
/// The order of the field is important because it defines
/// the way these structures are ordered between themselves.
///
/// The word in itself is not important.
// TODO do data oriented programming ? very arrays ?
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Match {
    /// The word index in the query sentence.
    /// Same as the `attribute_index` but for the query words.
    ///
    /// Used to retrieve the automaton that match this word.
    pub query_index: u32,

    /// The distance the word has with the query word
    /// (i.e. the Levenshtein distance).
    pub distance: u8,

    /// The attribute in the document where the word was found
    /// along with the index in it.
    pub attribute: u16,
    pub word_index: u16,

    /// Whether the word that match is an exact match or a prefix.
    pub is_exact: bool,

    /// The position in bytes where the word was found
    /// along with the length of it.
    ///
    /// It informs on the original word area in the text indexed
    /// without needing to run the tokenizer again.
    pub char_index: u16,
    pub char_length: u16,
}

impl Match {
    pub fn zero() -> Self {
        Match {
            query_index: 0,
            distance: 0,
            attribute: 0,
            word_index: 0,
            is_exact: false,
            char_index: 0,
            char_length: 0,
        }
    }

    pub fn max() -> Self {
        Match {
            query_index: u32::max_value(),
            distance: u8::max_value(),
            attribute: u16::max_value(),
            word_index: u16::max_value(),
            is_exact: true,
            char_index: u16::max_value(),
            char_length: u16::max_value(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Document {
    pub id: DocumentId,
    pub matches: Vec<Match>,
}

impl Document {
    fn from_raw(raw: &RawDocument) -> Document {
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

    pub fn word_index(&self) -> &[u16] {
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

    pub fn char_index(&self) -> &[u16] {
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
    word_index: Vec<u16>,
    is_exact: Vec<bool>,
    char_index: Vec<u16>,
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn docindex_mem_size() {
        assert_eq!(mem::size_of::<DocIndex>(), 16);
    }
}

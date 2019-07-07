#[cfg(test)]
#[macro_use] extern crate assert_matches;

mod automaton;
mod distinct_map;
mod query_builder;
mod query_enhancer;
mod reordered_attrs;
mod store;
pub mod criterion;

use std::fmt;
use std::sync::Arc;

use sdset::SetBuf;
use serde::{Serialize, Deserialize};
use slice_group_by::GroupBy;
use zerocopy::{AsBytes, FromBytes};

pub use self::query_builder::{QueryBuilder, DistinctQueryBuilder, normalize_str};
pub use self::store::Store;

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
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Highlight {
    /// The attribute in the document where the word was found
    /// along with the index in it.
    pub attribute: u16,

    /// The position in bytes where the word was found.
    ///
    /// It informs on the original word area in the text indexed
    /// without needing to run the tokenizer again.
    pub char_index: u16,

    /// The length in bytes of the found word.
    ///
    /// It informs on the original word area in the text indexed
    /// without needing to run the tokenizer again.
    pub char_length: u16,
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TmpMatch {
    pub query_index: u32,
    pub distance: u8,
    pub attribute: u16,
    pub word_index: u16,
    pub is_exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Document {
    pub id: DocumentId,
    pub highlights: Vec<Highlight>,

    #[cfg(test)]
    pub matches: Vec<TmpMatch>,
}

impl Document {
    #[cfg(not(test))]
    fn from_raw(raw: RawDocument) -> Document {
        Document { id: raw.id, highlights: raw.highlights }
    }

    #[cfg(test)]
    fn from_raw(raw: RawDocument) -> Document {
        let len = raw.query_index().len();
        let mut matches = Vec::with_capacity(len);

        let query_index = raw.query_index();
        let distance = raw.distance();
        let attribute = raw.attribute();
        let word_index = raw.word_index();
        let is_exact = raw.is_exact();

        for i in 0..len {
            let match_ = TmpMatch {
                query_index: query_index[i],
                distance: distance[i],
                attribute: attribute[i],
                word_index: word_index[i],
                is_exact: is_exact[i],
            };
            matches.push(match_);
        }

        Document { id: raw.id, matches, highlights: raw.highlights }
    }
}

#[derive(Clone)]
pub struct RawDocument {
    pub id: DocumentId,
    pub matches: SharedMatches,
    pub highlights: Vec<Highlight>,
}

impl RawDocument {
    fn new(id: DocumentId, matches: SharedMatches, highlights: Vec<Highlight>) -> RawDocument {
        RawDocument { id, matches, highlights }
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
}

impl fmt::Debug for RawDocument {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RawDocument")
            .field("id", &self.id)
            .field("query_index", &self.query_index())
            .field("distance", &self.distance())
            .field("attribute", &self.attribute())
            .field("word_index", &self.word_index())
            .field("is_exact", &self.is_exact())
            .finish()
    }
}

fn raw_documents_from_matches(matches: SetBuf<(DocumentId, TmpMatch, Highlight)>) -> Vec<RawDocument> {
    let mut docs_ranges: Vec<(_, Range, _)> = Vec::new();
    let mut matches2 = Matches::with_capacity(matches.len());

    for group in matches.linear_group_by(|(a, _, _), (b, _, _)| a == b) {
        let document_id = group[0].0;
        let start = docs_ranges.last().map(|(_, r, _)| r.end).unwrap_or(0);
        let end = start + group.len();

        let highlights = group.iter().map(|(_, _, h)| *h).collect();
        docs_ranges.push((document_id, Range { start, end }, highlights));

        matches2.extend_from_slice(group);
    }

    let matches = Arc::new(matches2);
    docs_ranges.into_iter().map(|(i, range, highlights)| {
        let matches = SharedMatches { range, matches: matches.clone() };
        RawDocument::new(i, matches, highlights)
    }).collect()
}

#[derive(Debug, Copy, Clone)]
struct Range {
    start: usize,
    end: usize,
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
}

impl Matches {
    fn with_capacity(cap: usize) -> Matches {
        Matches {
            query_index: Vec::with_capacity(cap),
            distance: Vec::with_capacity(cap),
            attribute: Vec::with_capacity(cap),
            word_index: Vec::with_capacity(cap),
            is_exact: Vec::with_capacity(cap),
        }
    }

    fn extend_from_slice(&mut self, matches: &[(DocumentId, TmpMatch, Highlight)]) {
        for (_, match_, _) in matches {
            self.query_index.push(match_.query_index);
            self.distance.push(match_.distance);
            self.attribute.push(match_.attribute);
            self.word_index.push(match_.word_index);
            self.is_exact.push(match_.is_exact);
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

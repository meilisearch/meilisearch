#[cfg(test)]
#[macro_use] extern crate assert_matches;

mod automaton;
mod distinct_map;
mod query_builder;
mod query_enhancer;
mod raw_document;
mod reordered_attrs;
mod store;
pub mod criterion;

use serde::{Serialize, Deserialize};
use zerocopy::{AsBytes, FromBytes};

use self::raw_document::raw_documents_from_matches;

pub use self::query_builder::{QueryBuilder, DistinctQueryBuilder, normalize_str};
pub use self::raw_document::RawDocument;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn docindex_mem_size() {
        assert_eq!(mem::size_of::<DocIndex>(), 16);
    }
}

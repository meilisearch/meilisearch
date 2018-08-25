#![feature(nll)]

extern crate fst;
extern crate fnv;
extern crate group_by;
extern crate levenshtein_automata;
extern crate byteorder;
extern crate rocksdb;

pub mod rank;
pub mod metadata;
pub mod levenshtein;

pub use self::metadata::{
    Metadata, MetadataBuilder,
    StreamWithState, StreamWithStateBuilder,
    UnionWithState, OpWithStateBuilder,
    IndexedValuesWithState,
};
pub use self::rank::{RankedStream};
pub use self::levenshtein::LevBuilder;

pub type DocumentId = u64;

/// This structure represent the position of a word
/// in a document and its attributes.
///
/// This is stored in the map, generated at index time,
/// extracted and interpreted at search time.
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct DocIndex {

    /// The document identifier where the word was found.
    pub document: DocumentId,

    /// The attribute identifier in the document
    /// where the word was found.
    ///
    /// This is an `u8` therefore a document
    /// can not have more than `2^8` attributes.
    pub attribute: u8,

    /// The index where the word was found in the attribute.
    ///
    /// Only the first 1000 words are indexed.
    pub attribute_index: u32,
}

/// This structure represent a matching word with informations
/// on the location of the word in the document.
///
/// The order of the field is important because it defines
/// the way these structures are ordered between themselves.
///
/// The word in itself is not important.
// TODO do data oriented programming ? very arrays ?
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Match {

    /// The word index in the query sentence.
    /// Same as the `attribute_index` but for the query words.
    ///
    /// Used to retrieve the automaton that match this word.
    pub query_index: u32,

    /// The distance the word has with the query word
    /// (i.e. the Levenshtein distance).
    pub distance: u8,

    /// The attribute in which the word is located
    /// (i.e. Title is 0, Description is 1).
    ///
    /// This is an `u8` therefore a document
    /// can not have more than `2^8` attributes.
    pub attribute: u8,

    /// Where does this word is located in the attribute string
    /// (i.e. at the start or the end of the attribute).
    ///
    /// The index in the attribute is limited to a maximum of `2^32`
    /// this is because we index only the first 1000 words
    /// in an attribute.
    pub attribute_index: u32,

    /// Whether the word that match is an exact match or a prefix.
    pub is_exact: bool,
}

impl Match {
    pub fn zero() -> Self {
        Match {
            query_index: 0,
            distance: 0,
            attribute: 0,
            attribute_index: 0,
            is_exact: false,
        }
    }

    pub fn max() -> Self {
        Match {
            query_index: u32::max_value(),
            distance: u8::max_value(),
            attribute: u8::max_value(),
            attribute_index: u32::max_value(),
            is_exact: true,
        }
    }
}

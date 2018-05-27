#[macro_use] extern crate serde_derive;
extern crate bincode;
extern crate fst;
extern crate levenshtein_automata;
extern crate serde;

pub mod map;
pub mod capped_btree_map;
mod levenshtein;

pub use self::map::{Map, MapBuilder, Values};
pub use self::map::{
    OpBuilder, IndexedValues,
    OpWithStateBuilder, IndexedValuesWithState,
};
pub use self::capped_btree_map::{CappedBTreeMap, Insertion};
pub use self::levenshtein::LevBuilder;

pub type DocIndexMap = Map<DocIndex>;
pub type DocIndexMapBuilder = MapBuilder<DocIndex>;

/// This structure represent the position of a word
/// in a document and its attributes.
///
/// This is stored in the map, generated at index time,
/// extracted and interpreted at search time.
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DocIndex {

    /// The document identifier where the word was found.
    pub document: u64,

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
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Match {

    /// The distance the word has with the query word
    /// (i.e. the Levenshtein distance).
    pub distance: u8,

    /// The attribute in which the word is located
    /// (i.e. Title is 0, Description is 1).
    ///
    /// This is an `u8` therefore a document
    /// can not have more than `2^8` attributes.
    pub attribute: u8,

    /// The word index in the query sentence.
    /// Same as the `attribute_index` but for the query words.
    ///
    /// Used to retrieve the automaton that match this word.
    pub query_index: u32,

    /// Where does this word is located in the attribute string
    /// (i.e. at the start or the end of the attribute).
    ///
    /// The index in the attribute is limited to a maximum of `2^32`
    /// this is because we index only the first 1000 words in an attribute.
    pub attribute_index: u32,
}

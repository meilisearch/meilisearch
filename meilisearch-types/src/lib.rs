#[cfg(feature = "zerocopy")]
use zerocopy::{AsBytes, FromBytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Represent an internally generated document unique identifier.
///
/// It is used to inform the database the document you want to deserialize.
/// Helpful for custom ranking.
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "zerocopy", derive(AsBytes, FromBytes))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(C)]
pub struct DocumentId(pub u64);

/// This structure represent the position of a word
/// in a document and its attributes.
///
/// This is stored in the map, generated at index time,
/// extracted and interpreted at search time.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "zerocopy", derive(AsBytes, FromBytes))]
#[repr(C)]
pub struct DocIndex {
    /// The document identifier where the word was found.
    pub document_id: DocumentId,

    /// The attribute in the document where the word was found
    /// along with the index in it.
    /// This is an IndexedPos and not a FieldId. Must be converted each time.
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
#[cfg_attr(feature = "zerocopy", derive(AsBytes, FromBytes))]
#[repr(C)]
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

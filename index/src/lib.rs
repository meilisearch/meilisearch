pub use milli;
pub use search::{
    all_documents, perform_search, retrieve_document, retrieve_documents, settings,
    MatchingStrategy, SearchQuery, SearchResult, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER,
    DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT,
};
pub use updates::{apply_settings_to_builder, Checked, Facets, Settings, Unchecked};

use serde_json::{Map, Value};

// mod dump;
pub mod error;
mod search;
pub mod updates;

pub type Document = Map<String, Value>;

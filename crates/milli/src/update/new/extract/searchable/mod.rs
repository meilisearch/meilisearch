mod extract_word_docids;
mod extract_word_pair_proximity_docids;
mod tokenize_document;

pub use extract_word_docids::{WordDocidsCaches, WordDocidsExtractors};
pub use extract_word_pair_proximity_docids::WordPairProximityDocidsExtractor;

use crate::attribute_patterns::{match_field_legacy, PatternMatch};

pub fn match_searchable_field(
    field_name: &str,
    searchable_fields: Option<&[&str]>,
) -> PatternMatch {
    let Some(searchable_fields) = searchable_fields else {
        // If no searchable fields are provided, consider all fields as searchable
        return PatternMatch::Match;
    };

    let mut selection = PatternMatch::NoMatch;
    for pattern in searchable_fields {
        match match_field_legacy(pattern, field_name) {
            PatternMatch::Match => return PatternMatch::Match,
            PatternMatch::Parent => selection = PatternMatch::Parent,
            PatternMatch::NoMatch => (),
        }
    }

    selection
}

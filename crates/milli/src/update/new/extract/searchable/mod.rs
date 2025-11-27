mod extract_word_docids;
mod extract_word_pair_proximity_docids;
mod tokenize_document;

pub use extract_word_docids::{
    SettingsChangeWordDocidsExtractors, WordDocidsCaches, WordDocidsExtractors,
};
pub use extract_word_pair_proximity_docids::{
    SettingsChangeWordPairProximityDocidsExtractors, WordPairProximityDocidsExtractor,
};

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

/// return `true` if the provided `field_name` is a parent of at least one of the fields contained in `searchable`,
/// or if `searchable` is `None`.
fn has_searchable_children<I, A>(field_name: &str, searchable: Option<I>) -> bool
where
    I: IntoIterator<Item = A>,
    A: AsRef<str>,
{
    searchable.is_none_or(|fields| {
        fields
            .into_iter()
            .any(|attr| match_field_legacy(attr.as_ref(), field_name) == PatternMatch::Parent)
    })
}

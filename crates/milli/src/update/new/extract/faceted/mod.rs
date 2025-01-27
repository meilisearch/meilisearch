mod extract_facets;
mod facet_document;

use std::collections::HashSet;

pub use extract_facets::FacetedDocidsExtractor;

use crate::{
    attribute_patterns::{match_field_legacy, PatternMatch},
    filterable_fields::match_pattern_by_features,
    FilterableAttributesSettings,
};

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum FacetKind {
    Number = 0,
    String = 1,
    Null = 2,
    Empty = 3,
    Exists,
}

impl From<u8> for FacetKind {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Number,
            1 => Self::String,
            2 => Self::Null,
            3 => Self::Empty,
            4 => Self::Exists,
            _ => unreachable!(),
        }
    }
}

impl FacetKind {
    pub fn extract_from_key(key: &[u8]) -> (FacetKind, &[u8]) {
        (FacetKind::from(key[0]), &key[1..])
    }
}

pub fn match_faceted_field(
    field_name: &str,
    filterable_fields: &[FilterableAttributesSettings],
    sortable_fields: &HashSet<String>,
) -> PatternMatch {
    // Check if the field matches any filterable or facet searchable field
    let mut selection = match_pattern_by_features(field_name, &filterable_fields, &|features| {
        features.is_facet_searchable() || features.is_filterable()
    });

    // If the field matches the pattern, return Match
    if selection == PatternMatch::Match {
        return selection;
    }

    // Otherwise, check if the field matches any sortable field
    for pattern in sortable_fields {
        match match_field_legacy(&pattern, field_name) {
            PatternMatch::Match => return PatternMatch::Match,
            PatternMatch::Parent => selection = PatternMatch::Parent,
            PatternMatch::NoMatch => (),
        }
    }

    selection
}

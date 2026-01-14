use deserr::Deserr;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::is_faceted_by;

/// A collection of patterns used to match attribute names. Patterns can
/// include wildcards (`*`) for flexible matching. For example, `title`
/// matches exactly, `overview_*` matches any attribute starting with
/// `overview_`, and `*_date` matches any attribute ending with `_date`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub struct AttributePatterns {
    /// An array of attribute name patterns. Each pattern can be an exact
    /// attribute name, or include wildcards (`*`) at the start, end, or
    /// both. Examples: `["title", "description_*", "*_date", "*content*"]`.
    #[schema(example = json!(["title", "overview_*", "release_date"]))]
    pub patterns: Vec<String>,
}

impl<E: deserr::DeserializeError> Deserr<E> for AttributePatterns {
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        Vec::<String>::deserialize_from_value(value, location).map(|patterns| Self { patterns })
    }
}

impl From<Vec<String>> for AttributePatterns {
    fn from(patterns: Vec<String>) -> Self {
        Self { patterns }
    }
}

impl AttributePatterns {
    /// Match a string against the attribute patterns using the
    /// match_pattern function.
    pub fn match_str(&self, str: &str) -> PatternMatch {
        let mut pattern_match = PatternMatch::NoMatch;
        for pattern in &self.patterns {
            match match_pattern(pattern, str) {
                PatternMatch::Match => return PatternMatch::Match,
                PatternMatch::Parent => pattern_match = PatternMatch::Parent,
                PatternMatch::NoMatch => {}
            }
        }
        pattern_match
    }
}

/// Match a string against a pattern.
///
/// The pattern can be a wildcard, a prefix, a suffix or an exact match.
///
/// # Arguments
///
/// * `pattern` - The pattern to match against.
/// * `str` - The string to match against the pattern.
pub fn match_pattern(pattern: &str, str: &str) -> PatternMatch {
    // If the pattern is a wildcard, return Match
    if pattern == "*" {
        return PatternMatch::Match;
    } else if pattern.starts_with('*') && pattern.ends_with('*') {
        // If the pattern starts and ends with a wildcard, return Match if the string contains the pattern without the wildcards
        if str.contains(&pattern[1..pattern.len() - 1]) {
            return PatternMatch::Match;
        }
    } else if let Some(pattern) = pattern.strip_prefix('*') {
        // If the pattern starts with a wildcard, return Match if the string ends with the pattern without the wildcard
        if str.ends_with(pattern) {
            return PatternMatch::Match;
        }
    } else if let Some(pattern) = pattern.strip_suffix('*') {
        // If the pattern ends with a wildcard, return Match if the string starts with the pattern without the wildcard
        if str.starts_with(pattern) {
            return PatternMatch::Match;
        }
    } else if pattern == str {
        // If the pattern is exactly the string, return Match
        return PatternMatch::Match;
    }

    // If the field is a parent field of the pattern, return Parent
    if is_faceted_by(pattern, str) {
        PatternMatch::Parent
    } else {
        PatternMatch::NoMatch
    }
}

/// Match a field against a pattern using the legacy behavior.
///
/// A field matches a pattern if it is a parent of the pattern or if it is
/// the pattern itself. This behavior is used to match the sortable
/// attributes, the searchable attributes and the filterable attributes
/// rules `Field`.
///
/// # Arguments
///
/// * `pattern` - The pattern to match against.
/// * `field` - The field to match against the pattern.
pub fn match_field_legacy(pattern: &str, field: &str) -> PatternMatch {
    if is_faceted_by(field, pattern) {
        // If the field matches the pattern or is a nested field of the pattern, return Match (legacy behavior)
        PatternMatch::Match
    } else if is_faceted_by(pattern, field) {
        // If the field is a parent field of the pattern, return Parent
        PatternMatch::Parent
    } else {
        // If the field does not match the pattern and is not a parent of a nested field that matches the pattern, return NoMatch
        PatternMatch::NoMatch
    }
}

/// Match a field against a distinct field.
pub fn match_distinct_field(distinct_field: Option<&str>, field: &str) -> PatternMatch {
    if let Some(distinct_field) = distinct_field {
        if field == distinct_field {
            // If the field matches exactly the distinct field, return Match
            return PatternMatch::Match;
        } else if is_faceted_by(distinct_field, field) {
            // If the field is a parent field of the distinct field, return Parent
            return PatternMatch::Parent;
        }
    }
    // If the field does not match the distinct field and is not a parent of a nested field that matches the distinct field, return NoMatch
    PatternMatch::NoMatch
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternMatch {
    /// The field is a parent of a nested field that matches the pattern
    /// For example, the field is `toto`, and the pattern is `toto.titi`
    Parent,
    /// The field matches the pattern
    Match,
    /// The field does not match the pattern
    NoMatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_pattern() {
        assert_eq!(match_pattern("*", "test"), PatternMatch::Match);
        assert_eq!(match_pattern("test*", "test"), PatternMatch::Match);
        assert_eq!(match_pattern("test*", "testa"), PatternMatch::Match);
        assert_eq!(match_pattern("*test", "test"), PatternMatch::Match);
        assert_eq!(match_pattern("*test", "atest"), PatternMatch::Match);
        assert_eq!(match_pattern("*test*", "test"), PatternMatch::Match);
        assert_eq!(match_pattern("*test*", "atesta"), PatternMatch::Match);
        assert_eq!(match_pattern("*test*", "atest"), PatternMatch::Match);
        assert_eq!(match_pattern("*test*", "testa"), PatternMatch::Match);
        assert_eq!(match_pattern("test*test", "test"), PatternMatch::NoMatch);
        assert_eq!(match_pattern("*test", "testa"), PatternMatch::NoMatch);
        assert_eq!(match_pattern("test*", "atest"), PatternMatch::NoMatch);
    }
}

use deserr::{DeserializeError, Deserr, ValuePointerRef};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use utoipa::ToSchema;

use crate::{
    attribute_patterns::{match_distinct_field, match_field_legacy, PatternMatch},
    constants::RESERVED_GEO_FIELD_NAME,
    AttributePatterns, FieldsIdsMap,
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, ToSchema)]
#[serde(untagged)]
pub enum FilterableAttributesRule {
    Field(String),
    Pattern(FilterableAttributesPatterns),
}

impl FilterableAttributesRule {
    /// Match a field against the filterable attributes rule.
    pub fn match_str(&self, field: &str) -> PatternMatch {
        match self {
            // If the rule is a field, match the field against the pattern using the legacy behavior
            FilterableAttributesRule::Field(pattern) => match_field_legacy(pattern, field),
            // If the rule is a pattern, match the field against the pattern using the new behavior
            FilterableAttributesRule::Pattern(patterns) => patterns.match_str(field),
        }
    }

    /// Check if the rule is a geo field.
    ///
    /// prefer using `index.is_geo_enabled`, `index.is_geo_filtering_enabled` or `index.is_geo_sorting_enabled`
    /// to check if the geo feature is enabled.
    pub fn has_geo(&self) -> bool {
        matches!(self, FilterableAttributesRule::Field(field_name) if field_name == RESERVED_GEO_FIELD_NAME)
    }

    /// Get the features of the rule.
    pub fn features(&self) -> FilterableAttributesFeatures {
        match self {
            // If the rule is a field, return the legacy default features
            FilterableAttributesRule::Field(_) => FilterableAttributesFeatures::legacy_default(),
            // If the rule is a pattern, return the features of the pattern
            FilterableAttributesRule::Pattern(patterns) => patterns.features(),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct FilterableAttributesPatterns {
    pub patterns: AttributePatterns,
    #[serde(default)]
    #[deserr(default)]
    pub features: FilterableAttributesFeatures,
}

impl FilterableAttributesPatterns {
    pub fn match_str(&self, field: &str) -> PatternMatch {
        self.patterns.match_str(field)
    }

    pub fn features(&self) -> FilterableAttributesFeatures {
        self.features.clone()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
#[derive(Default)]
pub struct FilterableAttributesFeatures {
    facet_search: bool,
    filter: FilterFeatures,
}

impl FilterableAttributesFeatures {
    /// Create a new `FilterableAttributesFeatures` with the legacy default features.
    ///
    /// This is the default behavior for `FilterableAttributesRule::Field`.
    /// This will set the facet search to true and activate all the filter operators.
    pub fn legacy_default() -> Self {
        Self { facet_search: true, filter: FilterFeatures::legacy_default() }
    }

    /// Create a new `FilterableAttributesFeatures` with no features.
    pub fn no_features() -> Self {
        Self { facet_search: false, filter: FilterFeatures::no_features() }
    }

    pub fn is_filterable(&self) -> bool {
        self.filter.is_filterable()
    }

    /// Check if `IS EMPTY` is allowed
    pub fn is_filterable_empty(&self) -> bool {
        self.filter.is_filterable_empty()
    }

    /// Check if `=` and `IN` are allowed
    pub fn is_filterable_equality(&self) -> bool {
        self.filter.is_filterable_equality()
    }

    /// Check if `IS NULL` is allowed
    pub fn is_filterable_null(&self) -> bool {
        self.filter.is_filterable_null()
    }

    /// Check if `IS EXISTS` is allowed
    pub fn is_filterable_exists(&self) -> bool {
        self.filter.is_filterable_exists()
    }

    /// Check if `<`, `>`, `<=`, `>=` or `TO` are allowed
    pub fn is_filterable_comparison(&self) -> bool {
        self.filter.is_filterable_comparison()
    }

    /// Check if the facet search is allowed
    pub fn is_facet_searchable(&self) -> bool {
        self.facet_search
    }

    pub fn allowed_filter_operators(&self) -> Vec<String> {
        self.filter.allowed_operators()
    }
}

impl<E: DeserializeError> Deserr<E> for FilterableAttributesRule {
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: ValuePointerRef,
    ) -> Result<Self, E> {
        if value.kind() == deserr::ValueKind::Map {
            Ok(Self::Pattern(FilterableAttributesPatterns::deserialize_from_value(
                value, location,
            )?))
        } else {
            Ok(Self::Field(String::deserialize_from_value(value, location)?))
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Deserr, ToSchema)]
pub struct FilterFeatures {
    equality: bool,
    comparison: bool,
}

impl FilterFeatures {
    /// Get the allowed operators for the filter.
    pub fn allowed_operators(&self) -> Vec<String> {
        if !self.is_filterable() {
            return vec![];
        }

        let mut operators = vec!["OR", "AND", "NOT"];
        if self.is_filterable_equality() {
            operators.extend_from_slice(&["=", "!=", "IN"]);
        }
        if self.is_filterable_comparison() {
            operators.extend_from_slice(&["<", ">", "<=", ">=", "TO"]);
        }
        if self.is_filterable_empty() {
            operators.push("IS EMPTY");
        }
        if self.is_filterable_null() {
            operators.push("IS NULL");
        }
        if self.is_filterable_exists() {
            operators.push("EXISTS");
        }

        operators.into_iter().map(String::from).collect()
    }

    pub fn is_filterable(&self) -> bool {
        self.equality || self.comparison
    }

    pub fn is_filterable_equality(&self) -> bool {
        self.equality
    }

    /// Check if `<`, `>`, `<=`, `>=` or `TO` are allowed
    pub fn is_filterable_comparison(&self) -> bool {
        self.comparison
    }

    /// Check if `IS EMPTY` is allowed
    pub fn is_filterable_empty(&self) -> bool {
        self.is_filterable()
    }

    /// Check if `IS EXISTS` is allowed
    pub fn is_filterable_exists(&self) -> bool {
        self.is_filterable()
    }

    /// Check if `IS NULL` is allowed
    pub fn is_filterable_null(&self) -> bool {
        self.is_filterable()
    }

    /// Create a new `FilterFeatures` with the legacy default features.
    ///
    /// This is the default behavior for `FilterableAttributesRule::Field`.
    /// This will set the equality and comparison to true.
    pub fn legacy_default() -> Self {
        Self { equality: true, comparison: true }
    }

    /// Create a new `FilterFeatures` with no features.
    pub fn no_features() -> Self {
        Self { equality: false, comparison: false }
    }
}

impl Default for FilterFeatures {
    fn default() -> Self {
        Self { equality: true, comparison: false }
    }
}

/// Match a field against a set of filterable attributes rules.
///
/// This function will return the set of field names that match the given filter.
///
/// # Arguments
///
/// * `filterable_attributes` - The set of filterable attributes rules to match against.
/// * `fields_ids_map` - The map of field names to field ids.
/// * `filter` - The filter function to apply to the filterable attributes rules.
pub fn filtered_matching_field_names<'fim>(
    filterable_attributes: &[FilterableAttributesRule],
    fields_ids_map: &'fim FieldsIdsMap,
    filter: &impl Fn(&FilterableAttributesFeatures) -> bool,
) -> BTreeSet<&'fim str> {
    let mut result = BTreeSet::new();
    for (_, field_name) in fields_ids_map.iter() {
        for filterable_attribute in filterable_attributes {
            if filterable_attribute.match_str(field_name) == PatternMatch::Match {
                let features = filterable_attribute.features();
                if filter(&features) {
                    result.insert(field_name);
                }
            }
        }
    }
    result
}

/// Match a field against a set of filterable attributes rules.
///
/// This function will return the features that match the given field name.
///
/// # Arguments
///
/// * `field_name` - The field name to match against.
/// * `filterable_attributes` - The set of filterable attributes rules to match against.
pub fn matching_features(
    field_name: &str,
    filterable_attributes: &[FilterableAttributesRule],
) -> Option<FilterableAttributesFeatures> {
    for filterable_attribute in filterable_attributes {
        if filterable_attribute.match_str(field_name) == PatternMatch::Match {
            return Some(filterable_attribute.features());
        }
    }
    None
}

/// Check if a field is filterable calling the method `FilterableAttributesFeatures::is_filterable()`.
///
/// # Arguments
///
/// * `field_name` - The field name to check.
/// * `filterable_attributes` - The set of filterable attributes rules to match against.
pub fn is_field_filterable(
    field_name: &str,
    filterable_attributes: &[FilterableAttributesRule],
) -> bool {
    matching_features(field_name, filterable_attributes)
        .map_or(false, |features| features.is_filterable())
}

/// Check if a field is facet searchable calling the method `FilterableAttributesFeatures::is_facet_searchable()`.
///
/// # Arguments
///
/// * `field_name` - The field name to check.
/// * `filterable_attributes` - The set of filterable attributes rules to match against.
pub fn is_field_facet_searchable(
    field_name: &str,
    filterable_attributes: &[FilterableAttributesRule],
) -> bool {
    matching_features(field_name, filterable_attributes)
        .map_or(false, |features| features.is_facet_searchable())
}

/// Match a field against a set of filterable, facet searchable fields, distinct field, sortable fields, and asc_desc fields.
pub fn match_faceted_field(
    field_name: &str,
    filterable_fields: &[FilterableAttributesRule],
    sortable_fields: &HashSet<String>,
    asc_desc_fields: &HashSet<String>,
    distinct_field: &Option<String>,
) -> PatternMatch {
    // Check if the field matches any filterable or facet searchable field
    let mut selection = match_pattern_by_features(field_name, filterable_fields, &|features| {
        features.is_facet_searchable() || features.is_filterable()
    });

    // If the field matches the pattern, return Match
    if selection == PatternMatch::Match {
        return selection;
    }

    match match_distinct_field(distinct_field.as_deref(), field_name) {
        PatternMatch::Match => return PatternMatch::Match,
        PatternMatch::Parent => selection = PatternMatch::Parent,
        PatternMatch::NoMatch => (),
    }

    // Otherwise, check if the field matches any sortable/asc_desc field
    for pattern in sortable_fields.iter().chain(asc_desc_fields.iter()) {
        match match_field_legacy(pattern, field_name) {
            PatternMatch::Match => return PatternMatch::Match,
            PatternMatch::Parent => selection = PatternMatch::Parent,
            PatternMatch::NoMatch => (),
        }
    }

    selection
}

fn match_pattern_by_features(
    field_name: &str,
    filterable_attributes: &[FilterableAttributesRule],
    filter: &impl Fn(&FilterableAttributesFeatures) -> bool,
) -> PatternMatch {
    let mut selection = PatternMatch::NoMatch;
    // Check if the field name matches any pattern that is facet searchable or filterable
    for pattern in filterable_attributes {
        let features = pattern.features();
        if filter(&features) {
            match pattern.match_str(field_name) {
                PatternMatch::Match => return PatternMatch::Match,
                PatternMatch::Parent => selection = PatternMatch::Parent,
                PatternMatch::NoMatch => (),
            }
        }
    }

    selection
}

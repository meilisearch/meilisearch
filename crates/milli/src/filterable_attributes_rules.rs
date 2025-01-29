use deserr::{DeserializeError, Deserr, ValuePointerRef};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use utoipa::ToSchema;

use crate::{
    attribute_patterns::{match_field_legacy, PatternMatch},
    constants::RESERVED_GEO_FIELD_NAME,
    AttributePatterns, FieldId, FieldsIdsMap,
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, ToSchema)]
#[serde(untagged)]
pub enum FilterableAttributesRule {
    Field(String),
    Pattern(FilterableAttributesPatterns),
}

impl FilterableAttributesRule {
    pub fn match_str(&self, field: &str) -> PatternMatch {
        match self {
            FilterableAttributesRule::Field(pattern) => match_field_legacy(pattern, field),
            FilterableAttributesRule::Pattern(patterns) => patterns.match_str(field),
        }
    }

    pub fn has_geo(&self) -> bool {
        matches!(self, FilterableAttributesRule::Field(field_name) if field_name == RESERVED_GEO_FIELD_NAME)
    }

    pub fn features(&self) -> FilterableAttributesFeatures {
        match self {
            FilterableAttributesRule::Field(_) => FilterableAttributesFeatures::legacy_default(),
            FilterableAttributesRule::Pattern(patterns) => patterns.features(),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct FilterableAttributesPatterns {
    pub patterns: AttributePatterns,
    pub features: Option<FilterableAttributesFeatures>,
}

impl FilterableAttributesPatterns {
    pub fn match_str(&self, field: &str) -> PatternMatch {
        self.patterns.match_str(field)
    }

    pub fn features(&self) -> FilterableAttributesFeatures {
        self.features.clone().unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct FilterableAttributesFeatures {
    facet_search: bool,
    filter: FilterFeature,
}

impl Default for FilterableAttributesFeatures {
    fn default() -> Self {
        Self { facet_search: false, filter: FilterFeature::Equal }
    }
}

impl FilterableAttributesFeatures {
    pub fn legacy_default() -> Self {
        Self { facet_search: true, filter: FilterFeature::Order }
    }

    pub fn no_features() -> Self {
        Self { facet_search: false, filter: FilterFeature::Disabled }
    }

    pub fn is_filterable(&self) -> bool {
        self.filter != FilterFeature::Disabled
    }

    /// Check if `IS NULL` is allowed
    pub fn is_filterable_null(&self) -> bool {
        self.filter != FilterFeature::Disabled
    }

    /// Check if `IS EMPTY` is allowed
    pub fn is_filterable_empty(&self) -> bool {
        self.filter != FilterFeature::Disabled
    }

    /// Check if `IS EXISTS` is allowed
    pub fn is_filterable_exists(&self) -> bool {
        self.filter != FilterFeature::Disabled
    }

    /// Check if `<`, `>`, `<=`, `>=` or `TO` are allowed
    pub fn is_filterable_order(&self) -> bool {
        self.filter == FilterFeature::Order
    }

    /// Check if the facet search is allowed
    pub fn is_facet_searchable(&self) -> bool {
        self.facet_search
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
pub enum FilterFeature {
    Disabled,
    Equal,
    Order,
}

pub fn matching_field_ids(
    filterable_attributes: &[FilterableAttributesRule],
    fields_ids_map: &FieldsIdsMap,
) -> HashSet<FieldId> {
    let mut result = HashSet::new();
    for (field_id, field_name) in fields_ids_map.iter() {
        for filterable_attribute in filterable_attributes {
            if filterable_attribute.match_str(field_name) == PatternMatch::Match {
                result.insert(field_id);
            }
        }
    }
    result
}

pub fn matching_field_names<'fim>(
    filterable_attributes: &[FilterableAttributesRule],
    fields_ids_map: &'fim FieldsIdsMap,
) -> BTreeSet<&'fim str> {
    filtered_matching_field_names(filterable_attributes, fields_ids_map, &|_| true)
}

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

pub fn is_field_filterable(
    field_name: &str,
    filterable_attributes: &[FilterableAttributesRule],
) -> bool {
    matching_features(field_name, filterable_attributes)
        .map_or(false, |features| features.is_filterable())
}

pub fn match_pattern_by_features(
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

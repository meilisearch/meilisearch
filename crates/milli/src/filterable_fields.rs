use deserr::{DeserializeError, Deserr, ValuePointerRef};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use utoipa::ToSchema;

use crate::{
    constants::RESERVED_GEO_FIELD_NAME, is_faceted_by, AttributePatterns, FieldId, FieldsIdsMap,
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, ToSchema)]
#[serde(untagged)]
pub enum FilterableAttributesSettings {
    Field(String),
    Pattern(FilterableAttributesPatterns),
}

impl FilterableAttributesSettings {
    pub fn match_str(&self, field: &str) -> bool {
        match self {
            FilterableAttributesSettings::Field(field_name) => is_faceted_by(field, field_name),
            FilterableAttributesSettings::Pattern(patterns) => patterns.patterns.match_str(field),
        }
    }

    pub fn has_geo(&self) -> bool {
        /// TODO: This is a temporary solution to check if the geo field is activated.
        matches!(self, FilterableAttributesSettings::Field(field_name) if field_name == RESERVED_GEO_FIELD_NAME)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct FilterableAttributesPatterns {
    pub patterns: AttributePatterns,
    pub features: Option<FilterableAttributesFeatures>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct FilterableAttributesFeatures {
    facet_search: Option<String>,
    filter: Option<String>,
}

impl<E: DeserializeError> Deserr<E> for FilterableAttributesSettings {
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

pub fn matching_field_ids(
    filterable_attributes: &[FilterableAttributesSettings],
    fields_ids_map: &FieldsIdsMap,
) -> HashSet<FieldId> {
    let mut result = HashSet::new();
    for (field_id, field_name) in fields_ids_map.iter() {
        for filterable_attribute in filterable_attributes {
            if filterable_attribute.match_str(field_name) {
                result.insert(field_id);
            }
        }
    }
    result
}

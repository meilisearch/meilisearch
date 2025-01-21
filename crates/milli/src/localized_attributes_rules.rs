use std::collections::HashMap;

use charabia::Language;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::fields_ids_map::FieldsIdsMap;
use crate::FieldId;

/// A rule that defines which locales are supported for a given attribute.
///
/// The rule is a list of attribute patterns and a list of locales.
/// The attribute patterns are matched against the attribute name.
/// The pattern `*` matches any attribute name.
/// The pattern `attribute_name*` matches any attribute name that starts with `attribute_name`.
/// The pattern `*attribute_name` matches any attribute name that ends with `attribute_name`.
/// The pattern `*attribute_name*` matches any attribute name that contains `attribute_name`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LocalizedAttributesRule {
    pub attribute_patterns: Vec<String>,
    #[schema(value_type = Vec<String>)]
    pub locales: Vec<Language>,
}

impl LocalizedAttributesRule {
    pub fn new(attribute_patterns: Vec<String>, locales: Vec<Language>) -> Self {
        Self { attribute_patterns, locales }
    }

    pub fn match_str(&self, str: &str) -> bool {
        self.attribute_patterns.iter().any(|pattern| match_pattern(pattern.as_str(), str))
    }

    pub fn locales(&self) -> &[Language] {
        &self.locales
    }
}

fn match_pattern(pattern: &str, str: &str) -> bool {
    if pattern == "*" {
        true
    } else if pattern.starts_with('*') && pattern.ends_with('*') {
        str.contains(&pattern[1..pattern.len() - 1])
    } else if let Some(pattern) = pattern.strip_prefix('*') {
        str.ends_with(pattern)
    } else if let Some(pattern) = pattern.strip_suffix('*') {
        str.starts_with(pattern)
    } else {
        pattern == str
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalizedFieldIds {
    field_id_to_locales: HashMap<FieldId, Vec<Language>>,
}

impl LocalizedFieldIds {
    pub fn new<I: Iterator<Item = FieldId>>(
        rules: &Option<Vec<LocalizedAttributesRule>>,
        fields_ids_map: &FieldsIdsMap,
        fields_ids: I,
    ) -> Self {
        let mut field_id_to_locales = HashMap::new();

        if let Some(rules) = rules {
            let fields = fields_ids.filter_map(|field_id| {
                fields_ids_map.name(field_id).map(|field_name| (field_id, field_name))
            });

            for (field_id, field_name) in fields {
                let mut locales = Vec::new();
                for rule in rules {
                    if rule.match_str(field_name) {
                        locales.extend(rule.locales.iter());
                        // Take the first rule that matches
                        break;
                    }
                }

                if !locales.is_empty() {
                    locales.sort();
                    locales.dedup();
                    field_id_to_locales.insert(field_id, locales);
                }
            }
        }

        Self { field_id_to_locales }
    }

    pub fn locales(&self, fields_id: FieldId) -> Option<&[Language]> {
        self.field_id_to_locales.get(&fields_id).map(Vec::as_slice)
    }

    pub fn all_locales(&self) -> Vec<Language> {
        let mut locales = Vec::new();
        for field_locales in self.field_id_to_locales.values() {
            if !field_locales.is_empty() {
                locales.extend(field_locales);
            } else {
                // If a field has no locales, we consider it as not localized
                return Vec::new();
            }
        }
        locales.sort();
        locales.dedup();
        locales
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_pattern() {
        assert!(match_pattern("*", "test"));
        assert!(match_pattern("test*", "test"));
        assert!(match_pattern("test*", "testa"));
        assert!(match_pattern("*test", "test"));
        assert!(match_pattern("*test", "atest"));
        assert!(match_pattern("*test*", "test"));
        assert!(match_pattern("*test*", "atesta"));
        assert!(match_pattern("*test*", "atest"));
        assert!(match_pattern("*test*", "testa"));
        assert!(!match_pattern("test*test", "test"));
        assert!(!match_pattern("*test", "testa"));
        assert!(!match_pattern("test*", "atest"));
    }
}

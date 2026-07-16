use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroU16;

use charabia::Language;
use heed::RoTxn;

use super::FieldsIdsMap;
use crate::attribute_patterns::{
    field_match_any_patterns_legacy, match_distinct_field, match_field_legacy, PatternMatch,
};
use crate::constants::{
    RESERVED_GEOJSON_FIELD_NAME, RESERVED_GEO_FIELD_NAME, RESERVED_VECTORS_FIELD_NAME,
};
use crate::order_by_map::OrderByMap;
use crate::{
    is_faceted_by, Criterion, FieldId, FilterableAttributesFeatures, FilterableAttributesRule,
    Index, LocalizedAttributesRule, OrderBy, Result, Weight,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Metadata {
    /// The weight as defined in the FieldidsWeightsMap of the searchable attribute if it is searchable.
    pub searchable: (PatternMatch, Option<Weight>),
    /// The field is part of the exact attributes.
    pub exact: PatternMatch,
    /// The field is part of the sortable attributes.
    pub sortable: PatternMatch,
    /// The field is defined as the distinct attribute.
    pub distinct: PatternMatch,
    /// The field has been defined as asc/desc in the ranking rules.
    pub asc_desc: (PatternMatch, Option<FieldSortOrder>),
    /// The field is a geo field (`_geo`, `_geo.lat`, `_geo.lng`).
    pub geo: PatternMatch,
    /// The field is a geo json field (`_geojson`).
    pub geo_json: PatternMatch,
    /// The field is defined as a field that can be displayed.
    pub displayed: PatternMatch,
    /// The id of the localized attributes rule if the field is localized.
    pub localized_attributes_rule_id: Option<NonZeroU16>,
    /// The id of the filterable attributes rule if the field is filterable.
    pub filterable_attributes_rule_id: (PatternMatch, Option<NonZeroU16>),
    /// How that field will be sorted by.
    pub sort_by: OrderBy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldSortOrder {
    Asc,
    Desc,
}

impl std::fmt::Display for FieldSortOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldSortOrder::Asc => write!(f, "asc"),
            FieldSortOrder::Desc => write!(f, "desc"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldIdMapWithMetadata {
    fields_ids_map: FieldsIdsMap,
    builder: MetadataBuilder,
    metadata: BTreeMap<FieldId, Metadata>,
}

impl FieldIdMapWithMetadata {
    pub fn new(existing_fields_ids_map: FieldsIdsMap, builder: MetadataBuilder) -> Self {
        let metadata = existing_fields_ids_map
            .iter()
            .map(|(id, name)| (id, builder.metadata_for_field(name)))
            .collect();
        Self { fields_ids_map: existing_fields_ids_map, builder, metadata }
    }

    pub fn empty() -> Self {
        Self {
            fields_ids_map: Default::default(),
            builder: MetadataBuilder::empty(),
            metadata: Default::default(),
        }
    }

    pub fn as_fields_ids_map(&self) -> &FieldsIdsMap {
        &self.fields_ids_map
    }

    /// Returns the number of fields ids in the map.
    pub fn len(&self) -> usize {
        self.fields_ids_map.len()
    }

    /// Returns `true` if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.fields_ids_map.is_empty()
    }

    /// Returns the field id related to a field name, it will create a new field id if the
    /// name is not already known. Returns `None` if the maximum field id as been reached.
    pub fn insert(&mut self, name: &str) -> Option<FieldId> {
        let id = self.fields_ids_map.insert(name)?;
        self.metadata.insert(id, self.builder.metadata_for_field(name));
        Some(id)
    }

    /// Get the id of a field based on its name.
    pub fn id(&self, name: &str) -> Option<FieldId> {
        self.fields_ids_map.id(name)
    }

    pub fn id_with_metadata(&self, name: &str) -> Option<(FieldId, Metadata)> {
        let id = self.fields_ids_map.id(name)?;
        Some((id, self.metadata(id).unwrap()))
    }

    /// Get the name of a field based on its id.
    pub fn name(&self, id: FieldId) -> Option<&str> {
        self.fields_ids_map.name(id)
    }

    /// Get the name of a field based on its id.
    pub fn name_with_metadata(&self, id: FieldId) -> Option<(&str, Metadata)> {
        let name = self.fields_ids_map.name(id)?;
        Some((name, self.metadata(id).unwrap()))
    }

    pub fn metadata(&self, id: FieldId) -> Option<Metadata> {
        self.metadata.get(&id).copied()
    }

    /// Iterate over the ids and names in the ids order.
    pub fn iter(&self) -> impl Iterator<Item = (FieldId, &str, Metadata)> {
        self.fields_ids_map.iter().map(|(id, name)| (id, name, self.metadata(id).unwrap()))
    }

    pub fn iter_id_metadata(&self) -> impl Iterator<Item = (FieldId, Metadata)> + '_ {
        self.metadata.iter().map(|(k, v)| (*k, *v))
    }

    pub fn iter_metadata(&self) -> impl Iterator<Item = Metadata> + '_ {
        self.metadata.values().copied()
    }

    pub fn metadata_builder(&self) -> &MetadataBuilder {
        &self.builder
    }
}

impl Metadata {
    pub fn locales<'rules>(
        &self,
        rules: &'rules [LocalizedAttributesRule],
    ) -> Option<&'rules [Language]> {
        self.localized_attributes_rule_with_index(rules).map(|(_, rule)| rule.locales())
    }

    fn localized_attributes_rule_with_index<'rules>(
        &self,
        rules: &'rules [LocalizedAttributesRule],
    ) -> Option<(usize, &'rules LocalizedAttributesRule)> {
        let localized_attributes_rule_id = self.localized_attributes_rule_id?.get();
        // - 1: `localized_attributes_rule_id` is NonZero
        let rule = rules.get((localized_attributes_rule_id - 1) as usize).unwrap();
        Some((localized_attributes_rule_id.into(), rule))
    }

    pub fn filterable_attributes<'rules>(
        &self,
        rules: &'rules [FilterableAttributesRule],
    ) -> Option<&'rules FilterableAttributesRule> {
        self.filterable_attributes_with_rule_index(rules).map(|(_, rule)| rule)
    }

    pub fn filterable_attributes_with_rule_index<'rules>(
        &self,
        rules: &'rules [FilterableAttributesRule],
    ) -> Option<(usize, &'rules FilterableAttributesRule)> {
        let filterable_attributes_rule_id = self.filterable_attributes_rule_id.1?.get();
        let rule_id = (filterable_attributes_rule_id - 1) as usize;
        let rule = rules.get(rule_id).unwrap();
        Some((rule_id, rule))
    }

    pub fn filterable_attributes_features(
        &self,
        rules: &[FilterableAttributesRule],
    ) -> FilterableAttributesFeatures {
        let (_, features) = self.filterable_attributes_features_with_rule_index(rules);
        features
    }

    pub fn filterable_attributes_features_with_rule_index(
        &self,
        rules: &[FilterableAttributesRule],
    ) -> (Option<usize>, FilterableAttributesFeatures) {
        self.filterable_attributes_with_rule_index(rules)
            .map(|(rule_index, rule)| (Some(rule_index), rule.features()))
            // if there is no filterable attributes rule, return no features
            .unwrap_or_else(|| (None, FilterableAttributesFeatures::no_features()))
    }

    pub fn is_sortable(&self) -> PatternMatch {
        self.sortable
    }

    pub fn is_searchable(&self) -> PatternMatch {
        let (pattern_match, _) = self.searchable;

        pattern_match
    }

    pub fn searchable_weight(&self) -> Option<Weight> {
        let (_, weight) = self.searchable;

        weight
    }

    pub fn is_distinct(&self) -> PatternMatch {
        self.distinct
    }

    pub fn is_asc_desc(&self) -> PatternMatch {
        let (pattern_match, _) = self.asc_desc;

        pattern_match
    }

    pub fn is_geo(&self) -> PatternMatch {
        self.geo
    }

    pub fn is_geo_enabled(&self) -> bool {
        self.geo == PatternMatch::Match
            && (self.sortable == PatternMatch::Match
                || self.filterable_attributes_rule_id.0 == PatternMatch::Match)
    }

    pub fn is_geojson_enabled(&self) -> bool {
        self.geo_json == PatternMatch::Match
            && self.filterable_attributes_rule_id.0 == PatternMatch::Match
    }

    /// Returns the pattern match if the field is part of the facet databases. (sortable, distinct, asc_desc, filterable or facet searchable)
    pub fn is_faceted(&self, rules: &[FilterableAttributesRule]) -> PatternMatch {
        let mut pattern_match = PatternMatch::NoMatch;
        match self.distinct {
            PatternMatch::Match => return PatternMatch::Match,
            PatternMatch::Parent => pattern_match = PatternMatch::Parent,
            PatternMatch::NoMatch => (),
        }
        match self.sortable {
            PatternMatch::Match => return PatternMatch::Match,
            PatternMatch::Parent => pattern_match = PatternMatch::Parent,
            PatternMatch::NoMatch => (),
        }
        match self.asc_desc.0 {
            PatternMatch::Match => return PatternMatch::Match,
            PatternMatch::Parent => pattern_match = PatternMatch::Parent,
            PatternMatch::NoMatch => (),
        }
        match self.filterable_attributes_rule_id {
            (PatternMatch::Match, _) => {
                let features = self.filterable_attributes_features(rules);
                if features.is_filterable() || features.is_facet_searchable() {
                    return PatternMatch::Match;
                }
                // If the field is not filterable or facet searchable,
                // it may be a parent field of a filterable or facet searchable field.
                // This case can happen when a field is added to the filterable attributes with every features deactivated.
                pattern_match = PatternMatch::Parent;
            }
            (PatternMatch::Parent, _) => pattern_match = PatternMatch::Parent,
            (PatternMatch::NoMatch, _) => (),
        }

        pattern_match
    }

    pub fn require_facet_level_database(&self, rules: &[FilterableAttributesRule]) -> bool {
        let features = self.filterable_attributes_features(rules);

        self.is_sortable() == PatternMatch::Match
            || self.is_asc_desc() == PatternMatch::Match
            || features.is_filterable_comparison()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataBuilder {
    searchable_attributes: Option<Vec<String>>,
    exact_searchable_attributes: Vec<String>,
    filterable_attributes: Vec<FilterableAttributesRule>,
    sortable_attributes: HashSet<String>,
    localized_attributes: Option<Vec<LocalizedAttributesRule>>,
    distinct_attribute: Option<String>,
    asc_desc_attributes: HashMap<String, FieldSortOrder>,
    displayed_attributes: Option<HashSet<String>>,
    order_by_map: OrderByMap,
}

impl MetadataBuilder {
    pub fn from_index(index: &Index, rtxn: &RoTxn) -> Result<Self> {
        let searchable_attributes = index
            .user_defined_searchable_fields(rtxn)?
            .map(|fields| fields.into_iter().map(String::from).collect());
        let exact_searchable_attributes =
            index.exact_attributes(rtxn)?.into_iter().map(String::from).collect();
        let filterable_attributes = index.filterable_attributes_rules(rtxn)?;
        let sortable_attributes = index.sortable_fields(rtxn)?;
        let localized_attributes = index.localized_attributes_rules(rtxn)?;
        let distinct_attribute = index.distinct_field(rtxn)?.map(String::from);
        let asc_desc_attributes = index
            .criteria(rtxn)?
            .into_iter()
            .filter_map(|criterion| match criterion {
                Criterion::Asc(field) => Some((field, FieldSortOrder::Asc)),
                Criterion::Desc(field) => Some((field, FieldSortOrder::Desc)),
                _otherwise => None,
            })
            .collect();

        let displayed_attributes = index
            .displayed_fields(rtxn)?
            .map(|fields| fields.into_iter().map(String::from).collect());

        Ok(Self {
            searchable_attributes,
            exact_searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            localized_attributes,
            distinct_attribute,
            asc_desc_attributes,
            displayed_attributes,
            order_by_map: index.sort_facet_values_by(rtxn)?,
        })
    }

    /// Build a new `MetadataBuilder` from the given parameters.
    ///
    /// This is used for testing, prefer using `MetadataBuilder::from_index` instead.
    pub fn new(
        searchable_attributes: Option<Vec<String>>,
        exact_searchable_attributes: Vec<String>,
        filterable_attributes: Vec<FilterableAttributesRule>,
        sortable_attributes: HashSet<String>,
        localized_attributes: Option<Vec<LocalizedAttributesRule>>,
        distinct_attribute: Option<String>,
        asc_desc_attributes: HashMap<String, FieldSortOrder>,
    ) -> Self {
        let searchable_attributes = match searchable_attributes {
            Some(fields) if fields.iter().any(|f| f == "*") => None,
            Some(fields) => Some(fields),
            None => None,
        };

        Self {
            searchable_attributes,
            exact_searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            localized_attributes,
            distinct_attribute,
            asc_desc_attributes,
            displayed_attributes: None,
            order_by_map: OrderByMap::default(),
        }
    }

    pub fn empty() -> Self {
        Self {
            searchable_attributes: None,
            exact_searchable_attributes: Default::default(),
            filterable_attributes: Default::default(),
            sortable_attributes: Default::default(),
            localized_attributes: None,
            distinct_attribute: None,
            asc_desc_attributes: Default::default(),
            displayed_attributes: None,
            order_by_map: OrderByMap::default(),
        }
    }

    /// Computes the full metadata for a field
    ///
    /// Use when you need the full metadata, otherwise consider using one of the intermediate functions
    pub fn metadata_for_field(&self, field: &str) -> Metadata {
        if let Some(metadata) = self.has_reserved_field_metadata(field) {
            return metadata;
        }

        let localized_attributes_rule_id =
            self.localized_rule_with_index_not_reserved(field).map(|(id, _)| {
                NonZeroU16::new(id.saturating_add(1).try_into().unwrap())
                    // saturating_add(1): make `id` `NonZero`
                    .unwrap()
            });

        Metadata {
            searchable: self.is_searchable_not_reserved(field),
            exact: self.is_exact_not_reserved(field),
            sortable: self.is_sortable_not_reserved(field),
            distinct: self.is_distinct_not_reserved(field),
            asc_desc: self.is_asc_desc_not_reserved(field),
            geo: PatternMatch::NoMatch,
            geo_json: PatternMatch::NoMatch,
            localized_attributes_rule_id,
            filterable_attributes_rule_id: self.filterable_attribute_rules_not_reserved(field),
            displayed: self.is_displayed(field),
            sort_by: self.order_by_map.get(field),
        }
    }

    // Partial metadata

    /// `Some` if the field is faceted by the reserved vector field name, otherwise `None`
    pub fn has_vector_metadata(&self, field: &str) -> Option<Metadata> {
        // Vectors fields are not searchable, filterable, distinct or asc_desc
        is_faceted_by(field, RESERVED_VECTORS_FIELD_NAME).then_some(
            // Vectors fields are not searchable, filterable, distinct or asc_desc
            Metadata {
                searchable: (PatternMatch::NoMatch, None),
                exact: PatternMatch::NoMatch,
                sortable: PatternMatch::NoMatch,
                distinct: PatternMatch::NoMatch,
                asc_desc: (PatternMatch::NoMatch, None),
                geo: PatternMatch::NoMatch,
                geo_json: PatternMatch::NoMatch,
                localized_attributes_rule_id: None,
                filterable_attributes_rule_id: (PatternMatch::NoMatch, None),
                displayed: self.is_displayed(field),
                sort_by: OrderBy::default(),
            },
        )
    }

    /// `Some` if the field is matching legacy the reserved geo field name, otherwise `None`
    pub fn has_geo_metadata(&self, field: &str) -> Option<Metadata> {
        (match_field_legacy(RESERVED_GEO_FIELD_NAME, field) == PatternMatch::Match).then_some(
            // Geo fields are not searchable, distinct or asc_desc
            Metadata {
                searchable: (PatternMatch::NoMatch, None),
                exact: PatternMatch::NoMatch,
                sortable: self.is_sortable_not_reserved(field),
                distinct: PatternMatch::NoMatch,
                asc_desc: (PatternMatch::NoMatch, None),
                geo: PatternMatch::Match,
                geo_json: PatternMatch::NoMatch,
                localized_attributes_rule_id: None,
                filterable_attributes_rule_id: self.filterable_attribute_rules_not_reserved(field),
                displayed: self.is_displayed(field),
                sort_by: self.order_by_map.get(field),
            },
        )
    }

    /// `Some` if the field is matching legacy the reserved geojson field name, otherwise `None`
    pub fn has_geo_json_metadata(&self, field: &str) -> Option<Metadata> {
        (match_field_legacy(RESERVED_GEOJSON_FIELD_NAME, field) == PatternMatch::Match).then_some(
            Metadata {
                searchable: (PatternMatch::NoMatch, None),
                exact: PatternMatch::NoMatch,
                // geojson field should not be sortable
                sortable: PatternMatch::NoMatch,
                distinct: PatternMatch::NoMatch,
                asc_desc: (PatternMatch::NoMatch, None),
                geo: PatternMatch::NoMatch,
                geo_json: PatternMatch::Match,
                localized_attributes_rule_id: None,
                filterable_attributes_rule_id: self.filterable_attribute_rules_not_reserved(field),
                displayed: self.is_displayed(field),
                sort_by: self.order_by_map.get(field),
            },
        )
    }

    /// `Some` if the field is any of the reserved fields with special metadata, `None` otherwise.
    pub fn has_reserved_field_metadata(&self, field: &str) -> Option<Metadata> {
        self.has_vector_metadata(field)
            .or_else(|| self.has_geo_metadata(field))
            .or_else(|| self.has_geo_json_metadata(field))
    }

    /// Whether the field is a displayable attribute.
    pub fn is_displayed(&self, field: &str) -> PatternMatch {
        match self.displayed_attributes.as_ref() {
            Some(attrs) => field_match_any_patterns_legacy(attrs, field),
            None => PatternMatch::Match,
        }
    }

    /// Whether the field is a searchable attribute.
    pub fn is_searchable(&self, field: &str) -> (PatternMatch, Option<Weight>) {
        if let Some(Metadata { searchable, .. }) = self.has_reserved_field_metadata(field) {
            searchable
        } else {
            self.is_searchable_not_reserved(field)
        }
    }

    /// Whether the field is an exact attribute.
    pub fn is_exact(&self, field: &str) -> PatternMatch {
        if let Some(Metadata { exact, .. }) = self.has_reserved_field_metadata(field) {
            exact
        } else {
            self.is_exact_not_reserved(field)
        }
    }

    /// Whether the field is a sortable attribute.
    pub fn is_sortable(&self, field: &str) -> PatternMatch {
        if let Some(Metadata { sortable, .. }) = self.has_reserved_field_metadata(field) {
            sortable
        } else {
            self.is_sortable_not_reserved(field)
        }
    }

    /// Whether the field is the distinct field.
    pub fn is_distinct(&self, field: &str) -> PatternMatch {
        if let Some(Metadata { distinct, .. }) = self.has_reserved_field_metadata(field) {
            distinct
        } else {
            self.is_distinct_not_reserved(field)
        }
    }

    /// Whether the field matches any filterable rule, and if so the matching rule and its index.
    pub fn filterable_rule_with_index<'rules>(
        &'rules self,
        field: &str,
    ) -> (PatternMatch, Option<(usize, &'rules FilterableAttributesRule)>) {
        if let Some(metadata) = self.has_reserved_field_metadata(field) {
            (
                metadata.filterable_attributes_rule_id.0,
                metadata
                    .filterable_attributes_with_rule_index(self.filterable_attributes.as_slice()),
            )
        } else {
            self.filterable_rule_with_index_not_reserved(field)
        }
    }

    /// Whether the field matches any localized rule, and if so the matching rule and its index.
    pub fn localized_rule_with_index<'rules>(
        &'rules self,
        field: &str,
    ) -> Option<(usize, &'rules LocalizedAttributesRule)> {
        let localized_attributes_rules = self.localized_attributes.as_deref()?;

        if let Some(metadata) = self.has_reserved_field_metadata(field) {
            metadata.localized_attributes_rule_with_index(localized_attributes_rules)
        } else {
            self.localized_rule_with_index_not_reserved(field)
        }
    }

    /// Whether the field matches any asc desc custom rule, and if so the field sort order.
    pub fn is_asc_desc(&self, field: &str) -> (PatternMatch, Option<FieldSortOrder>) {
        if let Some(Metadata { asc_desc, .. }) = self.has_reserved_field_metadata(field) {
            asc_desc
        } else {
            self.is_asc_desc_not_reserved(field)
        }
    }

    // partial metadata implementation
    // the following functions assume that callers already checked whether the field is a reserved field with a special metadata

    fn is_searchable_not_reserved(&self, field: &str) -> (PatternMatch, Option<Weight>) {
        match &self.searchable_attributes {
            // A field is searchable if it is faceted by a searchable attribute
            Some(attributes) => {
                let mut matching_searchable = PatternMatch::NoMatch;
                let weight =
                    attributes.iter().enumerate().find_map(
                        |(i, pattern)| match match_field_legacy(pattern, field) {
                            PatternMatch::Match => {
                                matching_searchable = PatternMatch::Match;
                                Some(i as u16)
                            }
                            PatternMatch::Parent => {
                                matching_searchable = PatternMatch::Parent;
                                None
                            }
                            PatternMatch::NoMatch => None,
                        },
                    );
                (matching_searchable, weight)
            }
            None => (PatternMatch::Match, Some(0)),
        }
    }

    fn is_exact_not_reserved(&self, field: &str) -> PatternMatch {
        field_match_any_patterns_legacy(&self.exact_searchable_attributes, field)
    }

    fn is_sortable_not_reserved(&self, field: &str) -> PatternMatch {
        // A field is sortable if it is faceted by a sortable attribute
        field_match_any_patterns_legacy(&self.sortable_attributes, field)
    }

    fn is_distinct_not_reserved(&self, field: &str) -> PatternMatch {
        match_distinct_field(self.distinct_attribute.as_deref(), field)
    }

    fn filterable_rule_with_index_not_reserved<'rules>(
        &'rules self,
        field: &str,
    ) -> (PatternMatch, Option<(usize, &'rules FilterableAttributesRule)>) {
        let mut matching_filterable = PatternMatch::NoMatch;
        let filterable_attributes_rule_id =
            self.filterable_attributes.iter().enumerate().find(|(_rule_id, attribute)| {
                match attribute.match_str(field) {
                    PatternMatch::Match => {
                        matching_filterable = PatternMatch::Match;
                        true
                    }
                    PatternMatch::Parent => {
                        matching_filterable = PatternMatch::Parent;
                        false
                    }
                    PatternMatch::NoMatch => false,
                }
            });
        (matching_filterable, filterable_attributes_rule_id)
    }

    fn filterable_attribute_rules_not_reserved(
        &self,
        field: &str,
    ) -> (PatternMatch, Option<NonZeroU16>) {
        let (matching_filterable, filterable_attributes_rule) =
            self.filterable_rule_with_index_not_reserved(field);
        let filterable_attributes_rule_id = filterable_attributes_rule.map(|(rule_id, _)| {
            NonZeroU16::new(rule_id.saturating_add(1).try_into().unwrap())
                // saturating_add(1): make `id` `NonZero`
                .unwrap()
        });
        (matching_filterable, filterable_attributes_rule_id)
    }

    fn localized_rule_with_index_not_reserved<'rules>(
        &'rules self,
        field: &str,
    ) -> Option<(usize, &'rules LocalizedAttributesRule)> {
        self.localized_attributes
            .iter()
            .flat_map(|v| v.iter())
            .enumerate()
            .find(|(_rule_id, rule)| rule.match_str(field) == PatternMatch::Match)
    }

    fn is_asc_desc_not_reserved(&self, field: &str) -> (PatternMatch, Option<FieldSortOrder>) {
        match field_match_any_patterns_legacy(self.asc_desc_attributes.keys(), field) {
            PatternMatch::Match => {
                // If the field matches an asc_desc attribute, return the order
                match self.asc_desc_attributes.get(field) {
                    Some(&order) => (PatternMatch::Match, Some(order)),
                    None => (PatternMatch::NoMatch, None),
                }
            }
            PatternMatch::Parent => (PatternMatch::Parent, None),
            PatternMatch::NoMatch => (PatternMatch::NoMatch, None),
        }
    }

    // Accessors

    pub fn searchable_attributes(&self) -> Option<&[String]> {
        self.searchable_attributes.as_deref()
    }

    pub fn sortable_attributes(&self) -> &HashSet<String> {
        &self.sortable_attributes
    }

    pub fn filterable_attributes(&self) -> &[FilterableAttributesRule] {
        &self.filterable_attributes
    }

    pub fn localized_attributes_rules(&self) -> Option<&[LocalizedAttributesRule]> {
        self.localized_attributes.as_deref()
    }
}

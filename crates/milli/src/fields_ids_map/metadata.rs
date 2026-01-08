use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU16;

use charabia::Language;
use heed::RoTxn;

use super::FieldsIdsMap;
use crate::attribute_patterns::{match_field_legacy, PatternMatch};
use crate::constants::{
    RESERVED_GEOJSON_FIELD_NAME, RESERVED_GEO_FIELD_NAME, RESERVED_VECTORS_FIELD_NAME,
};
use crate::order_by_map::OrderByMap;
use crate::{
    is_faceted_by, FieldId, FilterableAttributesFeatures, FilterableAttributesRule, Index,
    LocalizedAttributesRule, OrderBy, Result, Weight,
};

#[derive(Debug, Clone, Copy)]
pub struct Metadata {
    /// The weight as defined in the FieldidsWeightsMap of the searchable attribute if it is searchable.
    pub searchable: Option<Weight>,
    /// The field is part of the exact attributes.
    pub exact: bool,
    /// The field is part of the sortable attributes.
    pub sortable: bool,
    /// The field is defined as the distinct attribute.
    pub distinct: bool,
    /// The field has been defined as asc/desc in the ranking rules.
    pub asc_desc: bool,
    /// The field is a geo field (`_geo`, `_geo.lat`, `_geo.lng`).
    pub geo: bool,
    /// The field is a geo json field (`_geojson`).
    pub geo_json: bool,
    /// The field is defined as a field that can be displayed.
    pub displayed: bool,
    /// The id of the localized attributes rule if the field is localized.
    pub localized_attributes_rule_id: Option<NonZeroU16>,
    /// The id of the filterable attributes rule if the field is filterable.
    pub filterable_attributes_rule_id: Option<NonZeroU16>,
    /// How that field will be sorted by.
    pub sort_by: OrderBy,
}

#[derive(Debug, Clone)]
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
        let localized_attributes_rule_id = self.localized_attributes_rule_id?.get();
        // - 1: `localized_attributes_rule_id` is NonZero
        let rule = rules.get((localized_attributes_rule_id - 1) as usize).unwrap();
        Some(rule.locales())
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
        let filterable_attributes_rule_id = self.filterable_attributes_rule_id?.get();
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

    pub fn is_sortable(&self) -> bool {
        self.sortable
    }

    pub fn is_searchable(&self) -> bool {
        self.searchable.is_some()
    }

    pub fn searchable_weight(&self) -> Option<Weight> {
        self.searchable
    }

    pub fn is_distinct(&self) -> bool {
        self.distinct
    }

    pub fn is_asc_desc(&self) -> bool {
        self.asc_desc
    }

    pub fn is_geo(&self) -> bool {
        self.geo
    }

    /// Returns `true` if the field is part of the facet databases. (sortable, distinct, asc_desc, filterable or facet searchable)
    pub fn is_faceted(&self, rules: &[FilterableAttributesRule]) -> bool {
        if self.is_distinct() || self.is_sortable() || self.is_asc_desc() {
            return true;
        }

        let features = self.filterable_attributes_features(rules);
        if features.is_filterable() || features.is_facet_searchable() {
            return true;
        }

        false
    }

    pub fn require_facet_level_database(&self, rules: &[FilterableAttributesRule]) -> bool {
        let features = self.filterable_attributes_features(rules);

        self.is_sortable() || self.is_asc_desc() || features.is_filterable_comparison()
    }
}

#[derive(Debug, Clone)]
pub struct MetadataBuilder {
    searchable_attributes: Option<Vec<String>>,
    exact_searchable_attributes: Vec<String>,
    filterable_attributes: Vec<FilterableAttributesRule>,
    sortable_attributes: HashSet<String>,
    localized_attributes: Option<Vec<LocalizedAttributesRule>>,
    distinct_attribute: Option<String>,
    asc_desc_attributes: HashSet<String>,
    displayed_attributes: Option<HashSet<String>>,
    fields_metadata: BTreeMap<String, Metadata>,
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
        let asc_desc_attributes = index.asc_desc_fields(rtxn)?;

        let displayed_attributes = index
            .displayed_fields(rtxn)?
            .map(|fields| fields.into_iter().map(String::from).collect());

        let mut this = Self {
            searchable_attributes,
            exact_searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            localized_attributes,
            distinct_attribute,
            asc_desc_attributes,
            displayed_attributes,
            fields_metadata: BTreeMap::default(),
            order_by_map: index.sort_facet_values_by(rtxn)?,
        };

        for field_name in index.fields_ids_map(rtxn)?.names() {
            this.fields_metadata.insert(field_name.to_owned(), this.metadata_for_field(field_name));
        }

        Ok(this)
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
        asc_desc_attributes: HashSet<String>,
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
            fields_metadata: BTreeMap::default(),
            order_by_map: OrderByMap::default(),
        }
    }

    pub fn metadata_for_field(&self, field: &str) -> Metadata {
        if is_faceted_by(field, RESERVED_VECTORS_FIELD_NAME) {
            // Vectors fields are not searchable, filterable, distinct or asc_desc
            return Metadata {
                searchable: None,
                exact: false,
                sortable: false,
                distinct: false,
                asc_desc: false,
                geo: false,
                geo_json: false,
                localized_attributes_rule_id: None,
                filterable_attributes_rule_id: None,
                displayed: self.is_field_displayed(field),
                sort_by: OrderBy::default(),
            };
        }

        // A field is sortable if it is faceted by a sortable attribute
        let sortable = self
            .sortable_attributes
            .iter()
            .any(|pattern| match_field_legacy(pattern, field) == PatternMatch::Match);

        let filterable_attributes_rule_id = self
            .filterable_attributes
            .iter()
            .position(|attribute| attribute.match_str(field) == PatternMatch::Match)
            // saturating_add(1): make `id` `NonZero`
            .map(|id| NonZeroU16::new(id.saturating_add(1).try_into().unwrap()).unwrap());

        if match_field_legacy(RESERVED_GEO_FIELD_NAME, field) == PatternMatch::Match {
            // Geo fields are not searchable, distinct or asc_desc
            return Metadata {
                searchable: None,
                exact: false,
                sortable,
                distinct: false,
                asc_desc: false,
                geo: true,
                geo_json: false,
                localized_attributes_rule_id: None,
                filterable_attributes_rule_id,
                displayed: self.is_field_displayed(field),
                sort_by: self.order_by_map.get(field),
            };
        }
        if match_field_legacy(RESERVED_GEOJSON_FIELD_NAME, field) == PatternMatch::Match {
            debug_assert!(!sortable, "geojson fields should not be sortable");
            return Metadata {
                searchable: None,
                exact: false,
                sortable,
                distinct: false,
                asc_desc: false,
                geo: false,
                geo_json: true,
                localized_attributes_rule_id: None,
                filterable_attributes_rule_id,
                displayed: self.is_field_displayed(field),
                sort_by: self.order_by_map.get(field),
            };
        }

        let searchable = match &self.searchable_attributes {
            // A field is searchable if it is faceted by a searchable attribute
            Some(attributes) => attributes
                .iter()
                .enumerate()
                .find(|(_i, pattern)| is_faceted_by(field, pattern))
                .map(|(i, _)| i as u16),
            None => Some(0),
        };

        let exact = self.exact_searchable_attributes.iter().any(|attr| is_faceted_by(field, attr));

        let distinct =
            self.distinct_attribute.as_ref().is_some_and(|distinct_field| field == distinct_field);
        let asc_desc = self.asc_desc_attributes.contains(field);

        let localized_attributes_rule_id = self
            .localized_attributes
            .iter()
            .flat_map(|v| v.iter())
            .position(|rule| rule.match_str(field) == PatternMatch::Match)
            // saturating_add(1): make `id` `NonZero`
            .map(|id| NonZeroU16::new(id.saturating_add(1).try_into().unwrap()).unwrap());

        Metadata {
            searchable,
            exact,
            sortable,
            distinct,
            asc_desc,
            geo: false,
            geo_json: false,
            localized_attributes_rule_id,
            filterable_attributes_rule_id,
            displayed: self.is_field_displayed(field),
            sort_by: self.order_by_map.get(field),
        }
    }

    fn is_field_displayed(&self, field: &str) -> bool {
        self.displayed_attributes.as_ref().map(|attrs| attrs.contains(field)).unwrap_or(true)
    }

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

    pub fn fields_metadata(&self) -> &BTreeMap<String, Metadata> {
        &self.fields_metadata
    }
}

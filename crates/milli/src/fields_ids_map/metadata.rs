use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU16;

use charabia::Language;
use heed::RoTxn;

use super::FieldsIdsMap;
use crate::{FieldId, Index, LocalizedAttributesRule, Result};

#[derive(Debug, Clone, Copy)]
pub struct Metadata {
    pub searchable: bool,
    pub filterable: bool,
    pub sortable: bool,
    localized_attributes_rule_id: Option<NonZeroU16>,
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
}

#[derive(Debug, Clone)]
pub struct MetadataBuilder {
    searchable_attributes: Vec<String>,
    filterable_attributes: HashSet<String>,
    sortable_attributes: HashSet<String>,
    localized_attributes: Option<Vec<LocalizedAttributesRule>>,
}

impl MetadataBuilder {
    pub fn from_index(index: &Index, rtxn: &RoTxn) -> Result<Self> {
        let searchable_attributes =
            index.searchable_fields(rtxn)?.into_iter().map(|s| s.to_string()).collect();
        let filterable_attributes = index.filterable_fields(rtxn)?;
        let sortable_attributes = index.sortable_fields(rtxn)?;
        let localized_attributes = index.localized_attributes_rules(rtxn)?;

        Ok(Self {
            searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            localized_attributes,
        })
    }

    pub fn new(
        searchable_attributes: Vec<String>,
        filterable_attributes: HashSet<String>,
        sortable_attributes: HashSet<String>,
        localized_attributes: Option<Vec<LocalizedAttributesRule>>,
    ) -> Self {
        Self {
            searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            localized_attributes,
        }
    }

    pub fn metadata_for_field(&self, field: &str) -> Metadata {
        let searchable = self
            .searchable_attributes
            .iter()
            .any(|attribute| attribute == "*" || attribute == field);

        let filterable = self.filterable_attributes.contains(field);

        let sortable = self.sortable_attributes.contains(field);

        let localized_attributes_rule_id = self
            .localized_attributes
            .iter()
            .flat_map(|v| v.iter())
            .position(|rule| rule.match_str(field))
            // saturating_add(1): make `id` `NonZero`
            .map(|id| NonZeroU16::new(id.saturating_add(1).try_into().unwrap()).unwrap());

        Metadata { searchable, filterable, sortable, localized_attributes_rule_id }
    }

    pub fn searchable_attributes(&self) -> &[String] {
        self.searchable_attributes.as_slice()
    }

    pub fn sortable_attributes(&self) -> &HashSet<String> {
        &self.sortable_attributes
    }

    pub fn filterable_attributes(&self) -> &HashSet<String> {
        &self.filterable_attributes
    }

    pub fn localized_attributes_rules(&self) -> Option<&[LocalizedAttributesRule]> {
        self.localized_attributes.as_deref()
    }
}

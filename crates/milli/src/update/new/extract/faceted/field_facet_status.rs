use crate::fields_ids_map::metadata::{FieldIdMapWithMetadata, Metadata};
use crate::{FieldId, FilterFeatures, FilterableAttributesFeatures, FilterableAttributesRule};

/// A summary of the facet-related capabilities of a field.
///
/// This is the "complex check" that needs to be evaluated for both the
/// document-side facet extraction and the database-side facet deletion.
/// Keeping this logic in a single place avoids forgetting a part of the
/// strategy when a new facet capability is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldFacetStatus {
    pub facet_search: bool,
    pub equality: bool,
    pub comparison: bool,
    pub asc_desc: bool,
    pub sortable: bool,
    pub distinct: bool,
}

impl FieldFacetStatus {
    /// A status where the field has no facet-related capability at all.
    ///
    /// This is the equivalent of the field being completely missing from
    /// the fields ids map.
    pub const NONE: Self = Self {
        facet_search: false,
        equality: false,
        comparison: false,
        asc_desc: false,
        sortable: false,
        distinct: false,
    };

    /// Compute the [`FieldFacetStatus`] for a field given its [`Metadata`]
    /// and the corresponding filterable attributes rules.
    pub fn from_metadata(metadata: &Metadata, rules: &[FilterableAttributesRule]) -> Self {
        let FilterableAttributesFeatures { facet_search, filter } =
            metadata.filterable_attributes_features(rules);
        let FilterFeatures { equality, comparison } = filter;
        Self {
            facet_search,
            equality,
            comparison,
            asc_desc: metadata.asc_desc.is_some(),
            sortable: metadata.sortable,
            distinct: metadata.distinct,
        }
    }

    /// Compute the [`FieldFacetStatus`] for a field id in the given map,
    /// returning [`Self::NONE`] when the field is absent.
    pub fn from_field_id(
        fields_ids_map: &FieldIdMapWithMetadata,
        rules: &[FilterableAttributesRule],
        field_id: FieldId,
    ) -> Self {
        match fields_ids_map.metadata(field_id) {
            Some(metadata) => Self::from_metadata(&metadata, rules),
            None => Self::NONE,
        }
    }

    /// Whether this field is involved in any facet-related datastructure
    /// (filterable, facet-searchable, sortable, asc/desc or distinct).
    pub fn is_faceted(&self) -> bool {
        self.equality
            || self.comparison
            || self.facet_search
            || self.asc_desc
            || self.sortable
            || self.distinct
    }

    /// Whether this field is involved in the comparison datastructures
    /// (sortable, asc/desc or comparison filter).
    pub fn is_comparison(&self) -> bool {
        self.sortable || self.asc_desc || self.comparison
    }
}

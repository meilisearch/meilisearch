use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Display;
use std::ops::ControlFlow;
use std::{fmt, mem};

use heed::types::Bytes;
use heed::BytesDecode;
use indexmap::IndexMap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::attribute_patterns::match_field_legacy;
use crate::facet::FacetType;
use crate::filterable_attributes_rules::{filtered_matching_patterns, matching_features};
use crate::heed_codec::facet::{
    FacetGroupKeyCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec, OrderedF64Codec,
};
use crate::heed_codec::{BytesRefCodec, StrRefCodec};
use crate::search::facet::facet_distribution_iter::{
    count_iterate_over_facet_distribution, lexicographically_iterate_over_facet_distribution,
};
use crate::{Error, FieldId, FilterableAttributesRule, Index, PatternMatch, Result, UserError};

/// The default number of values by facets that will
/// be fetched from the key-value store.
pub const DEFAULT_VALUES_PER_FACET: usize = 100;

/// Threshold on the number of candidates that will make
/// the system to choose between one algorithm or another.
const CANDIDATES_THRESHOLD: u64 = 3000;

/// How should we fetch the facets?
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderBy {
    /// By lexicographic order...
    #[default]
    Lexicographic,
    /// Or by number of docids in common?
    Count,
}

impl Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderBy::Lexicographic => f.write_str("alphabetically"),
            OrderBy::Count => f.write_str("by count"),
        }
    }
}

pub struct FacetDistribution<'a> {
    facets: Option<HashMap<String, OrderBy>>,
    candidates: Option<RoaringBitmap>,
    max_values_per_facet: usize,
    default_order_by: OrderBy,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> FacetDistribution<'a> {
    pub fn new(rtxn: &'a heed::RoTxn<'a>, index: &'a Index) -> FacetDistribution<'a> {
        FacetDistribution {
            facets: None,
            candidates: None,
            max_values_per_facet: DEFAULT_VALUES_PER_FACET,
            default_order_by: OrderBy::default(),
            rtxn,
            index,
        }
    }

    pub fn facets<I: IntoIterator<Item = (A, OrderBy)>, A: AsRef<str>>(
        &mut self,
        names_ordered_by: I,
    ) -> &mut Self {
        self.facets = Some(
            names_ordered_by
                .into_iter()
                .map(|(name, order_by)| (name.as_ref().to_string(), order_by))
                .collect(),
        );
        self
    }

    pub fn max_values_per_facet(&mut self, max: usize) -> &mut Self {
        self.max_values_per_facet = max;
        self
    }

    pub fn default_order_by(&mut self, order_by: OrderBy) -> &mut Self {
        self.default_order_by = order_by;
        self
    }

    pub fn candidates(&mut self, candidates: RoaringBitmap) -> &mut Self {
        self.candidates = Some(candidates);
        self
    }

    /// There is a small amount of candidates OR we ask for facet string values so we
    /// decide to iterate over the facet values of each one of them, one by one.
    fn facet_distribution_from_documents(
        &self,
        field_id: FieldId,
        facet_type: FacetType,
        candidates: &RoaringBitmap,
        distribution: &mut IndexMap<String, u64>,
    ) -> heed::Result<()> {
        match facet_type {
            FacetType::Number => {
                let mut lexicographic_distribution = BTreeMap::new();
                let mut key_buffer: Vec<_> = field_id.to_be_bytes().to_vec();

                let db = self.index.field_id_docid_facet_f64s;
                for docid in candidates {
                    key_buffer.truncate(mem::size_of::<FieldId>());
                    key_buffer.extend_from_slice(&docid.to_be_bytes());
                    let iter = db
                        .remap_key_type::<Bytes>()
                        .prefix_iter(self.rtxn, &key_buffer)?
                        .remap_key_type::<FieldDocIdFacetF64Codec>();

                    for result in iter {
                        let ((_, _, value), ()) = result?;
                        *lexicographic_distribution.entry(value.to_string()).or_insert(0) += 1;
                    }
                }

                distribution.extend(
                    lexicographic_distribution
                        .into_iter()
                        .take(self.max_values_per_facet.saturating_sub(distribution.len())),
                );
            }
            FacetType::String => {
                let mut normalized_distribution = BTreeMap::new();
                let mut key_buffer: Vec<_> = field_id.to_be_bytes().to_vec();

                let db = self.index.field_id_docid_facet_strings;
                for docid in candidates {
                    key_buffer.truncate(mem::size_of::<FieldId>());
                    key_buffer.extend_from_slice(&docid.to_be_bytes());
                    let iter = db
                        .remap_key_type::<Bytes>()
                        .prefix_iter(self.rtxn, &key_buffer)?
                        .remap_key_type::<FieldDocIdFacetStringCodec>();

                    for result in iter {
                        let ((_, _, normalized_value), original_value) = result?;
                        let (_, count) = normalized_distribution
                            .entry(normalized_value)
                            .or_insert_with(|| (original_value, 0));
                        *count += 1;

                        // we'd like to break here if we have enough facet values, but we are collecting them by increasing docid,
                        // so higher ranked facets could be in later docids
                    }
                }

                let iter = normalized_distribution
                    .into_iter()
                    .take(self.max_values_per_facet.saturating_sub(distribution.len()))
                    .map(|(_normalized, (original, count))| (original.to_string(), count));
                distribution.extend(iter);
            }
        }

        Ok(())
    }

    /// There is too much documents, we use the facet levels to move throught
    /// the facet values, to find the candidates and values associated.
    fn facet_numbers_distribution_from_facet_levels(
        &self,
        field_id: FieldId,
        candidates: &RoaringBitmap,
        order_by: OrderBy,
        distribution: &mut IndexMap<String, u64>,
    ) -> heed::Result<()> {
        let search_function = match order_by {
            OrderBy::Lexicographic => lexicographically_iterate_over_facet_distribution,
            OrderBy::Count => count_iterate_over_facet_distribution,
        };

        search_function(
            self.rtxn,
            self.index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>(),
            field_id,
            candidates,
            |facet_key, nbr_docids, _| {
                let facet_key = OrderedF64Codec::bytes_decode(facet_key).unwrap();
                distribution.insert(facet_key.to_string(), nbr_docids);
                if distribution.len() == self.max_values_per_facet {
                    Ok(ControlFlow::Break(()))
                } else {
                    Ok(ControlFlow::Continue(()))
                }
            },
        )
    }

    fn facet_strings_distribution_from_facet_levels(
        &self,
        field_id: FieldId,
        candidates: &RoaringBitmap,
        order_by: OrderBy,
        distribution: &mut IndexMap<String, u64>,
    ) -> heed::Result<()> {
        let search_function = match order_by {
            OrderBy::Lexicographic => lexicographically_iterate_over_facet_distribution,
            OrderBy::Count => count_iterate_over_facet_distribution,
        };

        search_function(
            self.rtxn,
            self.index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>(),
            field_id,
            candidates,
            |facet_key, nbr_docids, any_docid| {
                let facet_key = StrRefCodec::bytes_decode(facet_key).unwrap();

                let key: (FieldId, _, &str) = (field_id, any_docid, facet_key);
                let optional_original_string =
                    self.index.field_id_docid_facet_strings.get(self.rtxn, &key)?;

                let original_string = match optional_original_string {
                    Some(original_string) => original_string.to_owned(),
                    None => {
                        tracing::error!(
                            "Missing original facet string. Using the normalized facet {} instead",
                            facet_key
                        );
                        facet_key.to_string()
                    }
                };

                distribution.insert(original_string, nbr_docids);
                if distribution.len() == self.max_values_per_facet {
                    Ok(ControlFlow::Break(()))
                } else {
                    Ok(ControlFlow::Continue(()))
                }
            },
        )
    }

    fn facet_values(
        &self,
        field_id: FieldId,
        order_by: OrderBy,
    ) -> heed::Result<IndexMap<String, u64>> {
        use FacetType::{Number, String};

        let mut distribution = IndexMap::new();
        match (order_by, &self.candidates) {
            (OrderBy::Lexicographic, Some(cnd)) if cnd.len() <= CANDIDATES_THRESHOLD => {
                // Classic search, candidates were specified, we must return facet values only related
                // to those candidates. We also enter here for facet strings for performance reasons.
                self.facet_distribution_from_documents(field_id, Number, cnd, &mut distribution)?;
                self.facet_distribution_from_documents(field_id, String, cnd, &mut distribution)?;
            }
            _ => {
                let universe;
                let candidates = match &self.candidates {
                    Some(cnd) => cnd,
                    None => {
                        universe = self.index.documents_ids(self.rtxn)?;
                        &universe
                    }
                };

                self.facet_numbers_distribution_from_facet_levels(
                    field_id,
                    candidates,
                    order_by,
                    &mut distribution,
                )?;
                self.facet_strings_distribution_from_facet_levels(
                    field_id,
                    candidates,
                    order_by,
                    &mut distribution,
                )?;
            }
        };

        Ok(distribution)
    }

    pub fn compute_stats(&self) -> Result<BTreeMap<String, (f64, f64)>> {
        let candidates = if let Some(candidates) = self.candidates.clone() {
            candidates
        } else {
            return Ok(Default::default());
        };

        let fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let filterable_attributes_rules = self.index.filterable_attributes_rules(self.rtxn)?;
        self.check_faceted_fields(&filterable_attributes_rules)?;

        let mut distribution = BTreeMap::new();
        for (fid, name) in fields_ids_map.iter() {
            if self.select_field(name, &filterable_attributes_rules) {
                let min_value = if let Some(min_value) = crate::search::facet::facet_min_value(
                    self.index,
                    self.rtxn,
                    fid,
                    candidates.clone(),
                )? {
                    min_value
                } else {
                    continue;
                };
                let max_value = if let Some(max_value) = crate::search::facet::facet_max_value(
                    self.index,
                    self.rtxn,
                    fid,
                    candidates.clone(),
                )? {
                    max_value
                } else {
                    continue;
                };

                distribution.insert(name.to_string(), (min_value, max_value));
            }
        }

        Ok(distribution)
    }

    pub fn execute(&self) -> Result<BTreeMap<String, IndexMap<String, u64>>> {
        let fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let filterable_attributes_rules = self.index.filterable_attributes_rules(self.rtxn)?;
        self.check_faceted_fields(&filterable_attributes_rules)?;

        let mut distribution = BTreeMap::new();
        for (fid, name) in fields_ids_map.iter() {
            if self.select_field(name, &filterable_attributes_rules) {
                let order_by = self
                    .facets
                    .as_ref()
                    .and_then(|facets| facets.get(name).copied())
                    .unwrap_or(self.default_order_by);
                let values = self.facet_values(fid, order_by)?;
                distribution.insert(name.to_string(), values);
            }
        }

        Ok(distribution)
    }

    /// Select a field if it is filterable and in the facets.
    fn select_field(
        &self,
        name: &str,
        filterable_attributes_rules: &[FilterableAttributesRule],
    ) -> bool {
        // If the field is not filterable, we don't want to compute the facet distribution.
        if !matching_features(name, filterable_attributes_rules)
            .is_some_and(|(_, features)| features.is_filterable())
        {
            return false;
        }

        match &self.facets {
            Some(facets) => {
                // The list of facets provided by the user is a legacy pattern ("dog.age" must be selected with "dog").
                facets.keys().any(|key| match_field_legacy(key, name) == PatternMatch::Match)
            }
            None => true,
        }
    }

    /// Check if the fields in the facets are valid filterable fields.
    fn check_faceted_fields(
        &self,
        filterable_attributes_rules: &[FilterableAttributesRule],
    ) -> Result<()> {
        let mut invalid_facets = BTreeSet::new();
        let mut matching_rule_indices = HashMap::new();

        if let Some(facets) = &self.facets {
            for field in facets.keys() {
                let matched_rule = matching_features(field, filterable_attributes_rules);
                let is_filterable = matched_rule.is_some_and(|(_, f)| f.is_filterable());

                if !is_filterable {
                    invalid_facets.insert(field.to_string());

                    // If the field matched a rule but that rule doesn't enable filtering,
                    // store the rule index for better error messages
                    if let Some((rule_index, _)) = matched_rule {
                        matching_rule_indices.insert(field.to_string(), rule_index);
                    }
                }
            }
        }

        if !invalid_facets.is_empty() {
            let valid_patterns =
                filtered_matching_patterns(filterable_attributes_rules, &|features| {
                    features.is_filterable()
                })
                .into_iter()
                .map(String::from)
                .collect();
            return Err(Error::UserError(UserError::InvalidFacetsDistribution {
                invalid_facets_name: invalid_facets,
                valid_patterns,
                matching_rule_indices,
            }));
        }

        Ok(())
    }
}

impl fmt::Debug for FacetDistribution<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let FacetDistribution {
            facets,
            candidates,
            max_values_per_facet,
            default_order_by,
            rtxn: _,
            index: _,
        } = self;

        f.debug_struct("FacetDistribution")
            .field("facets", facets)
            .field("candidates", candidates)
            .field("max_values_per_facet", max_values_per_facet)
            .field("default_order_by", default_order_by)
            .finish()
    }
}

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::ops::ControlFlow;
use std::{fmt, mem};

use heed::types::Bytes;
use heed::BytesDecode;
use indexmap::IndexMap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::error::UserError;
use crate::facet::FacetType;
use crate::heed_codec::facet::{
    FacetGroupKeyCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec, OrderedF64Codec,
};
use crate::heed_codec::{BytesRefCodec, StrRefCodec};
use crate::search::facet::facet_distribution_iter::{
    count_iterate_over_facet_distribution, lexicographically_iterate_over_facet_distribution,
};
use crate::{FieldId, Index, Result};

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
        let fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let filterable_fields = self.index.filterable_fields(self.rtxn)?;
        let candidates = if let Some(candidates) = self.candidates.clone() {
            candidates
        } else {
            return Ok(Default::default());
        };

        let fields = match &self.facets {
            Some(facets) => {
                let invalid_fields: HashSet<_> = facets
                    .iter()
                    .map(|(name, _)| name)
                    .filter(|facet| !crate::is_faceted(facet, &filterable_fields))
                    .collect();
                if !invalid_fields.is_empty() {
                    return Err(UserError::InvalidFacetsDistribution {
                        invalid_facets_name: invalid_fields.into_iter().cloned().collect(),
                        valid_facets_name: filterable_fields.into_iter().collect(),
                    }
                    .into());
                } else {
                    facets.iter().map(|(name, _)| name).cloned().collect()
                }
            }
            None => filterable_fields,
        };

        let mut distribution = BTreeMap::new();
        for (fid, name) in fields_ids_map.iter() {
            if crate::is_faceted(name, &fields) {
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
        let filterable_fields = self.index.filterable_fields(self.rtxn)?;

        let fields = match self.facets {
            Some(ref facets) => {
                let invalid_fields: HashSet<_> = facets
                    .iter()
                    .map(|(name, _)| name)
                    .filter(|facet| !crate::is_faceted(facet, &filterable_fields))
                    .collect();
                if !invalid_fields.is_empty() {
                    return Err(UserError::InvalidFacetsDistribution {
                        invalid_facets_name: invalid_fields.into_iter().cloned().collect(),
                        valid_facets_name: filterable_fields.into_iter().collect(),
                    }
                    .into());
                } else {
                    facets.iter().map(|(name, _)| name).cloned().collect()
                }
            }
            None => filterable_fields,
        };

        let mut distribution = BTreeMap::new();
        for (fid, name) in fields_ids_map.iter() {
            if crate::is_faceted(name, &fields) {
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

#[cfg(test)]
mod tests {
    use std::iter;

    use big_s::S;
    use maplit::hashset;

    use crate::documents::mmap_from_objects;
    use crate::index::tests::TempIndex;
    use crate::{milli_snap, FacetDistribution, OrderBy};

    #[test]
    fn few_candidates_few_facet_values() {
        // All the tests here avoid using the code in `facet_distribution_iter` because there aren't
        // enough candidates.

        let index = TempIndex::new();

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let documents = documents!([
            { "id": 0, "colour": "Blue" },
            { "id": 1, "colour": "  blue" },
            { "id": 2, "colour": "RED" }
        ]);

        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2, "RED": 1}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates([0, 1, 2].iter().copied().collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2, "RED": 1}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates([1, 2].iter().copied().collect())
            .execute()
            .unwrap();

        // I think it would be fine if "  blue" was "Blue" instead.
        // We just need to get any non-normalised string I think, even if it's not in
        // the candidates
        milli_snap!(format!("{map:?}"), @r###"{"colour": {"  blue": 1, "RED": 1}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates([2].iter().copied().collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"RED": 1}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates([0, 1, 2].iter().copied().collect())
            .max_values_per_facet(1)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::Count)))
            .candidates([0, 1, 2].iter().copied().collect())
            .max_values_per_facet(1)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2}}"###);
    }

    #[test]
    fn many_candidates_few_facet_values() {
        let index = TempIndex::new_with_map_size(4096 * 10_000);

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = ["Red", "RED", " red ", "Blue", "BLUE"];

        let mut documents = vec![];
        for i in 0..10_000 {
            let document = serde_json::json!({
                "id": i,
                "colour": facet_values[i % 5],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = mmap_from_objects(documents);
        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 4000, "Red": 6000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .max_values_per_facet(1)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 4000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..10_000).collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 4000, "Red": 6000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..5_000).collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2000, "Red": 3000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..5_000).collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2000, "Red": 3000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..5_000).collect())
            .max_values_per_facet(1)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::Count)))
            .candidates((0..5_000).collect())
            .max_values_per_facet(1)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Red": 3000}}"###);
    }

    #[test]
    fn many_candidates_many_facet_values() {
        let index = TempIndex::new_with_map_size(4096 * 10_000);

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).map(|x| format!("{x:x}")).collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..10_000 {
            let document = serde_json::json!({
                "id": i,
                "colour": facet_values[i % 1000],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = mmap_from_objects(documents);
        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"ac9229ed5964d893af96a7076e2f8af5");

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .max_values_per_facet(2)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates_with_max_2", @r###"{"colour": {"0": 10, "1": 10}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..10_000).collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_10_000", @"ac9229ed5964d893af96a7076e2f8af5");

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..5_000).collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_5_000", @"825f23a4090d05756f46176987b7d992");
    }

    #[test]
    fn facet_stats() {
        let index = TempIndex::new_with_map_size(4096 * 10_000);

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..1000 {
            let document = serde_json::json!({
                "id": i,
                "colour": facet_values[i % 1000],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = mmap_from_objects(documents);
        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..1000).collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 999.0)}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((217..777).collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (217.0, 776.0)}"###);
    }

    #[test]
    fn facet_stats_array() {
        let index = TempIndex::new_with_map_size(4096 * 10_000);

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..1000 {
            let document = serde_json::json!({
                "id": i,
                "colour": [facet_values[i % 1000], facet_values[i % 1000] + 1000],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = mmap_from_objects(documents);
        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..1000).collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 1999.0)}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((217..777).collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (217.0, 1776.0)}"###);
    }

    #[test]
    fn facet_stats_mixed_array() {
        let index = TempIndex::new_with_map_size(4096 * 10_000);

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..1000 {
            let document = serde_json::json!({
                "id": i,
                "colour": [facet_values[i % 1000], format!("{}", facet_values[i % 1000] + 1000)],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = mmap_from_objects(documents);
        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..1000).collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 999.0)}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((217..777).collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (217.0, 776.0)}"###);
    }

    #[test]
    fn facet_mixed_values() {
        let index = TempIndex::new_with_map_size(4096 * 10_000);

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..1000 {
            let document = if i % 2 == 0 {
                serde_json::json!({
                    "id": i,
                    "colour": [facet_values[i % 1000], facet_values[i % 1000] + 1000],
                })
            } else {
                serde_json::json!({
                    "id": i,
                    "colour": format!("{}", facet_values[i % 1000] + 10000),
                })
            };
            let document = document.as_object().unwrap().clone();
            documents.push(document);
        }

        let documents = mmap_from_objects(documents);
        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((0..1000).collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 1998.0)}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(iter::once(("colour", OrderBy::default())))
            .candidates((217..777).collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (218.0, 1776.0)}"###);
    }
}

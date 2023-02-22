use std::collections::{BTreeMap, HashSet};
use std::ops::ControlFlow;
use std::{fmt, mem};

use heed::types::ByteSlice;
use heed::BytesDecode;
use roaring::RoaringBitmap;

use crate::error::UserError;
use crate::facet::FacetType;
use crate::heed_codec::facet::{
    FacetGroupKeyCodec, FacetGroupValueCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec,
    OrderedF64Codec,
};
use crate::heed_codec::{ByteSliceRefCodec, StrRefCodec};
use crate::search::facet::facet_distribution_iter;
use crate::{FieldId, Index, Result};

/// The default number of values by facets that will
/// be fetched from the key-value store.
pub const DEFAULT_VALUES_PER_FACET: usize = 100;

/// Threshold on the number of candidates that will make
/// the system to choose between one algorithm or another.
const CANDIDATES_THRESHOLD: u64 = 3000;

pub struct FacetDistribution<'a> {
    facets: Option<HashSet<String>>,
    candidates: Option<RoaringBitmap>,
    max_values_per_facet: usize,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> FacetDistribution<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> FacetDistribution<'a> {
        FacetDistribution {
            facets: None,
            candidates: None,
            max_values_per_facet: DEFAULT_VALUES_PER_FACET,
            rtxn,
            index,
        }
    }

    pub fn facets<I: IntoIterator<Item = A>, A: AsRef<str>>(&mut self, names: I) -> &mut Self {
        self.facets = Some(names.into_iter().map(|s| s.as_ref().to_string()).collect());
        self
    }

    pub fn max_values_per_facet(&mut self, max: usize) -> &mut Self {
        self.max_values_per_facet = max;
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
        distribution: &mut BTreeMap<String, u64>,
    ) -> heed::Result<()> {
        match facet_type {
            FacetType::Number => {
                let mut key_buffer: Vec<_> = field_id.to_be_bytes().to_vec();

                let distribution_prelength = distribution.len();
                let db = self.index.field_id_docid_facet_f64s;
                for docid in candidates.into_iter() {
                    key_buffer.truncate(mem::size_of::<FieldId>());
                    key_buffer.extend_from_slice(&docid.to_be_bytes());
                    let iter = db
                        .remap_key_type::<ByteSlice>()
                        .prefix_iter(self.rtxn, &key_buffer)?
                        .remap_key_type::<FieldDocIdFacetF64Codec>();

                    for result in iter {
                        let ((_, _, value), ()) = result?;
                        *distribution.entry(value.to_string()).or_insert(0) += 1;

                        if distribution.len() - distribution_prelength == self.max_values_per_facet
                        {
                            break;
                        }
                    }
                }
            }
            FacetType::String => {
                let mut normalized_distribution = BTreeMap::new();
                let mut key_buffer: Vec<_> = field_id.to_be_bytes().to_vec();

                let db = self.index.field_id_docid_facet_strings;
                'outer: for docid in candidates.into_iter() {
                    key_buffer.truncate(mem::size_of::<FieldId>());
                    key_buffer.extend_from_slice(&docid.to_be_bytes());
                    let iter = db
                        .remap_key_type::<ByteSlice>()
                        .prefix_iter(self.rtxn, &key_buffer)?
                        .remap_key_type::<FieldDocIdFacetStringCodec>();

                    for result in iter {
                        let ((_, _, normalized_value), original_value) = result?;
                        let (_, count) = normalized_distribution
                            .entry(normalized_value)
                            .or_insert_with(|| (original_value, 0));
                        *count += 1;

                        if normalized_distribution.len() == self.max_values_per_facet {
                            break 'outer;
                        }
                    }
                }

                let iter = normalized_distribution
                    .into_iter()
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
        distribution: &mut BTreeMap<String, u64>,
    ) -> heed::Result<()> {
        facet_distribution_iter::iterate_over_facet_distribution(
            self.rtxn,
            self.index
                .facet_id_f64_docids
                .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
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
        distribution: &mut BTreeMap<String, u64>,
    ) -> heed::Result<()> {
        facet_distribution_iter::iterate_over_facet_distribution(
            self.rtxn,
            self.index
                .facet_id_string_docids
                .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
            field_id,
            candidates,
            |facet_key, nbr_docids, any_docid| {
                let facet_key = StrRefCodec::bytes_decode(facet_key).unwrap();

                let key: (FieldId, _, &str) = (field_id, any_docid, facet_key);
                let original_string = self
                    .index
                    .field_id_docid_facet_strings
                    .get(self.rtxn, &key)?
                    .unwrap()
                    .to_owned();

                distribution.insert(original_string, nbr_docids);
                if distribution.len() == self.max_values_per_facet {
                    Ok(ControlFlow::Break(()))
                } else {
                    Ok(ControlFlow::Continue(()))
                }
            },
        )
    }

    /// Placeholder search, a.k.a. no candidates were specified. We iterate throught the
    /// facet values one by one and iterate on the facet level 0 for numbers.
    fn facet_values_from_raw_facet_database(
        &self,
        field_id: FieldId,
    ) -> heed::Result<BTreeMap<String, u64>> {
        let mut distribution = BTreeMap::new();

        let db = self.index.facet_id_f64_docids;
        let mut prefix = vec![];
        prefix.extend_from_slice(&field_id.to_be_bytes());
        prefix.push(0); // read values from level 0 only

        let iter = db
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, ByteSlice>(self.rtxn, prefix.as_slice())?
            .remap_types::<FacetGroupKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>();

        for result in iter {
            let (key, value) = result?;
            distribution.insert(key.left_bound.to_string(), value.bitmap.len());
            if distribution.len() == self.max_values_per_facet {
                break;
            }
        }

        let iter = self
            .index
            .facet_id_string_docids
            .as_polymorph()
            .prefix_iter::<_, ByteSlice, ByteSlice>(self.rtxn, prefix.as_slice())?
            .remap_types::<FacetGroupKeyCodec<StrRefCodec>, FacetGroupValueCodec>();

        for result in iter {
            let (key, value) = result?;

            let docid = value.bitmap.iter().next().unwrap();
            let key: (FieldId, _, &'a str) = (field_id, docid, key.left_bound);
            let original_string =
                self.index.field_id_docid_facet_strings.get(self.rtxn, &key)?.unwrap().to_owned();

            distribution.insert(original_string, value.bitmap.len());
            if distribution.len() == self.max_values_per_facet {
                break;
            }
        }

        Ok(distribution)
    }

    fn facet_values(&self, field_id: FieldId) -> heed::Result<BTreeMap<String, u64>> {
        use FacetType::{Number, String};

        match self.candidates {
            Some(ref candidates) => {
                // Classic search, candidates were specified, we must return facet values only related
                // to those candidates. We also enter here for facet strings for performance reasons.
                let mut distribution = BTreeMap::new();
                if candidates.len() <= CANDIDATES_THRESHOLD {
                    self.facet_distribution_from_documents(
                        field_id,
                        Number,
                        candidates,
                        &mut distribution,
                    )?;
                    self.facet_distribution_from_documents(
                        field_id,
                        String,
                        candidates,
                        &mut distribution,
                    )?;
                } else {
                    self.facet_numbers_distribution_from_facet_levels(
                        field_id,
                        candidates,
                        &mut distribution,
                    )?;
                    self.facet_strings_distribution_from_facet_levels(
                        field_id,
                        candidates,
                        &mut distribution,
                    )?;
                }
                Ok(distribution)
            }
            None => self.facet_values_from_raw_facet_database(field_id),
        }
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
                    .filter(|facet| !crate::is_faceted(facet, &filterable_fields))
                    .collect();
                if !invalid_fields.is_empty() {
                    return Err(UserError::InvalidFacetsDistribution {
                        invalid_facets_name: invalid_fields.into_iter().cloned().collect(),
                        valid_facets_name: filterable_fields.into_iter().collect(),
                    }
                    .into());
                } else {
                    facets.clone()
                }
            }
            None => filterable_fields,
        };

        let mut distribution = BTreeMap::new();
        for (fid, name) in fields_ids_map.iter() {
            if crate::is_faceted(name, &fields) {
                let min_value = if let Some(min_value) = crate::search::criteria::facet_min_value(
                    self.index,
                    self.rtxn,
                    fid,
                    candidates.clone(),
                )? {
                    min_value
                } else {
                    continue;
                };
                let max_value = if let Some(max_value) = crate::search::criteria::facet_max_value(
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

    pub fn execute(&self) -> Result<BTreeMap<String, BTreeMap<String, u64>>> {
        let fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let filterable_fields = self.index.filterable_fields(self.rtxn)?;

        let fields = match self.facets {
            Some(ref facets) => {
                let invalid_fields: HashSet<_> = facets
                    .iter()
                    .filter(|facet| !crate::is_faceted(facet, &filterable_fields))
                    .collect();
                if !invalid_fields.is_empty() {
                    return Err(UserError::InvalidFacetsDistribution {
                        invalid_facets_name: invalid_fields.into_iter().cloned().collect(),
                        valid_facets_name: filterable_fields.into_iter().collect(),
                    }
                    .into());
                } else {
                    facets.clone()
                }
            }
            None => filterable_fields,
        };

        let mut distribution = BTreeMap::new();
        for (fid, name) in fields_ids_map.iter() {
            if crate::is_faceted(name, &fields) {
                let values = self.facet_values(fid)?;
                distribution.insert(name.to_string(), values);
            }
        }

        Ok(distribution)
    }
}

impl fmt::Debug for FacetDistribution<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let FacetDistribution { facets, candidates, max_values_per_facet, rtxn: _, index: _ } =
            self;

        f.debug_struct("FacetDistribution")
            .field("facets", facets)
            .field("candidates", candidates)
            .field("max_values_per_facet", max_values_per_facet)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use maplit::hashset;

    use crate::documents::documents_batch_reader_from_objects;
    use crate::index::tests::TempIndex;
    use crate::{milli_snap, FacetDistribution};

    #[test]
    fn few_candidates_few_facet_values() {
        // All the tests here avoid using the code in `facet_distribution_iter` because there aren't
        // enough candidates.

        let mut index = TempIndex::new();
        index.index_documents_config.autogenerate_docids = true;

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let documents = documents!([
            { "colour": "Blue" },
            { "colour": "  blue" },
            { "colour": "RED" }
        ]);

        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2, "RED": 1}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates([0, 1, 2].iter().copied().collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2, "RED": 1}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates([1, 2].iter().copied().collect())
            .execute()
            .unwrap();

        // I think it would be fine if "  blue" was "Blue" instead.
        // We just need to get any non-normalised string I think, even if it's not in
        // the candidates
        milli_snap!(format!("{map:?}"), @r###"{"colour": {"  blue": 1, "RED": 1}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates([2].iter().copied().collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"RED": 1}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates([0, 1, 2].iter().copied().collect())
            .max_values_per_facet(1)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 1}}"###);
    }

    #[test]
    fn many_candidates_few_facet_values() {
        let mut index = TempIndex::new_with_map_size(4096 * 10_000);
        index.index_documents_config.autogenerate_docids = true;

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = ["Red", "RED", " red ", "Blue", "BLUE"];

        let mut documents = vec![];
        for i in 0..10_000 {
            let document = serde_json::json!({
                "colour": facet_values[i % 5],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = documents_batch_reader_from_objects(documents);

        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 4000, "Red": 6000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .max_values_per_facet(1)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 4000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..10_000).into_iter().collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 4000, "Red": 6000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..5_000).into_iter().collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2000, "Red": 3000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..5_000).into_iter().collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2000, "Red": 3000}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..5_000).into_iter().collect())
            .max_values_per_facet(1)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2000}}"###);
    }

    #[test]
    fn many_candidates_many_facet_values() {
        let mut index = TempIndex::new_with_map_size(4096 * 10_000);
        index.index_documents_config.autogenerate_docids = true;

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).into_iter().map(|x| format!("{x:x}")).collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..10_000 {
            let document = serde_json::json!({
                "colour": facet_values[i % 1000],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = documents_batch_reader_from_objects(documents);

        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"ac9229ed5964d893af96a7076e2f8af5");

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .max_values_per_facet(2)
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates_with_max_2", @r###"{"colour": {"0": 10, "1": 10}}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..10_000).into_iter().collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_10_000", @"ac9229ed5964d893af96a7076e2f8af5");

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..5_000).into_iter().collect())
            .execute()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_5_000", @"825f23a4090d05756f46176987b7d992");
    }

    #[test]
    fn facet_stats() {
        let mut index = TempIndex::new_with_map_size(4096 * 10_000);
        index.index_documents_config.autogenerate_docids = true;

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).into_iter().collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..1000 {
            let document = serde_json::json!({
                "colour": facet_values[i % 1000],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = documents_batch_reader_from_objects(documents);

        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..1000).into_iter().collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 999.0)}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((217..777).into_iter().collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (217.0, 776.0)}"###);
    }

    #[test]
    fn facet_stats_array() {
        let mut index = TempIndex::new_with_map_size(4096 * 10_000);
        index.index_documents_config.autogenerate_docids = true;

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).into_iter().collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..1000 {
            let document = serde_json::json!({
                "colour": [facet_values[i % 1000], facet_values[i % 1000] + 1000],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = documents_batch_reader_from_objects(documents);

        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..1000).into_iter().collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 1999.0)}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((217..777).into_iter().collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (217.0, 1776.0)}"###);
    }

    #[test]
    fn facet_stats_mixed_array() {
        let mut index = TempIndex::new_with_map_size(4096 * 10_000);
        index.index_documents_config.autogenerate_docids = true;

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).into_iter().collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..1000 {
            let document = serde_json::json!({
                "colour": [facet_values[i % 1000], format!("{}", facet_values[i % 1000] + 1000)],
            })
            .as_object()
            .unwrap()
            .clone();
            documents.push(document);
        }

        let documents = documents_batch_reader_from_objects(documents);

        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..1000).into_iter().collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 999.0)}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((217..777).into_iter().collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (217.0, 776.0)}"###);
    }

    #[test]
    fn facet_mixed_values() {
        let mut index = TempIndex::new_with_map_size(4096 * 10_000);
        index.index_documents_config.autogenerate_docids = true;

        index
            .update_settings(|settings| settings.set_filterable_fields(hashset! { S("colour") }))
            .unwrap();

        let facet_values = (0..1000).into_iter().collect::<Vec<_>>();

        let mut documents = vec![];
        for i in 0..1000 {
            let document = if i % 2 == 0 {
                serde_json::json!({
                    "colour": [facet_values[i % 1000], facet_values[i % 1000] + 1000],
                })
            } else {
                serde_json::json!({
                    "colour": format!("{}", facet_values[i % 1000] + 10000),
                })
            };
            let document = document.as_object().unwrap().clone();
            documents.push(document);
        }

        let documents = documents_batch_reader_from_objects(documents);

        index.add_documents(documents).unwrap();

        let txn = index.read_txn().unwrap();

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((0..1000).into_iter().collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 1998.0)}"###);

        let map = FacetDistribution::new(&txn, &index)
            .facets(std::iter::once("colour"))
            .candidates((217..777).into_iter().collect())
            .compute_stats()
            .unwrap();

        milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (218.0, 1776.0)}"###);
    }
}

use std::collections::{BTreeMap, HashSet};
use std::ops::Bound::Unbounded;
use std::{fmt, mem};

use heed::types::ByteSlice;
use roaring::RoaringBitmap;

use crate::error::UserError;
use crate::facet::FacetType;
use crate::heed_codec::facet::{
    FacetStringLevelZeroCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec,
};
use crate::search::facet::{FacetNumberIter, FacetNumberRange, FacetStringIter};
use crate::{FieldId, Index, Result};

/// Threshold on the number of candidates that will make
/// the system to choose between one algorithm or another.
const CANDIDATES_THRESHOLD: u64 = 3000;

pub struct FacetDistribution<'a> {
    facets: Option<HashSet<String>>,
    candidates: Option<RoaringBitmap>,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> FacetDistribution<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> FacetDistribution<'a> {
        FacetDistribution { facets: None, candidates: None, rtxn, index }
    }

    pub fn facets<I: IntoIterator<Item = A>, A: AsRef<str>>(&mut self, names: I) -> &mut Self {
        self.facets = Some(names.into_iter().map(|s| s.as_ref().to_string()).collect());
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
                let mut key_buffer: Vec<_> = field_id.to_be_bytes().iter().copied().collect();

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
                    }
                }
            }
            FacetType::String => {
                let mut normalized_distribution = BTreeMap::new();
                let mut key_buffer: Vec<_> = field_id.to_be_bytes().iter().copied().collect();

                let db = self.index.field_id_docid_facet_strings;
                for docid in candidates.into_iter() {
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
        let iter =
            FacetNumberIter::new_non_reducing(self.rtxn, self.index, field_id, candidates.clone())?;

        for result in iter {
            let (value, mut docids) = result?;
            docids &= candidates;
            if !docids.is_empty() {
                distribution.insert(value.to_string(), docids.len());
            }
        }

        Ok(())
    }

    fn facet_strings_distribution_from_facet_levels(
        &self,
        field_id: FieldId,
        candidates: &RoaringBitmap,
        distribution: &mut BTreeMap<String, u64>,
    ) -> heed::Result<()> {
        let iter =
            FacetStringIter::new_non_reducing(self.rtxn, self.index, field_id, candidates.clone())?;

        for result in iter {
            let (_normalized, original, mut docids) = result?;
            docids &= candidates;
            if !docids.is_empty() {
                distribution.insert(original.to_string(), docids.len());
            }
        }

        Ok(())
    }

    /// Placeholder search, a.k.a. no candidates were specified. We iterate throught the
    /// facet values one by one and iterate on the facet level 0 for numbers.
    fn facet_values_from_raw_facet_database(
        &self,
        field_id: FieldId,
    ) -> heed::Result<BTreeMap<String, u64>> {
        let mut distribution = BTreeMap::new();

        let db = self.index.facet_id_f64_docids;
        let range = FacetNumberRange::new(self.rtxn, db, field_id, 0, Unbounded, Unbounded)?;

        for result in range {
            let ((_, _, value, _), docids) = result?;
            distribution.insert(value.to_string(), docids.len());
        }

        let iter = self
            .index
            .facet_id_string_docids
            .remap_key_type::<ByteSlice>()
            .prefix_iter(self.rtxn, &field_id.to_be_bytes())?
            .remap_key_type::<FacetStringLevelZeroCodec>();

        let mut normalized_distribution = BTreeMap::new();
        for result in iter {
            let ((_, normalized_value), (original_value, docids)) = result?;
            normalized_distribution.insert(normalized_value, (original_value, docids.len()));
        }

        let iter = normalized_distribution
            .into_iter()
            .map(|(_normalized, (original, count))| (original.to_string(), count));
        distribution.extend(iter);

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
        let FacetDistribution { facets, candidates, rtxn: _, index: _ } = self;

        f.debug_struct("FacetDistribution")
            .field("facets", facets)
            .field("candidates", candidates)
            .finish()
    }
}

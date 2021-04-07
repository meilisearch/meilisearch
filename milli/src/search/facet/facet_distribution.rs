use std::collections::{HashSet, BTreeMap};
use std::ops::Bound::Unbounded;
use std::{cmp, fmt};

use anyhow::Context;
use heed::BytesDecode;
use roaring::RoaringBitmap;

use crate::facet::{FacetType, FacetValue};
use crate::heed_codec::facet::{FacetValueStringCodec, FacetLevelValueF64Codec};
use crate::heed_codec::facet::{FieldDocIdFacetStringCodec, FieldDocIdFacetF64Codec};
use crate::search::facet::{FacetIter, FacetRange};
use crate::{Index, FieldId, DocumentId};

/// The default number of values by facets that will
/// be fetched from the key-value store.
const DEFAULT_VALUES_BY_FACET: usize = 100;

/// The hard limit in the number of values by facets that will be fetched from
/// the key-value store. Searching for more values could slow down the engine.
const MAX_VALUES_BY_FACET: usize = 1000;

/// Threshold on the number of candidates that will make
/// the system to choose between one algorithm or another.
const CANDIDATES_THRESHOLD: u64 = 1000;

pub struct FacetDistribution<'a> {
    facets: Option<HashSet<String>>,
    candidates: Option<RoaringBitmap>,
    max_values_by_facet: usize,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> FacetDistribution<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> FacetDistribution<'a> {
        FacetDistribution {
            facets: None,
            candidates: None,
            max_values_by_facet: DEFAULT_VALUES_BY_FACET,
            rtxn,
            index,
        }
    }

    pub fn facets<I: IntoIterator<Item=A>, A: AsRef<str>>(&mut self, names: I) -> &mut Self {
        self.facets = Some(names.into_iter().map(|s| s.as_ref().to_string()).collect());
        self
    }

    pub fn candidates(&mut self, candidates: RoaringBitmap) -> &mut Self {
        self.candidates = Some(candidates);
        self
    }

    pub fn max_values_by_facet(&mut self, max: usize) -> &mut Self {
        self.max_values_by_facet = cmp::min(max, MAX_VALUES_BY_FACET);
        self
    }

    /// There is a small amount of candidates OR we ask for facet string values so we
    /// decide to iterate over the facet values of each one of them, one by one.
    fn facet_values_from_documents(
        &self,
        field_id: FieldId,
        facet_type: FacetType,
        candidates: &RoaringBitmap,
    ) -> heed::Result<BTreeMap<FacetValue, u64>>
    {
        fn fetch_facet_values<'t, KC, K: 't>(
            index: &Index,
            rtxn: &'t heed::RoTxn,
            field_id: FieldId,
            candidates: &RoaringBitmap,
        ) -> heed::Result<BTreeMap<FacetValue, u64>>
        where
            KC: BytesDecode<'t, DItem = (FieldId, DocumentId, K)>,
            K: Into<FacetValue>,
        {
            let mut facet_values = BTreeMap::new();
            let mut key_buffer = vec![field_id];

            for docid in candidates.into_iter().take(CANDIDATES_THRESHOLD as usize) {
                key_buffer.truncate(1);
                key_buffer.extend_from_slice(&docid.to_be_bytes());
                let iter = index.field_id_docid_facet_values
                    .prefix_iter(rtxn, &key_buffer)?
                    .remap_key_type::<KC>();

                for result in iter {
                    let ((_, _, value), ()) = result?;
                    *facet_values.entry(value.into()).or_insert(0) += 1;
                }
            }

            Ok(facet_values)
        }

        let index = self.index;
        let rtxn = self.rtxn;
        match facet_type {
            FacetType::String => {
                fetch_facet_values::<FieldDocIdFacetStringCodec, _>(index, rtxn, field_id, candidates)
            },
            FacetType::Number => {
                fetch_facet_values::<FieldDocIdFacetF64Codec, _>(index, rtxn, field_id, candidates)
            },
        }
    }

    /// There is too much documents, we use the facet levels to move throught
    /// the facet values, to find the candidates and values associated.
    fn facet_values_from_facet_levels(
        &self,
        field_id: FieldId,
        facet_type: FacetType,
        candidates: &RoaringBitmap,
    ) -> heed::Result<BTreeMap<FacetValue, u64>>
    {
        let iter = match facet_type {
            FacetType::String => unreachable!(),
            FacetType::Number => {
                let iter = FacetIter::new_non_reducing(
                    self.rtxn, self.index, field_id, candidates.clone(),
                )?;
                iter.map(|r| r.map(|(v, docids)| (FacetValue::from(v), docids)))
            },
        };

        let mut facet_values = BTreeMap::new();
        for result in iter {
            let (value, mut docids) = result?;
            docids.intersect_with(candidates);
            if !docids.is_empty() {
                facet_values.insert(value, docids.len());
            }
            if facet_values.len() == self.max_values_by_facet {
                break;
            }
        }

        Ok(facet_values)
    }

    /// Placeholder search, a.k.a. no candidates were specified. We iterate throught the
    /// facet values one by one and iterate on the facet level 0 for numbers.
    fn facet_values_from_raw_facet_database(
        &self,
        field_id: FieldId,
        facet_type: FacetType,
    ) -> heed::Result<BTreeMap<FacetValue, u64>>
    {
        let db = self.index.facet_field_id_value_docids;
        let level = 0;
        let iter = match facet_type {
            FacetType::String => {
                let iter = db
                    .prefix_iter(self.rtxn, &[field_id])?
                    .remap_key_type::<FacetValueStringCodec>()
                    .map(|r| r.map(|((_, v), docids)| (FacetValue::from(v), docids)));
                Box::new(iter) as Box::<dyn Iterator<Item=_>>
            },
            FacetType::Number => {
                let db = db.remap_key_type::<FacetLevelValueF64Codec>();
                let range = FacetRange::new(
                    self.rtxn, db, field_id, level, Unbounded, Unbounded,
                )?;
                Box::new(range.map(|r| r.map(|((_, _, v, _), docids)| (FacetValue::from(v), docids))))
            },
        };

        let mut facet_values = BTreeMap::new();
        for result in iter {
            let (value, docids) = result?;
            facet_values.insert(value, docids.len());
            if facet_values.len() == self.max_values_by_facet {
                break;
            }
        }

        Ok(facet_values)
    }

    fn facet_values(
        &self,
        field_id: FieldId,
        facet_type: FacetType,
    ) -> heed::Result<BTreeMap<FacetValue, u64>>
    {
        if let Some(candidates) = self.candidates.as_ref() {
            // Classic search, candidates were specified, we must return facet values only related
            // to those candidates. We also enter here for facet strings for performance reasons.
            if candidates.len() <= CANDIDATES_THRESHOLD || facet_type == FacetType::String {
                self.facet_values_from_documents(field_id, facet_type, candidates)
            } else {
                self.facet_values_from_facet_levels(field_id, facet_type, candidates)
            }
        } else {
            self.facet_values_from_raw_facet_database(field_id, facet_type)
        }
    }

    pub fn execute(&self) -> anyhow::Result<BTreeMap<String, BTreeMap<FacetValue, u64>>> {
        let fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let faceted_fields = self.index.faceted_fields(self.rtxn)?;
        let fields_ids: Vec<_> = match &self.facets {
            Some(names) => names
                .iter()
                .filter_map(|n| faceted_fields.get(n).map(|t| (n.to_string(), *t)))
                .collect(),
            None => faceted_fields.into_iter().collect(),
        };

        let mut facets_values = BTreeMap::new();
        for (name, ftype) in fields_ids {
            let fid = fields_ids_map.id(&name).with_context(|| {
                format!("missing field name {:?} from the fields id map", name)
            })?;
            let values = self.facet_values(fid, ftype)?;
            facets_values.insert(name, values);
        }

        Ok(facets_values)
    }
}

impl fmt::Debug for FacetDistribution<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let FacetDistribution {
            facets,
            candidates,
            max_values_by_facet,
            rtxn: _,
            index: _,
        } = self;

        f.debug_struct("FacetDistribution")
            .field("facets", facets)
            .field("candidates", candidates)
            .field("max_values_by_facet", max_values_by_facet)
            .finish()
    }
}

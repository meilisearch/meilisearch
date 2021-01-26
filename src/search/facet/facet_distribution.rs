use std::collections::{HashSet, BTreeMap};
use std::ops::Bound::Unbounded;
use std::{cmp, fmt};

use roaring::RoaringBitmap;

use crate::facet::{FacetType, FacetValue};
use crate::heed_codec::facet::{FacetValueStringCodec, FacetLevelValueF64Codec, FacetLevelValueI64Codec};
use crate::heed_codec::facet::{FieldDocIdFacetStringCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetI64Codec};
use crate::search::facet::{FacetIter, FacetRange};
use crate::{Index, FieldId};

pub struct FacetDistribution<'a> {
    facets: Option<HashSet<String>>,
    candidates: Option<RoaringBitmap>,
    max_values_by_facet: usize,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> FacetDistribution<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> FacetDistribution<'a> {
        FacetDistribution { facets: None, candidates: None, max_values_by_facet: 100, rtxn, index }
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
        self.max_values_by_facet = cmp::min(max, 1000);
        self
    }

    fn facet_values(
        &self,
        field_id: FieldId,
        facet_type: FacetType,
    ) -> heed::Result<BTreeMap<FacetValue, u64>>
    {
        if let Some(candidates) = self.candidates.as_ref() {
            if candidates.len() <= 1000 {
                let mut key_buffer = vec![field_id];
                match facet_type {
                    FacetType::Float => {
                        let mut facet_values = BTreeMap::new();
                        for docid in candidates {
                            key_buffer.truncate(1);
                            key_buffer.extend_from_slice(&docid.to_be_bytes());
                            let iter = self.index.field_id_docid_facet_values
                                .prefix_iter(self.rtxn, &key_buffer)?
                                .remap_key_type::<FieldDocIdFacetF64Codec>();
                            for result in iter {
                                let ((_, _, value), ()) = result?;
                                *facet_values.entry(FacetValue::from(value)).or_insert(0) += 1;
                            }
                        }
                        Ok(facet_values)
                    },
                    FacetType::Integer => {
                        let mut facet_values = BTreeMap::new();
                        for docid in candidates {
                            key_buffer.truncate(1);
                            key_buffer.extend_from_slice(&docid.to_be_bytes());
                            let iter = self.index.field_id_docid_facet_values
                                .prefix_iter(self.rtxn, &key_buffer)?
                                .remap_key_type::<FieldDocIdFacetI64Codec>();
                            for result in iter {
                                let ((_, _, value), ()) = result?;
                                *facet_values.entry(FacetValue::from(value)).or_insert(0) += 1;
                            }
                        }
                        Ok(facet_values)
                    },
                    FacetType::String => {
                        let mut facet_values = BTreeMap::new();
                        for docid in candidates {
                            key_buffer.truncate(1);
                            key_buffer.extend_from_slice(&docid.to_be_bytes());
                            let iter = self.index.field_id_docid_facet_values
                                .prefix_iter(self.rtxn, &key_buffer)?
                                .remap_key_type::<FieldDocIdFacetStringCodec>();
                            for result in iter {
                                let ((_, _, value), ()) = result?;
                                *facet_values.entry(FacetValue::from(value)).or_insert(0) += 1;
                            }
                        }
                        Ok(facet_values)
                    },
                }
            } else {
                let iter = match facet_type {
                    FacetType::String => {
                        let db = self.index.facet_field_id_value_docids;
                        let iter = db
                            .prefix_iter(self.rtxn, &[field_id])?
                            .remap_key_type::<FacetValueStringCodec>()
                            .map(|r| r.map(|((_, v), docids)| (FacetValue::from(v), docids)));
                        Box::new(iter) as Box::<dyn Iterator<Item=_>>
                    },
                    FacetType::Integer => {
                        let iter = FacetIter::<i64, FacetLevelValueI64Codec>::new_non_reducing(
                            self.rtxn, self.index, field_id, candidates.clone(),
                        )?;
                        Box::new(iter.map(|r| r.map(|(v, docids)| (FacetValue::from(v), docids))))
                    },
                    FacetType::Float => {
                        let iter = FacetIter::<f64, FacetLevelValueF64Codec>::new_non_reducing(
                            self.rtxn, self.index, field_id, candidates.clone(),
                        )?;
                        Box::new(iter.map(|r| r.map(|(v, docids)| (FacetValue::from(v), docids))))
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
        } else {
            let db = self.index.facet_field_id_value_docids;
            let iter = match facet_type {
                FacetType::String => {
                    let iter = db
                        .prefix_iter(self.rtxn, &[field_id])?
                        .remap_key_type::<FacetValueStringCodec>()
                        .map(|r| r.map(|((_, v), docids)| (FacetValue::from(v), docids)));
                    Box::new(iter) as Box::<dyn Iterator<Item=_>>
                },
                FacetType::Integer => {
                    let db = db.remap_key_type::<FacetLevelValueI64Codec>();
                    let range = FacetRange::<i64, _>::new(
                        self.rtxn, db, field_id, 0, Unbounded, Unbounded,
                    )?;
                    Box::new(range.map(|r| r.map(|((_, _, v, _), docids)| (FacetValue::from(v), docids))))
                },
                FacetType::Float => {
                    let db = db.remap_key_type::<FacetLevelValueF64Codec>();
                    let range = FacetRange::<f64, _>::new(
                        self.rtxn, db, field_id, 0, Unbounded, Unbounded,
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
    }

    pub fn execute(&self) -> heed::Result<BTreeMap<String, BTreeMap<FacetValue, u64>>> {
        let fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let faceted_fields = self.index.faceted_fields(self.rtxn)?;
        let fields_ids: Vec<_> = match &self.facets {
            Some(names) => {
                names.iter().filter_map(|n| {
                    let id = fields_ids_map.id(n)?;
                    faceted_fields.get(&id).cloned().map(|t| (id, t))
                }).collect()
            },
            None => faceted_fields.iter().map(|(id, t)| (*id, *t)).collect(),
        };

        let mut facets_values = BTreeMap::new();
        for (fid, ftype) in fields_ids {
            let facet_name = fields_ids_map.name(fid).unwrap();
            let values = self.facet_values(fid, ftype)?;
            facets_values.insert(facet_name.to_string(), values);
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

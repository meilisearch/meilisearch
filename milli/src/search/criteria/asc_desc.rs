use std::collections::HashMap;
use std::mem::take;

use anyhow::bail;
use itertools::Itertools;
use log::debug;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetLevelValueF64Codec, FacetLevelValueI64Codec};
use crate::heed_codec::facet::{FieldDocIdFacetI64Codec, FieldDocIdFacetF64Codec};
use crate::search::criteria::{resolve_query_tree, CriteriaBuilder};
use crate::search::facet::FacetIter;
use crate::search::query_tree::Operation;
use crate::{FieldId, Index};
use super::{Criterion, CriterionResult};

pub struct AscDesc<'t> {
    index: &'t Index,
    rtxn: &'t heed::RoTxn<'t>,
    field_id: FieldId,
    facet_type: FacetType,
    ascending: bool,
    query_tree: Option<Operation>,
    candidates: RoaringBitmap,
    bucket_candidates: RoaringBitmap,
    faceted_candidates: RoaringBitmap,
    parent: Option<Box<dyn Criterion + 't>>,
}

impl<'t> AscDesc<'t> {
    pub fn initial_asc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
        field_id: FieldId,
        facet_type: FacetType,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Self::initial(index, rtxn, query_tree, candidates, field_id, facet_type, true)
    }

    pub fn initial_desc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
        field_id: FieldId,
        facet_type: FacetType,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Self::initial(index, rtxn, query_tree, candidates, field_id, facet_type, false)
    }

    pub fn asc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        parent: Box<dyn Criterion + 't>,
        field_id: FieldId,
        facet_type: FacetType,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Self::new(index, rtxn, parent, field_id, facet_type, true)
    }

    pub fn desc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        parent: Box<dyn Criterion + 't>,
        field_id: FieldId,
        facet_type: FacetType,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Self::new(index, rtxn, parent, field_id, facet_type, false)
    }

    fn initial(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
        field_id: FieldId,
        facet_type: FacetType,
        ascending: bool,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        let faceted_candidates = index.faceted_documents_ids(rtxn, field_id)?;
        let candidates = match &query_tree {
            Some(qt) => {
                let context = CriteriaBuilder::new(rtxn, index)?;
                let mut qt_candidates = resolve_query_tree(&context, qt, &mut HashMap::new())?;
                if let Some(candidates) = candidates {
                    qt_candidates.intersect_with(&candidates);
                }
                qt_candidates
            },
            None => candidates.unwrap_or(faceted_candidates.clone()),
        };

        Ok(AscDesc {
            index,
            rtxn,
            field_id,
            facet_type,
            ascending,
            query_tree,
            candidates,
            faceted_candidates,
            bucket_candidates: RoaringBitmap::new(),
            parent: None,
        })
    }

    fn new(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        parent: Box<dyn Criterion + 't>,
        field_id: FieldId,
        facet_type: FacetType,
        ascending: bool,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Ok(AscDesc {
            index,
            rtxn,
            field_id,
            facet_type,
            ascending,
            query_tree: None,
            candidates: RoaringBitmap::new(),
            faceted_candidates: index.faceted_documents_ids(rtxn, field_id)?,
            bucket_candidates: RoaringBitmap::new(),
            parent: Some(parent),
        })
    }
}

impl<'t> Criterion for AscDesc<'t> {
    fn next(&mut self) -> anyhow::Result<Option<CriterionResult>> {
        loop {
            debug!("Facet {} iteration ({:?})",
                if self.ascending { "Asc" } else { "Desc" }, self.candidates,
            );

            match &mut self.candidates {
                candidates if candidates.is_empty() => {
                    let query_tree = self.query_tree.take();
                    let candidates = take(&mut self.candidates);
                    let bucket_candidates = take(&mut self.bucket_candidates);

                    match self.parent.as_mut() {
                        Some(parent) => {
                            match parent.next()? {
                                Some(CriterionResult { query_tree, mut candidates, bucket_candidates }) => {
                                    self.query_tree = query_tree;
                                    candidates.intersect_with(&self.faceted_candidates);
                                    self.candidates = candidates;
                                    self.bucket_candidates = bucket_candidates;
                                },
                                None => return Ok(None),
                            }
                        },
                        None => if query_tree.is_none() && bucket_candidates.is_empty() {
                            return Ok(None)
                        },
                    }

                    return Ok(Some(CriterionResult { query_tree, candidates, bucket_candidates }));
                },
                candidates => {
                    let bucket_candidates = match self.parent {
                        Some(_) => take(&mut self.bucket_candidates),
                        None => candidates.clone(),
                    };

                    let found_candidates = facet_ordered(
                        self.index,
                        self.rtxn,
                        self.field_id,
                        self.facet_type,
                        self.ascending,
                        candidates.clone(),
                    )?;

                    candidates.difference_with(&found_candidates);

                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.clone(),
                        candidates: found_candidates,
                        bucket_candidates,
                    }));
                },
            }
        }
    }
}

fn facet_ordered(
    index: &Index,
    rtxn: &heed::RoTxn,
    field_id: FieldId,
    facet_type: FacetType,
    ascending: bool,
    candidates: RoaringBitmap,
) -> anyhow::Result<RoaringBitmap>
{
    match facet_type {
        FacetType::Float => {
            if candidates.len() <= 1000 {
                let db = index.field_id_docid_facet_values.remap_key_type::<FieldDocIdFacetF64Codec>();
                let mut docids_values = Vec::with_capacity(candidates.len() as usize);
                for docid in candidates.iter() {
                    let left = (field_id, docid, f64::MIN);
                    let right = (field_id, docid, f64::MAX);
                    let mut iter = db.range(rtxn, &(left..=right))?;
                    let entry = if ascending { iter.next() } else { iter.last() };
                    if let Some(((_, _, value), ())) = entry.transpose()? {
                        docids_values.push((docid, OrderedFloat(value)));
                    }
                }
                docids_values.sort_unstable_by_key(|(_, value)| *value);
                let iter = docids_values.into_iter();
                let iter = if ascending {
                    Box::new(iter) as Box<dyn Iterator<Item = _>>
                } else {
                    Box::new(iter.rev())
                };
                match iter.group_by(|(_, v)| *v).into_iter().next() {
                    Some((_, ids)) => Ok(ids.map(|(id, _)| id).into_iter().collect()),
                    None => Ok(RoaringBitmap::new())
                }
            } else {
                let facet_fn = if ascending {
                    FacetIter::<f64, FacetLevelValueF64Codec>::new_reducing
                } else {
                    FacetIter::<f64, FacetLevelValueF64Codec>::new_reverse_reducing
                };

                let mut iter = facet_fn(rtxn, index, field_id, candidates)?;
                Ok(iter.next().transpose()?.map(|(_, docids)| docids).unwrap_or_default())
            }
        },
        FacetType::Integer => {
            if candidates.len() <= 1000 {
                let db = index.field_id_docid_facet_values.remap_key_type::<FieldDocIdFacetI64Codec>();
                let mut docids_values = Vec::with_capacity(candidates.len() as usize);
                for docid in candidates.iter() {
                    let left = (field_id, docid, i64::MIN);
                    let right = (field_id, docid, i64::MAX);
                    let mut iter = db.range(rtxn, &(left..=right))?;
                    let entry = if ascending { iter.next() } else { iter.last() };
                    if let Some(((_, _, value), ())) = entry.transpose()? {
                        docids_values.push((docid, value));
                    }
                }
                docids_values.sort_unstable_by_key(|(_, value)| *value);
                let iter = docids_values.into_iter();
                let iter = if ascending {
                    Box::new(iter) as Box<dyn Iterator<Item = _>>
                } else {
                    Box::new(iter.rev())
                };
                match iter.group_by(|(_, v)| *v).into_iter().next() {
                    Some((_, ids)) => Ok(ids.map(|(id, _)| id).into_iter().collect()),
                    None => Ok(RoaringBitmap::new())
                }
            } else {
                let facet_fn = if ascending {
                    FacetIter::<i64, FacetLevelValueI64Codec>::new_reducing
                } else {
                    FacetIter::<i64, FacetLevelValueI64Codec>::new_reverse_reducing
                };

                let mut iter = facet_fn(rtxn, index, field_id, candidates)?;
                Ok(iter.next().transpose()?.map(|(_, docids)| docids).unwrap_or_default())
            }
        },
        FacetType::String => bail!("criteria facet type must be a number"),
    }
}

use std::collections::HashMap;
use std::mem::take;

use anyhow::{bail, Context as _};
use itertools::Itertools;
use log::debug;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::FieldDocIdFacetF64Codec;
use crate::search::criteria::{resolve_query_tree, CriteriaBuilder};
use crate::search::facet::FacetIter;
use crate::search::query_tree::Operation;
use crate::search::WordDerivationsCache;
use crate::{FieldsIdsMap, FieldId, Index};
use super::{Criterion, CriterionResult};

/// Threshold on the number of candidates that will make
/// the system to choose between one algorithm or another.
const CANDIDATES_THRESHOLD: u64 = 1000;

pub struct AscDesc<'t> {
    index: &'t Index,
    rtxn: &'t heed::RoTxn<'t>,
    field_name: String,
    field_id: FieldId,
    facet_type: FacetType,
    ascending: bool,
    query_tree: Option<Operation>,
    candidates: Box<dyn Iterator<Item = heed::Result<RoaringBitmap>> + 't>,
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
        field_name: String,
    ) -> anyhow::Result<Self>
    {
        Self::initial(index, rtxn, query_tree, candidates, field_name, true)
    }

    pub fn initial_desc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
        field_name: String,
    ) -> anyhow::Result<Self>
    {
        Self::initial(index, rtxn, query_tree, candidates, field_name, false)
    }

    pub fn asc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        parent: Box<dyn Criterion + 't>,
        field_name: String,
    ) -> anyhow::Result<Self>
    {
        Self::new(index, rtxn, parent, field_name, true)
    }

    pub fn desc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        parent: Box<dyn Criterion + 't>,
        field_name: String,
    ) -> anyhow::Result<Self>
    {
        Self::new(index, rtxn, parent, field_name, false)
    }

    fn initial(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
        field_name: String,
        ascending: bool,
    ) -> anyhow::Result<Self>
    {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let faceted_fields = index.faceted_fields(rtxn)?;
        let (field_id, facet_type) = field_id_facet_type(&fields_ids_map, &faceted_fields, &field_name)?;

        let faceted_candidates = index.faceted_documents_ids(rtxn, field_id)?;
        let candidates = match &query_tree {
            Some(qt) => {
                let context = CriteriaBuilder::new(rtxn, index)?;
                let mut qt_candidates = resolve_query_tree(&context, qt, &mut HashMap::new(), &mut WordDerivationsCache::new())?;
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
            field_name,
            field_id,
            facet_type,
            ascending,
            query_tree,
            candidates: facet_ordered(index, rtxn, field_id, facet_type, ascending, candidates)?,
            faceted_candidates,
            bucket_candidates: RoaringBitmap::new(),
            parent: None,
        })
    }

    fn new(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        parent: Box<dyn Criterion + 't>,
        field_name: String,
        ascending: bool,
    ) -> anyhow::Result<Self>
    {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let faceted_fields = index.faceted_fields(rtxn)?;
        let (field_id, facet_type) = field_id_facet_type(&fields_ids_map, &faceted_fields, &field_name)?;

        Ok(AscDesc {
            index,
            rtxn,
            field_name,
            field_id,
            facet_type,
            ascending,
            query_tree: None,
            candidates: Box::new(std::iter::empty()),
            faceted_candidates: index.faceted_documents_ids(rtxn, field_id)?,
            bucket_candidates: RoaringBitmap::new(),
            parent: Some(parent),
        })
    }
}

impl<'t> Criterion for AscDesc<'t> {
    #[logging_timer::time("AscDesc::{}")]
    fn next(&mut self, wdcache: &mut WordDerivationsCache) -> anyhow::Result<Option<CriterionResult>> {
        loop {
            debug!("Facet {}({}) iteration",
                if self.ascending { "Asc" } else { "Desc" }, self.field_name
            );

            match self.candidates.next().transpose()? {
                None => {
                    let query_tree = self.query_tree.take();
                    let bucket_candidates = take(&mut self.bucket_candidates);
                    match self.parent.as_mut() {
                        Some(parent) => {
                            match parent.next(wdcache)? {
                                Some(CriterionResult { query_tree, candidates, bucket_candidates }) => {
                                    self.query_tree = query_tree;
                                    let candidates = match (&self.query_tree, candidates) {
                                        (_, Some(mut candidates)) => {
                                            candidates.intersect_with(&self.faceted_candidates);
                                            candidates
                                        },
                                        (Some(qt), None) => {
                                            let context = CriteriaBuilder::new(&self.rtxn, &self.index)?;
                                            let mut candidates = resolve_query_tree(&context, qt, &mut HashMap::new(), wdcache)?;
                                            candidates.intersect_with(&self.faceted_candidates);
                                            candidates
                                        },
                                        (None, None) => take(&mut self.faceted_candidates),
                                    };
                                    if bucket_candidates.is_empty() {
                                        self.bucket_candidates.union_with(&candidates);
                                    } else {
                                        self.bucket_candidates.union_with(&bucket_candidates);
                                    }
                                    self.candidates = facet_ordered(
                                        self.index,
                                        self.rtxn,
                                        self.field_id,
                                        self.facet_type,
                                        self.ascending,
                                        candidates,
                                    )?;
                                },
                                None => return Ok(None),
                            }
                        },
                        None => if query_tree.is_none() && bucket_candidates.is_empty() {
                            return Ok(None)
                        },
                    }

                    return Ok(Some(CriterionResult {
                        query_tree,
                        candidates: Some(RoaringBitmap::new()),
                        bucket_candidates,
                    }));
                },
                Some(candidates) => {
                    let bucket_candidates = match self.parent {
                        Some(_) => take(&mut self.bucket_candidates),
                        None => candidates.clone(),
                    };

                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.clone(),
                        candidates: Some(candidates),
                        bucket_candidates,
                    }));
                },
            }
        }
    }
}

fn field_id_facet_type(
    fields_ids_map: &FieldsIdsMap,
    faceted_fields: &HashMap<String, FacetType>,
    field: &str,
) -> anyhow::Result<(FieldId, FacetType)>
{
    let id = fields_ids_map.id(field).with_context(|| {
        format!("field {:?} isn't registered", field)
    })?;
    let facet_type = faceted_fields.get(field).with_context(|| {
        format!("field {:?} isn't faceted", field)
    })?;
    Ok((id, *facet_type))
}

/// Returns an iterator over groups of the given candidates in ascending or descending order.
///
/// It will either use an iterative or a recursive method on the whole facet database depending
/// on the number of candidates to rank.
fn facet_ordered<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: FieldId,
    facet_type: FacetType,
    ascending: bool,
    candidates: RoaringBitmap,
) -> anyhow::Result<Box<dyn Iterator<Item = heed::Result<RoaringBitmap>> + 't>>
{
    match facet_type {
        FacetType::Number => {
            if candidates.len() <= CANDIDATES_THRESHOLD {
                let iter = iterative_facet_ordered_iter(
                    index, rtxn, field_id, ascending, candidates,
                )?;
                Ok(Box::new(iter.map(Ok)) as Box<dyn Iterator<Item = _>>)
            } else {
                let facet_fn = if ascending {
                    FacetIter::new_reducing
                } else {
                    FacetIter::new_reverse_reducing
                };
                let iter = facet_fn(rtxn, index, field_id, candidates)?;
                Ok(Box::new(iter.map(|res| res.map(|(_, docids)| docids))))
            }
        },
        FacetType::String => bail!("criteria facet type must be a number"),
    }
}

/// Fetch the whole list of candidates facet values one by one and order them by it.
///
/// This function is fast when the amount of candidates to rank is small.
fn iterative_facet_ordered_iter<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: FieldId,
    ascending: bool,
    candidates: RoaringBitmap,
) -> anyhow::Result<impl Iterator<Item = RoaringBitmap> + 't>
{
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
    docids_values.sort_unstable_by_key(|(_, v)| v.clone());
    let iter = docids_values.into_iter();
    let iter = if ascending {
        Box::new(iter) as Box<dyn Iterator<Item = _>>
    } else {
        Box::new(iter.rev())
    };

    // The itertools GroupBy iterator doesn't provide an owned version, we are therefore
    // required to collect the result into an owned collection (a Vec).
    // https://github.com/rust-itertools/itertools/issues/499
    let vec: Vec<_> = iter.group_by(|(_, v)| v.clone())
        .into_iter()
        .map(|(_, ids)| ids.map(|(id, _)| id).collect())
        .collect();

    Ok(vec.into_iter())
}

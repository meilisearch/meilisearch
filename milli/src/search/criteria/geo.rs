use std::iter;

use roaring::RoaringBitmap;
use rstar::RTree;

use super::{Criterion, CriterionParameters, CriterionResult};
use crate::search::criteria::{resolve_query_tree, CriteriaBuilder};
use crate::{lat_lng_to_xyz, GeoPoint, Index, Result};

pub struct Geo<'t> {
    index: &'t Index,
    rtxn: &'t heed::RoTxn<'t>,
    ascending: bool,
    parent: Box<dyn Criterion + 't>,
    candidates: Box<dyn Iterator<Item = RoaringBitmap>>,
    allowed_candidates: RoaringBitmap,
    bucket_candidates: RoaringBitmap,
    rtree: Option<RTree<GeoPoint>>,
    point: [f64; 2],
}

impl<'t> Geo<'t> {
    pub fn asc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn<'t>,
        parent: Box<dyn Criterion + 't>,
        point: [f64; 2],
    ) -> Result<Self> {
        Self::new(index, rtxn, parent, point, true)
    }

    pub fn desc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn<'t>,
        parent: Box<dyn Criterion + 't>,
        point: [f64; 2],
    ) -> Result<Self> {
        Self::new(index, rtxn, parent, point, false)
    }

    fn new(
        index: &'t Index,
        rtxn: &'t heed::RoTxn<'t>,
        parent: Box<dyn Criterion + 't>,
        point: [f64; 2],
        ascending: bool,
    ) -> Result<Self> {
        let candidates = Box::new(iter::empty());
        let allowed_candidates = index.geo_faceted_documents_ids(rtxn)?;
        let bucket_candidates = RoaringBitmap::new();
        let rtree = index.geo_rtree(rtxn)?;

        Ok(Self {
            index,
            rtxn,
            ascending,
            parent,
            candidates,
            allowed_candidates,
            bucket_candidates,
            rtree,
            point,
        })
    }
}

impl Criterion for Geo<'_> {
    fn next(&mut self, params: &mut CriterionParameters) -> Result<Option<CriterionResult>> {
        let rtree = self.rtree.as_ref();

        loop {
            match self.candidates.next() {
                Some(mut candidates) => {
                    candidates -= params.excluded_candidates;
                    self.allowed_candidates -= &candidates;
                    return Ok(Some(CriterionResult {
                        query_tree: None,
                        candidates: Some(candidates),
                        filtered_candidates: None,
                        bucket_candidates: Some(self.bucket_candidates.clone()),
                    }));
                }
                None => match self.parent.next(params)? {
                    Some(CriterionResult {
                        query_tree,
                        candidates,
                        filtered_candidates,
                        bucket_candidates,
                    }) => {
                        let mut candidates = match (&query_tree, candidates) {
                            (_, Some(candidates)) => candidates,
                            (Some(qt), None) => {
                                let context = CriteriaBuilder::new(self.rtxn, self.index)?;
                                resolve_query_tree(&context, qt, params.wdcache)?
                            }
                            (None, None) => self.index.documents_ids(self.rtxn)?,
                        };

                        if let Some(filtered_candidates) = filtered_candidates {
                            candidates &= filtered_candidates;
                        }

                        match bucket_candidates {
                            Some(bucket_candidates) => self.bucket_candidates |= bucket_candidates,
                            None => self.bucket_candidates |= &candidates,
                        }

                        if candidates.is_empty() {
                            continue;
                        }
                        self.allowed_candidates = &candidates - params.excluded_candidates;
                        self.candidates = match rtree {
                            Some(rtree) => geo_point(
                                rtree,
                                self.allowed_candidates.clone(),
                                self.point,
                                self.ascending,
                            ),
                            None => Box::new(std::iter::empty()),
                        };
                    }
                    None => return Ok(None),
                },
            }
        }
    }
}

fn geo_point(
    rtree: &RTree<GeoPoint>,
    mut candidates: RoaringBitmap,
    point: [f64; 2],
    ascending: bool,
) -> Box<dyn Iterator<Item = RoaringBitmap>> {
    let point = lat_lng_to_xyz(&point);

    let mut results = Vec::new();
    for point in rtree.nearest_neighbor_iter(&point) {
        if candidates.remove(point.data.0) {
            results.push(std::iter::once(point.data.0).collect());
            if candidates.is_empty() {
                break;
            }
        }
    }

    if ascending {
        Box::new(results.into_iter())
    } else {
        Box::new(results.into_iter().rev())
    }
}

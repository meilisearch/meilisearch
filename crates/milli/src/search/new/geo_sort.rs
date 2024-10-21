use std::collections::VecDeque;
use std::iter::FromIterator;

use heed::types::{Bytes, Unit};
use heed::{RoPrefix, RoTxn};
use roaring::RoaringBitmap;
use rstar::RTree;

use super::facet_string_values;
use super::ranking_rules::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait};
use crate::heed_codec::facet::{FieldDocIdFacetCodec, OrderedF64Codec};
use crate::score_details::{self, ScoreDetails};
use crate::{
    distance_between_two_points, lat_lng_to_xyz, GeoPoint, Index, Result, SearchContext,
    SearchLogger,
};

const FID_SIZE: usize = 2;
const DOCID_SIZE: usize = 4;

#[allow(clippy::drop_non_drop)]
fn facet_values_prefix_key(distinct: u16, id: u32) -> [u8; FID_SIZE + DOCID_SIZE] {
    concat_arrays::concat_arrays!(distinct.to_be_bytes(), id.to_be_bytes())
}

/// Return an iterator over each number value in the given field of the given document.
fn facet_number_values<'a>(
    docid: u32,
    field_id: u16,
    index: &Index,
    txn: &'a RoTxn<'a>,
) -> Result<RoPrefix<'a, FieldDocIdFacetCodec<OrderedF64Codec>, Unit>> {
    let key = facet_values_prefix_key(field_id, docid);

    let iter = index
        .field_id_docid_facet_f64s
        .remap_key_type::<Bytes>()
        .prefix_iter(txn, &key)?
        .remap_key_type();

    Ok(iter)
}

/// Define the strategy used by the geo sort.
/// The parameter represents the cache size, and, in the case of the Dynamic strategy,
/// the point where we move from using the iterative strategy to the rtree.
#[derive(Debug, Clone, Copy)]
pub enum Strategy {
    AlwaysIterative(usize),
    AlwaysRtree(usize),
    Dynamic(usize),
}

impl Default for Strategy {
    fn default() -> Self {
        Strategy::Dynamic(1000)
    }
}

impl Strategy {
    pub fn use_rtree(&self, candidates: usize) -> bool {
        match self {
            Strategy::AlwaysIterative(_) => false,
            Strategy::AlwaysRtree(_) => true,
            Strategy::Dynamic(i) => candidates >= *i,
        }
    }

    pub fn cache_size(&self) -> usize {
        match self {
            Strategy::AlwaysIterative(i) | Strategy::AlwaysRtree(i) | Strategy::Dynamic(i) => *i,
        }
    }
}

pub struct GeoSort<Q: RankingRuleQueryTrait> {
    query: Option<Q>,

    strategy: Strategy,
    ascending: bool,
    point: [f64; 2],
    field_ids: Option<[u16; 2]>,
    rtree: Option<RTree<GeoPoint>>,

    cached_sorted_docids: VecDeque<(u32, [f64; 2])>,
    geo_candidates: RoaringBitmap,
}

impl<Q: RankingRuleQueryTrait> GeoSort<Q> {
    pub fn new(
        strategy: Strategy,
        geo_faceted_docids: RoaringBitmap,
        point: [f64; 2],
        ascending: bool,
    ) -> Result<Self> {
        Ok(Self {
            query: None,
            strategy,
            ascending,
            point,
            geo_candidates: geo_faceted_docids,
            field_ids: None,
            rtree: None,
            cached_sorted_docids: VecDeque::new(),
        })
    }

    /// Refill the internal buffer of cached docids based on the strategy.
    /// Drop the rtree if we don't need it anymore.
    fn fill_buffer(
        &mut self,
        ctx: &mut SearchContext<'_>,
        geo_candidates: &RoaringBitmap,
    ) -> Result<()> {
        debug_assert!(self.field_ids.is_some(), "fill_buffer can't be called without the lat&lng");
        debug_assert!(self.cached_sorted_docids.is_empty());

        // lazily initialize the rtree if needed by the strategy, and cache it in `self.rtree`
        let rtree = if self.strategy.use_rtree(geo_candidates.len() as usize) {
            if let Some(rtree) = self.rtree.as_ref() {
                // get rtree from cache
                Some(rtree)
            } else {
                let rtree = ctx.index.geo_rtree(ctx.txn)?.expect("geo candidates but no rtree");
                // insert rtree in cache and returns it.
                // Can't use `get_or_insert_with` because getting the rtree from the DB is a fallible operation.
                Some(&*self.rtree.insert(rtree))
            }
        } else {
            None
        };

        let cache_size = self.strategy.cache_size();
        if let Some(rtree) = rtree {
            if self.ascending {
                let point = lat_lng_to_xyz(&self.point);
                for point in rtree.nearest_neighbor_iter(&point) {
                    if geo_candidates.contains(point.data.0) {
                        self.cached_sorted_docids.push_back(point.data);
                        if self.cached_sorted_docids.len() >= cache_size {
                            break;
                        }
                    }
                }
            } else {
                // in the case of the desc geo sort we look for the closest point to the opposite of the queried point
                // and we insert the points in reverse order they get reversed when emptying the cache later on
                let point = lat_lng_to_xyz(&opposite_of(self.point));
                for point in rtree.nearest_neighbor_iter(&point) {
                    if geo_candidates.contains(point.data.0) {
                        self.cached_sorted_docids.push_front(point.data);
                        if self.cached_sorted_docids.len() >= cache_size {
                            break;
                        }
                    }
                }
            }
        } else {
            // the iterative version
            let [lat, lng] = self.field_ids.unwrap();

            let mut documents = geo_candidates
                .iter()
                .map(|id| -> Result<_> { Ok((id, geo_value(id, lat, lng, ctx.index, ctx.txn)?)) })
                .collect::<Result<Vec<(u32, [f64; 2])>>>()?;
            // computing the distance between two points is expensive thus we cache the result
            documents
                .sort_by_cached_key(|(_, p)| distance_between_two_points(&self.point, p) as usize);
            self.cached_sorted_docids.extend(documents);
        };

        Ok(())
    }
}

/// Extracts the lat and long values from a single document.
///
/// If it is not able to find it in the facet number index it will extract it
/// from the facet string index and parse it as f64 (as the geo extraction behaves).
fn geo_value(
    docid: u32,
    field_lat: u16,
    field_lng: u16,
    index: &Index,
    rtxn: &RoTxn<'_>,
) -> Result<[f64; 2]> {
    let extract_geo = |geo_field: u16| -> Result<f64> {
        match facet_number_values(docid, geo_field, index, rtxn)?.next() {
            Some(Ok(((_, _, geo), ()))) => Ok(geo),
            Some(Err(e)) => Err(e.into()),
            None => match facet_string_values(docid, geo_field, index, rtxn)?.next() {
                Some(Ok((_, geo))) => {
                    Ok(geo.parse::<f64>().expect("cannot parse geo field as f64"))
                }
                Some(Err(e)) => Err(e.into()),
                None => panic!("A geo faceted document doesn't contain any lat or lng"),
            },
        }
    };

    let lat = extract_geo(field_lat)?;
    let lng = extract_geo(field_lng)?;

    Ok([lat, lng])
}

impl<'ctx, Q: RankingRuleQueryTrait> RankingRule<'ctx, Q> for GeoSort<Q> {
    fn id(&self) -> String {
        "geo_sort".to_owned()
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::geo_sort")]
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
        query: &Q,
    ) -> Result<()> {
        assert!(self.query.is_none());

        self.query = Some(query.clone());

        let geo_candidates = &self.geo_candidates & universe;

        if geo_candidates.is_empty() {
            return Ok(());
        }

        let fid_map = ctx.index.fields_ids_map(ctx.txn)?;
        let lat = fid_map.id("_geo.lat").expect("geo candidates but no fid for lat");
        let lng = fid_map.id("_geo.lng").expect("geo candidates but no fid for lng");
        self.field_ids = Some([lat, lng]);
        self.fill_buffer(ctx, &geo_candidates)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::geo_sort")]
    #[allow(clippy::only_used_in_recursion)]
    fn next_bucket(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Q>>> {
        let query = self.query.as_ref().unwrap().clone();

        let geo_candidates = &self.geo_candidates & universe;

        if geo_candidates.is_empty() {
            return Ok(Some(RankingRuleOutput {
                query,
                candidates: universe.clone(),
                score: ScoreDetails::GeoSort(score_details::GeoSort {
                    target_point: self.point,
                    ascending: self.ascending,
                    value: None,
                }),
            }));
        }

        let ascending = self.ascending;
        let next = |cache: &mut VecDeque<_>| {
            if ascending {
                cache.pop_front()
            } else {
                cache.pop_back()
            }
        };
        while let Some((id, point)) = next(&mut self.cached_sorted_docids) {
            if geo_candidates.contains(id) {
                return Ok(Some(RankingRuleOutput {
                    query,
                    candidates: RoaringBitmap::from_iter([id]),
                    score: ScoreDetails::GeoSort(score_details::GeoSort {
                        target_point: self.point,
                        ascending: self.ascending,
                        value: Some(point),
                    }),
                }));
            }
        }

        // if we got out of this loop it means we've exhausted our cache.
        // we need to refill it and run the function again.
        self.fill_buffer(ctx, &geo_candidates)?;
        self.next_bucket(ctx, logger, universe)
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::geo_sort")]
    fn end_iteration(&mut self, _ctx: &mut SearchContext<'ctx>, _logger: &mut dyn SearchLogger<Q>) {
        // we do not reset the rtree here, it could be used in a next iteration
        self.query = None;
        self.cached_sorted_docids.clear();
    }
}

/// Compute the antipodal coordinate of `coord`
fn opposite_of(mut coord: [f64; 2]) -> [f64; 2] {
    coord[0] *= -1.;
    // in the case of x,0 we want to return x,180
    if coord[1] > 0. {
        coord[1] -= 180.;
    } else {
        coord[1] += 180.;
    }

    coord
}

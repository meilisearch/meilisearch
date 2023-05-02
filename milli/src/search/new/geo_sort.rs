use std::collections::VecDeque;
use std::iter::FromIterator;

use heed::types::{ByteSlice, Unit};
use heed::{RoPrefix, RoTxn};
use roaring::RoaringBitmap;
use rstar::RTree;

use super::ranking_rules::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait};
use crate::heed_codec::facet::{FieldDocIdFacetCodec, OrderedF64Codec};
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
    txn: &'a RoTxn,
) -> Result<RoPrefix<'a, FieldDocIdFacetCodec<OrderedF64Codec>, Unit>> {
    let key = facet_values_prefix_key(field_id, docid);

    let iter = index
        .field_id_docid_facet_f64s
        .remap_key_type::<ByteSlice>()
        .prefix_iter(txn, &key)?
        .remap_key_type();

    Ok(iter)
}

/// Define the strategy used by the geo sort.
/// The paramater represents the cache size, and, in the case of the Dynamic strategy,
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

    cached_sorted_docids: VecDeque<u32>,
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
    fn fill_buffer(&mut self, ctx: &mut SearchContext) -> Result<()> {
        debug_assert!(self.field_ids.is_some(), "fill_buffer can't be called without the lat&lng");
        debug_assert!(self.cached_sorted_docids.is_empty());

        // if we had an rtree and the strategy doesn't require one anymore we can drop it
        let use_rtree = self.strategy.use_rtree(self.geo_candidates.len() as usize);
        if use_rtree && self.rtree.is_none() {
            self.rtree = Some(ctx.index.geo_rtree(ctx.txn)?.expect("geo candidates but no rtree"));
        }

        let cache_size = self.strategy.cache_size();
        if use_rtree {
            let rtree = self.rtree.as_ref().unwrap();
            let point = lat_lng_to_xyz(&self.point);

            if self.ascending {
                for point in rtree.nearest_neighbor_iter(&point) {
                    if self.geo_candidates.contains(point.data.0) {
                        self.cached_sorted_docids.push_back(point.data.0);
                        if self.cached_sorted_docids.len() >= cache_size {
                            break;
                        }
                    }
                }
            } else {
                // in the case of the desc geo sort we have to scan the whole database
                // and only keep the latest candidates.
                for point in rtree.nearest_neighbor_iter(&point) {
                    if self.geo_candidates.contains(point.data.0) {
                        // REVIEW COMMENT: that doesn't look right, because we only keep the furthest point in the cache.
                        // Then the cache will be exhausted after the first bucket and we'll need to repopulate it again immediately.
                        // I think it's okay if we keep every document id in the cache instead. It's a high memory usage,
                        // but we already have the whole rtree in memory, which is bigger than a vector of all document ids.
                        //
                        //      self.cached_sorted_docids.pop_front();
                        //
                        self.cached_sorted_docids.push_back(point.data.0);
                    }
                }
            }
        } else {
            // the iterative version
            let [lat, lng] = self.field_ids.unwrap();

            let mut documents = self
                .geo_candidates
                .iter()
                .map(|id| -> Result<_> {
                    Ok((
                        id,
                        [
                            facet_number_values(id, lat, ctx.index, ctx.txn)?
                                .next()
                                .expect("A geo faceted document doesn't contain any lat")?
                                .0
                                 .2,
                            facet_number_values(id, lng, ctx.index, ctx.txn)?
                                .next()
                                .expect("A geo faceted document doesn't contain any lng")?
                                .0
                                 .2,
                        ],
                    ))
                })
                .collect::<Result<Vec<(u32, [f64; 2])>>>()?;
            // computing the distance between two points is expensive thus we cache the result
            documents
                .sort_by_cached_key(|(_, p)| distance_between_two_points(&self.point, p) as usize);
            self.cached_sorted_docids.extend(documents.into_iter().map(|(doc_id, _)| doc_id));
        };

        Ok(())
    }
}

impl<'ctx, Q: RankingRuleQueryTrait> RankingRule<'ctx, Q> for GeoSort<Q> {
    fn id(&self) -> String {
        "geo_sort".to_owned()
    }

    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
        query: &Q,
    ) -> Result<()> {
        assert!(self.query.is_none());

        self.query = Some(query.clone());
        self.geo_candidates &= universe;

        if self.geo_candidates.is_empty() {
            return Ok(());
        }

        let fid_map = ctx.index.fields_ids_map(ctx.txn)?;
        let lat = fid_map.id("_geo.lat").expect("geo candidates but no fid for lat");
        let lng = fid_map.id("_geo.lng").expect("geo candidates but no fid for lng");
        self.field_ids = Some([lat, lng]);
        self.fill_buffer(ctx)?;
        Ok(())
    }

    #[allow(clippy::only_used_in_recursion)]
    fn next_bucket(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Q>>> {
        assert!(universe.len() > 1);
        let query = self.query.as_ref().unwrap().clone();
        self.geo_candidates &= universe;

        if self.geo_candidates.is_empty() {
            return Ok(Some(RankingRuleOutput { query, candidates: universe.clone() }));
        }

        let ascending = self.ascending;
        let next = |cache: &mut VecDeque<_>| {
            if ascending {
                cache.pop_front()
            } else {
                cache.pop_back()
            }
        };
        while let Some(id) = next(&mut self.cached_sorted_docids) {
            if self.geo_candidates.contains(id) {
                return Ok(Some(RankingRuleOutput {
                    query,
                    candidates: RoaringBitmap::from_iter([id]),
                }));
            }
        }

        // if we got out of this loop it means we've exhausted our cache.
        // we need to refill it and run the function again.
        self.fill_buffer(ctx)?;
        self.next_bucket(ctx, logger, universe)
    }

    fn end_iteration(&mut self, _ctx: &mut SearchContext<'ctx>, _logger: &mut dyn SearchLogger<Q>) {
        // we do not reset the rtree here, it could be used in a next iteration
        self.query = None;
        self.cached_sorted_docids.clear();
    }
}

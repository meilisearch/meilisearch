use std::collections::VecDeque;

use heed::types::{Bytes, Unit};
use heed::{RoPrefix, RoTxn};
use roaring::RoaringBitmap;
use rstar::RTree;

use super::facet_string_values;
use super::ranking_rules::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait};
use crate::documents::geo_sort::{fill_cache, next_bucket};
use crate::heed_codec::facet::{FieldDocIdFacetCodec, OrderedF64Codec};
use crate::score_details::{self, ScoreDetails};
use crate::{GeoPoint, Index, Result, SearchContext, SearchLogger};

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

#[derive(Debug, Clone, Copy)]
pub struct Parameter {
    // Define the strategy used by the geo sort
    pub strategy: Strategy,
    // Limit the number of docs in a single bucket to avoid unexpectedly large overhead
    pub max_bucket_size: u64,
    // Considering the errors of GPS and geographical calculations, distances less than distance_error_margin will be treated as equal
    pub distance_error_margin: f64,
}

impl Default for Parameter {
    fn default() -> Self {
        Self { strategy: Strategy::default(), max_bucket_size: 1000, distance_error_margin: 1.0 }
    }
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

    // Limit the number of docs in a single bucket to avoid unexpectedly large overhead
    max_bucket_size: u64,
    // Considering the errors of GPS and geographical calculations, distances less than distance_error_margin will be treated as equal
    distance_error_margin: f64,
}

impl<Q: RankingRuleQueryTrait> GeoSort<Q> {
    pub fn new(
        parameter: Parameter,
        geo_faceted_docids: RoaringBitmap,
        point: [f64; 2],
        ascending: bool,
    ) -> Result<Self> {
        let Parameter { strategy, max_bucket_size, distance_error_margin } = parameter;
        Ok(Self {
            query: None,
            strategy,
            ascending,
            point,
            geo_candidates: geo_faceted_docids,
            field_ids: None,
            rtree: None,
            cached_sorted_docids: VecDeque::new(),
            max_bucket_size,
            distance_error_margin,
        })
    }

    /// Refill the internal buffer of cached docids based on the strategy.
    /// Drop the rtree if we don't need it anymore.
    fn fill_buffer(
        &mut self,
        ctx: &mut SearchContext<'_>,
        geo_candidates: &RoaringBitmap,
    ) -> Result<()> {
        fill_cache(
            ctx.index,
            ctx.txn,
            self.strategy,
            self.ascending,
            self.point,
            &self.field_ids,
            &mut self.rtree,
            geo_candidates,
            &mut self.cached_sorted_docids,
        )?;

        Ok(())
    }
}

/// Extracts the lat and long values from a single document.
///
/// If it is not able to find it in the facet number index it will extract it
/// from the facet string index and parse it as f64 (as the geo extraction behaves).
pub(crate) fn geo_value(
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
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Q>>> {
        let query = self.query.as_ref().unwrap().clone();

        next_bucket(
            ctx.index,
            ctx.txn,
            universe,
            self.strategy,
            self.ascending,
            self.point,
            &self.field_ids,
            &mut self.rtree,
            &mut self.cached_sorted_docids,
            &self.geo_candidates,
            self.max_bucket_size,
            self.distance_error_margin,
        )
        .map(|o| {
            o.map(|(candidates, point)| RankingRuleOutput {
                query,
                candidates,
                score: ScoreDetails::GeoSort(score_details::GeoSort {
                    target_point: self.point,
                    ascending: self.ascending,
                    value: point,
                }),
            })
        })
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::geo_sort")]
    fn end_iteration(&mut self, _ctx: &mut SearchContext<'ctx>, _logger: &mut dyn SearchLogger<Q>) {
        // we do not reset the rtree here, it could be used in a next iteration
        self.query = None;
        self.cached_sorted_docids.clear();
    }
}

/// Compute the antipodal coordinate of `coord`
pub(crate) fn opposite_of(mut coord: [f64; 2]) -> [f64; 2] {
    coord[0] *= -1.;
    // in the case of x,0 we want to return x,180
    if coord[1] > 0. {
        coord[1] -= 180.;
    } else {
        coord[1] += 180.;
    }

    coord
}

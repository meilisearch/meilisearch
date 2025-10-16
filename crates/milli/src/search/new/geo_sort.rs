use std::collections::VecDeque;

use roaring::RoaringBitmap;
use rstar::RTree;

use super::ranking_rules::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait};
use crate::documents::geo_sort::{fill_cache, next_bucket};
use crate::documents::{GeoSortParameter, GeoSortStrategy};
use crate::score_details::{self, ScoreDetails};
use crate::{GeoPoint, Result, SearchContext, SearchLogger, TimeBudget};

pub struct GeoSort<Q: RankingRuleQueryTrait> {
    query: Option<Q>,

    strategy: GeoSortStrategy,
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
        parameter: GeoSortParameter,
        geo_faceted_docids: RoaringBitmap,
        point: [f64; 2],
        ascending: bool,
    ) -> Result<Self> {
        let GeoSortParameter { strategy, max_bucket_size, distance_error_margin } = parameter;
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
        _time_budget: &TimeBudget,
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
        _time_budget: &TimeBudget,
    ) -> Result<Option<RankingRuleOutput<Q>>> {
        let query = self.query.as_ref().unwrap().clone();

        next_bucket(
            ctx.index,
            ctx.txn,
            universe,
            self.ascending,
            self.point,
            &self.field_ids,
            &mut self.rtree,
            &mut self.cached_sorted_docids,
            &self.geo_candidates,
            GeoSortParameter {
                strategy: self.strategy,
                max_bucket_size: self.max_bucket_size,
                distance_error_margin: self.distance_error_margin,
            },
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

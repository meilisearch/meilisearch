use std::iter::FromIterator;
use std::task::Poll;
use std::time::Instant;

use itertools::Itertools;
use roaring::RoaringBitmap;

use super::ranking_rules::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait};
use super::VectorStoreStats;
use crate::progress::Progress;
use crate::score_details::{self, ScoreDetails};
use crate::search::new::ranking_rules::RankingRuleId;
use crate::search::steps::{ComputingBucketSortStep, RankingRuleStep};
use crate::vector::{DistributionShift, Embedder, VectorStore};
use crate::{DocumentId, Result, SearchContext, SearchLogger, TimeBudget};

pub struct VectorSort<Q: RankingRuleQueryTrait> {
    query: Option<Q>,
    target: Vec<f32>,
    vector_candidates: RoaringBitmap,
    cached_sorted_docids: itertools::ChunkBy<
        f32,
        std::vec::IntoIter<(DocumentId, f32)>,
        for<'a> fn(&'a (DocumentId, f32)) -> f32,
    >,
    limit: usize,
    distribution_shift: Option<DistributionShift>,
    embedder_index: u8,
    quantized: bool,
}

impl<Q: RankingRuleQueryTrait> VectorSort<Q> {
    pub fn new(
        ctx: &SearchContext<'_>,
        target: Vec<f32>,
        vector_candidates: RoaringBitmap,
        limit: usize,
        embedder_name: &str,
        embedder: &Embedder,
        quantized: bool,
    ) -> Result<Self> {
        let embedder_index = ctx
            .index
            .embedding_configs()
            .embedder_id(ctx.txn, embedder_name)?
            .ok_or_else(|| crate::UserError::InvalidSearchEmbedder(embedder_name.to_owned()))?;

        Ok(Self {
            query: None,
            target,
            vector_candidates,
            cached_sorted_docids: vec![].into_iter().chunk_by(by_distance),
            limit,
            distribution_shift: embedder.distribution(),
            embedder_index,
            quantized,
        })
    }

    fn fill_buffer(
        &mut self,
        ctx: &mut SearchContext<'_>,
        vector_candidates: &RoaringBitmap,
        time_budget: &TimeBudget,
    ) -> Result<usize> {
        let target = &self.target;
        let backend = ctx.index.get_vector_store(ctx.txn)?.unwrap_or_default();

        let before = Instant::now();
        let reader =
            VectorStore::new(backend, ctx.index.vector_store, self.embedder_index, self.quantized);
        let results = reader.nns_by_vector(
            ctx.txn,
            target,
            self.limit,
            Some(vector_candidates),
            time_budget,
        )?;
        let total_results = results.len();
        self.cached_sorted_docids = results.into_iter().chunk_by(by_distance);
        *ctx.vector_store_stats.get_or_insert_default() +=
            VectorStoreStats { total_time: before.elapsed(), total_queries: 1, total_results };

        Ok(total_results)
    }

    fn next_results(&mut self, vector_candidates: &RoaringBitmap) -> Option<(RoaringBitmap, f32)> {
        for (distance, group) in &self.cached_sorted_docids {
            let mut candidates = RoaringBitmap::from_iter(group.map(|(docid, _)| docid));
            candidates &= vector_candidates;

            if !candidates.is_empty() {
                let score = 1.0 - distance;
                let score = self
                    .distribution_shift
                    .map(|distribution| distribution.shift(score))
                    .unwrap_or(score);
                return Some((candidates, score));
            }
        }
        None
    }
}

fn by_distance((_docid, distance): &(DocumentId, f32)) -> f32 {
    *distance
}

impl<'ctx, Q: RankingRuleQueryTrait> RankingRule<'ctx, Q> for VectorSort<Q> {
    fn id(&self) -> RankingRuleId {
        RankingRuleId::VectorSort
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::vector_sort")]
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
        query: &Q,
        time_budget: &TimeBudget,
        progress: &Progress,
    ) -> Result<()> {
        progress.update_progress(ComputingBucketSortStep::from(self.id()));
        let _step = progress.update_progress_scoped(RankingRuleStep::StartIteration);
        assert!(self.query.is_none());

        self.query = Some(query.clone());
        let vector_candidates = &self.vector_candidates & universe;
        self.fill_buffer(ctx, &vector_candidates, time_budget)?;
        Ok(())
    }

    #[allow(clippy::only_used_in_recursion)]
    #[tracing::instrument(level = "trace", skip_all, target = "search::vector_sort")]
    fn next_bucket(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
        time_budget: &TimeBudget,
        progress: &Progress,
    ) -> Result<Option<RankingRuleOutput<Q>>> {
        progress.update_progress(ComputingBucketSortStep::from(self.id()));
        let _step = progress.update_progress_scoped(RankingRuleStep::NextBucket);
        let query = self.query.as_ref().unwrap().clone();
        let vector_candidates = &self.vector_candidates & universe;

        if vector_candidates.is_empty() {
            return Ok(Some(RankingRuleOutput {
                query,
                candidates: universe.clone(),
                score: ScoreDetails::Vector(score_details::Vector { similarity: None }),
            }));
        }

        if let Some((candidates, score)) = self.next_results(&vector_candidates) {
            return Ok(Some(RankingRuleOutput {
                query,
                candidates,
                score: ScoreDetails::Vector(score_details::Vector { similarity: Some(score) }),
            }));
        }

        // if we got out of this loop it means we've exhausted our cache.
        // we need to refill it and run the function again.
        let total_results = self.fill_buffer(ctx, &vector_candidates, time_budget)?;

        // we tried filling the buffer, but it remained empty 😢
        // it means we don't actually have any document remaining in the universe with a vector.
        // => exit
        if total_results == 0 {
            return Ok(Some(RankingRuleOutput {
                query,
                candidates: universe.clone(),
                score: ScoreDetails::Vector(score_details::Vector { similarity: None }),
            }));
        }

        self.next_bucket(ctx, _logger, universe, time_budget, progress)
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::vector_sort")]
    fn end_iteration(&mut self, _ctx: &mut SearchContext<'ctx>, _logger: &mut dyn SearchLogger<Q>) {
        self.query = None;
    }

    fn non_blocking_next_bucket(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
        progress: &Progress,
    ) -> Result<Poll<RankingRuleOutput<Q>>> {
        let _step = progress.update_progress_scoped(RankingRuleStep::NonBlockingNextBucket);
        let query = self.query.as_ref().unwrap().clone();
        let vector_candidates = &self.vector_candidates & universe;

        if vector_candidates.is_empty() {
            return Ok(Poll::Ready(RankingRuleOutput {
                query,
                candidates: universe.clone(),
                score: ScoreDetails::Vector(score_details::Vector { similarity: None }),
            }));
        }

        if let Some((candidates, score)) = self.next_results(&vector_candidates) {
            Ok(Poll::Ready(RankingRuleOutput {
                query,
                candidates,
                score: ScoreDetails::Vector(score_details::Vector { similarity: Some(score) }),
            }))
        } else {
            Ok(Poll::Pending)
        }
    }
}

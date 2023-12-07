use std::future::Future;
use std::iter::FromIterator;
use std::pin::Pin;

use nolife::DynBoxScope;
use roaring::RoaringBitmap;

use super::ranking_rules::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait};
use crate::distance::NDotProductPoint;
use crate::index::Hnsw;
use crate::score_details::{self, ScoreDetails};
use crate::{Result, SearchContext, SearchLogger, UserError};

pub struct VectorSort<'ctx, Q: RankingRuleQueryTrait> {
    query: Option<Q>,
    target: Vec<f32>,
    vector_candidates: RoaringBitmap,
    reader: arroy::Reader<'ctx, arroy::distances::DotProduct>,
    limit: usize,
}

impl<'ctx, Q: RankingRuleQueryTrait> VectorSort<'ctx, Q> {
    pub fn new(
        ctx: &'ctx SearchContext,
        target: Vec<f32>,
        vector_candidates: RoaringBitmap,
        limit: usize,
    ) -> Result<Self> {
        /// FIXME? what to do in case of missing metadata
        let reader = arroy::Reader::open(ctx.txn, 0, ctx.index.vector_arroy)?;

        let target_clone = target.clone();

        Ok(Self { query: None, target, vector_candidates, reader, limit })
    }
}

impl<'ctx, Q: RankingRuleQueryTrait> RankingRule<'ctx, Q> for VectorSort<'ctx, Q> {
    fn id(&self) -> String {
        "vector_sort".to_owned()
    }

    fn start_iteration(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
        query: &Q,
    ) -> Result<()> {
        assert!(self.query.is_none());

        self.query = Some(query.clone());
        self.vector_candidates &= universe;

        Ok(())
    }

    #[allow(clippy::only_used_in_recursion)]
    fn next_bucket(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Q>>> {
        let query = self.query.as_ref().unwrap().clone();
        self.vector_candidates &= universe;

        if self.vector_candidates.is_empty() {
            return Ok(Some(RankingRuleOutput {
                query,
                candidates: universe.clone(),
                score: ScoreDetails::Vector(score_details::Vector {
                    target_vector: self.target.clone(),
                    value_similarity: None,
                }),
            }));
        }
        let target = &self.target;
        let vector_candidates = &self.vector_candidates;

        let result = self.reader.nns_by_vector(ctx.txn, &target, count, search_k, candidates)

        scope.enter(|it| {
            for item in it.by_ref() {
                let item: Item = item;
                let index = item.pid.into_inner();
                let docid = ctx.index.vector_id_docid.get(ctx.txn, &index)?.unwrap();

                if vector_candidates.contains(docid) {
                    return Ok(Some(RankingRuleOutput {
                        query,
                        candidates: RoaringBitmap::from_iter([docid]),
                        score: ScoreDetails::Vector(score_details::Vector {
                            target_vector: target.clone(),
                            value_similarity: Some((
                                item.point.clone().into_inner(),
                                1.0 - item.distance,
                            )),
                        }),
                    }));
                }
            }
            Ok(Some(RankingRuleOutput {
                query,
                candidates: universe.clone(),
                score: ScoreDetails::Vector(score_details::Vector {
                    target_vector: target.clone(),
                    value_similarity: None,
                }),
            }))
        })
    }

    fn end_iteration(&mut self, _ctx: &mut SearchContext<'ctx>, _logger: &mut dyn SearchLogger<Q>) {
        self.query = None;
    }
}

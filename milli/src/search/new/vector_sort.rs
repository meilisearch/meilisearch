use std::iter::FromIterator;

use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use super::ranking_rules::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait};
use crate::score_details::{self, ScoreDetails};
use crate::{DocumentId, Result, SearchContext, SearchLogger};

pub struct VectorSort<Q: RankingRuleQueryTrait> {
    query: Option<Q>,
    target: Vec<f32>,
    vector_candidates: RoaringBitmap,
    cached_sorted_docids: std::vec::IntoIter<(DocumentId, f32, Vec<f32>)>,
    limit: usize,
}

impl<Q: RankingRuleQueryTrait> VectorSort<Q> {
    pub fn new(
        _ctx: &SearchContext,
        target: Vec<f32>,
        vector_candidates: RoaringBitmap,
        limit: usize,
    ) -> Result<Self> {
        Ok(Self {
            query: None,
            target,
            vector_candidates,
            cached_sorted_docids: Default::default(),
            limit,
        })
    }

    fn fill_buffer(&mut self, ctx: &mut SearchContext<'_>) -> Result<()> {
        let readers: std::result::Result<Vec<_>, _> = (0..=u8::MAX)
            .map_while(|k| {
                arroy::Reader::open(ctx.txn, k.into(), ctx.index.vector_arroy)
                    .map(Some)
                    .or_else(|e| match e {
                        arroy::Error::MissingMetadata => Ok(None),
                        e => Err(e),
                    })
                    .transpose()
            })
            .collect();

        let readers = readers?;

        let target = &self.target;
        let mut results = Vec::new();

        for reader in readers.iter() {
            let nns_by_vector = reader.nns_by_vector(
                ctx.txn,
                &target,
                self.limit,
                None,
                Some(&self.vector_candidates),
            )?;
            let vectors: std::result::Result<Vec<_>, _> = nns_by_vector
                .iter()
                .map(|(docid, _)| reader.item_vector(ctx.txn, *docid).transpose().unwrap())
                .collect();
            let vectors = vectors?;
            results.extend(nns_by_vector.into_iter().zip(vectors).map(|((x, y), z)| (x, y, z)));
        }
        results.sort_unstable_by_key(|(_, distance, _)| OrderedFloat(*distance));
        self.cached_sorted_docids = results.into_iter();
        Ok(())
    }
}

impl<'ctx, Q: RankingRuleQueryTrait> RankingRule<'ctx, Q> for VectorSort<Q> {
    fn id(&self) -> String {
        "vector_sort".to_owned()
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
        self.vector_candidates &= universe;
        self.fill_buffer(ctx)?;
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

        while let Some((docid, distance, vector)) = self.cached_sorted_docids.next() {
            if self.vector_candidates.contains(docid) {
                return Ok(Some(RankingRuleOutput {
                    query,
                    candidates: RoaringBitmap::from_iter([docid]),
                    score: ScoreDetails::Vector(score_details::Vector {
                        target_vector: self.target.clone(),
                        value_similarity: Some((vector, 1.0 - distance)),
                    }),
                }));
            }
        }

        // if we got out of this loop it means we've exhausted our cache.
        // we need to refill it and run the function again.
        self.fill_buffer(ctx)?;
        self.next_bucket(ctx, _logger, universe)
    }

    fn end_iteration(&mut self, _ctx: &mut SearchContext<'ctx>, _logger: &mut dyn SearchLogger<Q>) {
        self.query = None;
    }
}

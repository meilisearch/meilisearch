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

pub struct VectorSort<Q: RankingRuleQueryTrait> {
    query: Option<Q>,
    target: Vec<f32>,
    vector_candidates: RoaringBitmap,
    scope: nolife::DynBoxScope<SearchFamily>,
}

type Item<'a> = instant_distance::Item<'a, NDotProductPoint>;
type SearchFut = Pin<Box<dyn Future<Output = nolife::Never>>>;

struct SearchFamily;
impl<'a> nolife::Family<'a> for SearchFamily {
    type Family = Box<dyn Iterator<Item = Item<'a>> + 'a>;
}

async fn search_scope(
    mut time_capsule: nolife::TimeCapsule<SearchFamily>,
    hnsw: Hnsw,
    target: Vec<f32>,
) -> nolife::Never {
    let mut search = instant_distance::Search::default();
    let it = Box::new(hnsw.search(&NDotProductPoint::new(target), &mut search));
    let mut it: Box<dyn Iterator<Item = Item>> = it;
    loop {
        time_capsule.freeze(&mut it).await;
    }
}

impl<Q: RankingRuleQueryTrait> VectorSort<Q> {
    pub fn new(
        ctx: &SearchContext,
        target: Vec<f32>,
        vector_candidates: RoaringBitmap,
    ) -> Result<Self> {
        let hnsw =
            ctx.index.vector_hnsw(ctx.txn)?.unwrap_or(Hnsw::builder().build_hnsw(Vec::default()).0);

        if let Some(expected_size) = hnsw.iter().map(|(_, point)| point.len()).next() {
            if target.len() != expected_size {
                return Err(UserError::InvalidVectorDimensions {
                    expected: expected_size,
                    found: target.len(),
                }
                .into());
            }
        }

        let target_clone = target.clone();
        let producer = move |time_capsule| -> SearchFut {
            Box::pin(search_scope(time_capsule, hnsw, target_clone))
        };
        let scope = DynBoxScope::new(producer);

        Ok(Self { query: None, target, vector_candidates, scope })
    }
}

impl<'ctx, Q: RankingRuleQueryTrait> RankingRule<'ctx, Q> for VectorSort<Q> {
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

        let scope = &mut self.scope;
        let target = &self.target;
        let vector_candidates = &self.vector_candidates;

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

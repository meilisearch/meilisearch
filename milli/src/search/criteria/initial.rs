use roaring::RoaringBitmap;

use super::{Criterion, CriterionParameters, CriterionResult};
use crate::search::criteria::{resolve_query_tree, Context};
use crate::search::query_tree::Operation;
use crate::search::Distinct;
use crate::Result;

pub struct Initial<'t, D> {
    ctx: &'t dyn Context<'t>,
    answer: Option<CriterionResult>,
    exhaustive_number_hits: bool,
    distinct: Option<D>,
}

impl<'t, D> Initial<'t, D> {
    pub fn new(
        ctx: &'t dyn Context<'t>,
        query_tree: Option<Operation>,
        filtered_candidates: Option<RoaringBitmap>,
        exhaustive_number_hits: bool,
        distinct: Option<D>,
    ) -> Initial<D> {
        let answer = CriterionResult {
            query_tree,
            candidates: None,
            filtered_candidates,
            bucket_candidates: None,
        };
        Initial { ctx, answer: Some(answer), exhaustive_number_hits, distinct }
    }
}

impl<D: Distinct> Criterion for Initial<'_, D> {
    #[logging_timer::time("Initial::{}")]
    fn next(&mut self, params: &mut CriterionParameters) -> Result<Option<CriterionResult>> {
        self.answer
            .take()
            .map(|mut answer| {
                if self.exhaustive_number_hits && answer.query_tree.is_some() {
                    let candidates = resolve_query_tree(
                        self.ctx,
                        answer.query_tree.as_ref().unwrap(),
                        &mut params.wdcache,
                    )?;

                    let bucket_candidates = match &mut self.distinct {
                        // may be really time consuming
                        Some(distinct) => {
                            let mut bucket_candidates = RoaringBitmap::new();
                            for c in distinct.distinct(candidates.clone(), RoaringBitmap::new()) {
                                bucket_candidates.insert(c?);
                            }
                            bucket_candidates
                        }
                        None => candidates.clone(),
                    };

                    answer.candidates = Some(candidates);
                    answer.bucket_candidates = Some(bucket_candidates);
                }
                Ok(answer)
            })
            .transpose()
    }
}

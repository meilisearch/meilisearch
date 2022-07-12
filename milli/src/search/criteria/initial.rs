use roaring::RoaringBitmap;

use super::{Criterion, CriterionParameters, CriterionResult};
use crate::search::criteria::{resolve_query_tree, Context};
use crate::search::query_tree::Operation;
use crate::Result;

pub struct Initial<'t> {
    ctx: &'t dyn Context<'t>,
    answer: Option<CriterionResult>,
    exhaustive_number_hits: bool,
}

impl<'t> Initial<'t> {
    pub fn new(
        ctx: &'t dyn Context<'t>,
        query_tree: Option<Operation>,
        filtered_candidates: Option<RoaringBitmap>,
        exhaustive_number_hits: bool,
    ) -> Initial {
        let answer = CriterionResult {
            query_tree,
            candidates: None,
            filtered_candidates,
            bucket_candidates: None,
        };
        Initial { ctx, answer: Some(answer), exhaustive_number_hits }
    }
}

impl Criterion for Initial<'_> {
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

                    answer.candidates = Some(candidates.clone());
                    answer.bucket_candidates = Some(candidates);
                }
                Ok(answer)
            })
            .transpose()
    }
}

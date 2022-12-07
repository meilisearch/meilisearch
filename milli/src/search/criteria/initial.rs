use roaring::RoaringBitmap;

use super::{Criterion, CriterionParameters, CriterionResult};
use crate::search::criteria::{resolve_query_tree, Context, InitialCandidates};
use crate::search::query_tree::Operation;
use crate::search::Distinct;
use crate::Result;
/// Initial is a mandatory criterion, it is always the first
/// and is meant to initalize the CriterionResult used by the other criteria.
/// It behave like an [Once Iterator](https://doc.rust-lang.org/std/iter/struct.Once.html) and will return Some(CriterionResult) only one time.
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
            initial_candidates: None,
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
                    // resolve the whole query tree to retrieve an exhaustive list of documents matching the query.
                    // then remove the potential soft deleted documents.
                    let mut candidates = resolve_query_tree(
                        self.ctx,
                        answer.query_tree.as_ref().unwrap(),
                        params.wdcache,
                    )? - params.excluded_candidates;

                    // Apply the filters on the documents retrieved with the query tree.
                    if let Some(ref filtered_candidates) = answer.filtered_candidates {
                        candidates &= filtered_candidates;
                    }

                    // because the initial_candidates should be an exhaustive count of the matching documents,
                    // we precompute the distinct attributes.
                    let initial_candidates = match &mut self.distinct {
                        Some(distinct) => {
                            let mut initial_candidates = RoaringBitmap::new();
                            for c in distinct.distinct(candidates.clone(), RoaringBitmap::new()) {
                                initial_candidates.insert(c?);
                            }
                            initial_candidates
                        }
                        None => candidates.clone(),
                    };

                    answer.candidates = Some(candidates);
                    answer.initial_candidates =
                        Some(InitialCandidates::Exhaustive(initial_candidates));
                }
                Ok(answer)
            })
            .transpose()
    }
}

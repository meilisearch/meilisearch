use roaring::RoaringBitmap;

use crate::search::query_tree::Operation;

use super::{Criterion, CriterionResult, CriterionParameters};

pub struct Initial {
    answer: Option<CriterionResult>
}

impl Initial {
    pub fn new(query_tree: Option<Operation>, mut candidates: Option<RoaringBitmap>) -> Initial {
        let answer = CriterionResult {
            query_tree,
            candidates: candidates.clone(),
            bucket_candidates: candidates.take().unwrap_or_default(),
        };
        Initial { answer: Some(answer) }
    }
}

impl Criterion for Initial {
    #[logging_timer::time("Initial::{}")]
    fn next(&mut self, _: &mut CriterionParameters) -> anyhow::Result<Option<CriterionResult>> {
        Ok(self.answer.take())
    }
}

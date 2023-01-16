use log::debug;
use roaring::RoaringBitmap;

use super::{resolve_query_tree, Context, Criterion, CriterionParameters, CriterionResult};
use crate::search::criteria::InitialCandidates;
use crate::search::query_tree::Operation;
use crate::search::WordDerivationsCache;
use crate::Result;

/// The result of a call to the fetcher.
#[derive(Debug, Clone, PartialEq)]
pub struct FinalResult {
    /// The query tree corresponding to the current bucket of the last criterion.
    pub query_tree: Option<Operation>,
    /// The candidates of the current bucket of the last criterion.
    pub candidates: RoaringBitmap,
    /// Candidates that comes from the current bucket of the initial criterion.
    pub initial_candidates: InitialCandidates,
}

pub struct Final<'t> {
    ctx: &'t dyn Context<'t>,
    parent: Box<dyn Criterion + 't>,
    wdcache: WordDerivationsCache,
    returned_candidates: RoaringBitmap,
}

impl<'t> Final<'t> {
    pub fn new(ctx: &'t dyn Context<'t>, parent: Box<dyn Criterion + 't>) -> Final<'t> {
        Final {
            ctx,
            parent,
            wdcache: WordDerivationsCache::new(),
            returned_candidates: RoaringBitmap::new(),
        }
    }

    #[logging_timer::time("Final::{}")]
    pub fn next(&mut self, excluded_candidates: &RoaringBitmap) -> Result<Option<FinalResult>> {
        debug!("Final iteration");
        let excluded_candidates = &self.returned_candidates | excluded_candidates;
        let mut criterion_parameters = CriterionParameters {
            wdcache: &mut self.wdcache,
            // returned_candidates is merged with excluded_candidates to avoid duplicas
            excluded_candidates: &excluded_candidates,
        };

        match self.parent.next(&mut criterion_parameters)? {
            Some(CriterionResult {
                query_tree,
                candidates,
                filtered_candidates,
                initial_candidates,
            }) => {
                let mut candidates = match (candidates, query_tree.as_ref()) {
                    (Some(candidates), _) => candidates,
                    (None, Some(qt)) => {
                        resolve_query_tree(self.ctx, qt, &mut self.wdcache)? - excluded_candidates
                    }
                    (None, None) => self.ctx.documents_ids()? - excluded_candidates,
                };

                if let Some(filtered_candidates) = filtered_candidates {
                    candidates &= filtered_candidates;
                }

                let initial_candidates = initial_candidates
                    .unwrap_or_else(|| InitialCandidates::Estimated(candidates.clone()));

                self.returned_candidates |= &candidates;

                Ok(Some(FinalResult { query_tree, candidates, initial_candidates }))
            }
            None => Ok(None),
        }
    }
}

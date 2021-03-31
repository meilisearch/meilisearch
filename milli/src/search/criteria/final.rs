use std::collections::HashMap;

use log::debug;
use roaring::RoaringBitmap;

use crate::search::query_tree::Operation;
use crate::search::WordDerivationsCache;
use super::{resolve_query_tree, Criterion, CriterionResult, Context};

/// The result of a call to the fetcher.
#[derive(Debug, Clone, PartialEq)]
pub struct FinalResult {
    /// The query tree corresponding to the current bucket of the last criterion.
    pub query_tree: Option<Operation>,
    /// The candidates of the current bucket of the last criterion.
    pub candidates: RoaringBitmap,
    /// Candidates that comes from the current bucket of the initial criterion.
    pub bucket_candidates: RoaringBitmap,
}

pub struct Final<'t> {
    ctx: &'t dyn Context<'t>,
    parent: Box<dyn Criterion + 't>,
    wdcache: WordDerivationsCache,
}

impl<'t> Final<'t> {
    pub fn new(ctx: &'t dyn Context<'t>, parent: Box<dyn Criterion + 't>) -> Final<'t> {
        Final { ctx, parent, wdcache: WordDerivationsCache::new() }
    }

    #[logging_timer::time("Final::{}")]
    pub fn next(&mut self) -> anyhow::Result<Option<FinalResult>> {
        loop {
            debug!("Final iteration");

            match self.parent.next(&mut self.wdcache)? {
                Some(CriterionResult { query_tree, candidates, mut bucket_candidates }) => {
                    let candidates = match (&query_tree, candidates) {
                        (_, Some(candidates)) => candidates,
                        (Some(qt), None) => resolve_query_tree(self.ctx, qt, &mut HashMap::new(), &mut self.wdcache)?,
                        (None, None) => self.ctx.documents_ids()?,
                    };

                    bucket_candidates.union_with(&candidates);

                    return Ok(Some(FinalResult {
                        query_tree,
                        candidates,
                        bucket_candidates,
                    }));
                },
                None => return Ok(None),
            }
        }
    }
}

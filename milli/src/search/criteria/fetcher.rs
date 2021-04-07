use std::collections::HashMap;
use std::mem::take;

use log::debug;
use roaring::RoaringBitmap;

use crate::search::query_tree::Operation;
use crate::search::WordDerivationsCache;
use super::{resolve_query_tree, Candidates, Criterion, CriterionResult, Context, CriterionContext};

/// The result of a call to the fetcher.
#[derive(Debug, Clone, PartialEq)]
pub struct FetcherResult {
    /// The query tree corresponding to the current bucket of the last criterion.
    pub query_tree: Option<Operation>,
    /// The candidates of the current bucket of the last criterion.
    pub candidates: RoaringBitmap,
    /// Candidates that comes from the current bucket of the initial criterion.
    pub bucket_candidates: RoaringBitmap,
}

pub struct Fetcher<'t> {
    ctx: &'t dyn Context,
    query_tree: Option<Operation>,
    candidates: Candidates,
    parent: Option<Box<dyn Criterion + 't>>,
    should_get_documents_ids: bool,
    wdcache: WordDerivationsCache,
}

impl<'t> Fetcher<'t> {
    pub fn initial(
        ctx: &'t dyn Context,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
    ) -> Self
    {
        Fetcher {
            ctx,
            query_tree,
            candidates: candidates.map_or_else(Candidates::default, Candidates::Allowed),
            parent: None,
            should_get_documents_ids: true,
            wdcache: WordDerivationsCache::new(),
        }
    }

    pub fn new(
        ctx: &'t dyn Context,
        parent: Box<dyn Criterion + 't>,
    ) -> Self
    {
        Fetcher {
            ctx,
            query_tree: None,
            candidates: Candidates::default(),
            parent: Some(parent),
            should_get_documents_ids: true,
            wdcache: WordDerivationsCache::new(),
        }
    }

    #[logging_timer::time("Fetcher::{}")]
    pub fn next(&mut self, exclude: &RoaringBitmap) -> anyhow::Result<Option<FetcherResult>> {
        use Candidates::{Allowed, Forbidden};
        loop {
            debug!("Fetcher iteration (should_get_documents_ids: {}) ({:?})",
                self.should_get_documents_ids, self.candidates,
            );

            let should_get_documents_ids = take(&mut self.should_get_documents_ids);
            match &mut self.candidates {
                Allowed(_) => {
                    let candidates = take(&mut self.candidates).into_inner();
                    let candidates = match &self.query_tree {
                        Some(qt) if should_get_documents_ids => {
                            let mut docids = resolve_query_tree(self.ctx, &qt, &mut HashMap::new(), &mut self.wdcache)?;
                            docids.intersect_with(&candidates);
                            docids
                        },
                        _ => candidates,
                    };

                    return Ok(Some(FetcherResult {
                        query_tree: self.query_tree.take(),
                        candidates: candidates.clone(),
                        bucket_candidates: candidates,
                    }));
                },
                Forbidden(_) => {
                    match self.parent.as_mut() {
                        Some(parent) => {
                            let context = CriterionContext {
                                word_cache: &mut self.wdcache,
                                exclude
                            };
                            match parent.next(context)? {
                                Some(CriterionResult { query_tree, candidates, bucket_candidates }) => {
                                    let candidates = match (&query_tree, candidates) {
                                        (_, Some(candidates)) => candidates,
                                        (Some(qt), None) => resolve_query_tree(self.ctx, qt, &mut HashMap::new(), &mut self.wdcache)?,
                                        (None, None) => RoaringBitmap::new(),
                                    };

                                    return Ok(Some(FetcherResult { query_tree, candidates, bucket_candidates }))
                                },
                                None => if should_get_documents_ids {
                                    let candidates = match &self.query_tree {
                                        Some(qt) => resolve_query_tree(self.ctx, &qt, &mut HashMap::new(), &mut self.wdcache)?,
                                        None => self.ctx.documents_ids()?,
                                    };

                                    return Ok(Some(FetcherResult {
                                        query_tree: self.query_tree.clone(),
                                        candidates: candidates.clone(),
                                        bucket_candidates: candidates,
                                    }));
                                },
                            }
                        },
                        None => if should_get_documents_ids {
                            let candidates = match &self.query_tree {
                                Some(qt) => resolve_query_tree(self.ctx, &qt, &mut HashMap::new(), &mut self.wdcache)?,
                                None => self.ctx.documents_ids()?,
                            };

                            return Ok(Some(FetcherResult {
                                query_tree: self.query_tree.clone(),
                                candidates: candidates.clone(),
                                bucket_candidates: candidates,
                            }));
                        },
                    }
                    return Ok(None);
                },
            }
        }
    }
}

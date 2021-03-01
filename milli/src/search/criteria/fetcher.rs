use std::collections::HashMap;
use std::mem::take;

use log::debug;
use roaring::RoaringBitmap;

use crate::search::query_tree::Operation;
use super::{resolve_query_tree, Candidates, Criterion, CriterionResult, Context};

pub struct Fetcher<'t> {
    ctx: &'t dyn Context,
    query_tree: Option<Operation>,
    candidates: Candidates,
    parent: Option<Box<dyn Criterion + 't>>,
    should_get_documents_ids: bool,
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
        }
    }
}

impl<'t> Criterion for Fetcher<'t> {
    fn next(&mut self) -> anyhow::Result<Option<CriterionResult>> {
        use Candidates::{Allowed, Forbidden};
        loop {
            debug!("Fetcher iteration (should_get_documents_ids: {}) ({:?})",
                self.should_get_documents_ids, self.candidates,
            );

            let should_get_documents_ids = take(&mut self.should_get_documents_ids);
            match &mut self.candidates {
                Allowed(candidates) => {
                    let candidates = take(&mut self.candidates).into_inner();
                    let candidates = match &self.query_tree {
                        Some(qt) if should_get_documents_ids => {
                            let mut docids = resolve_query_tree(self.ctx, &qt, &mut HashMap::new())?;
                            docids.intersect_with(&candidates);
                            docids
                        },
                        _ => candidates,
                    };

                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.take(),
                        candidates: candidates.clone(),
                        bucket_candidates: candidates,
                    }));
                },
                Forbidden(_) => {
                    match self.parent.as_mut() {
                        Some(parent) => {
                            match parent.next()? {
                                Some(result) => return Ok(Some(result)),
                                None => if should_get_documents_ids {
                                    let candidates = match &self.query_tree {
                                        Some(qt) => resolve_query_tree(self.ctx, &qt, &mut HashMap::new())?,
                                        None => self.ctx.documents_ids()?,
                                    };

                                    return Ok(Some(CriterionResult {
                                        query_tree: self.query_tree.clone(),
                                        candidates: candidates.clone(),
                                        bucket_candidates: candidates,
                                    }));
                                },
                            }
                        },
                        None => if should_get_documents_ids {
                            let candidates = match &self.query_tree {
                                Some(qt) => resolve_query_tree(self.ctx, &qt, &mut HashMap::new())?,
                                None => self.ctx.documents_ids()?,
                            };

                            return Ok(Some(CriterionResult {
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

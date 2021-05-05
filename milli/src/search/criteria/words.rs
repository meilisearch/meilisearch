use std::mem::take;

use log::debug;
use roaring::RoaringBitmap;

use crate::search::query_tree::Operation;
use super::{Context, Criterion, CriterionParameters, CriterionResult, resolve_query_tree};

pub struct Words<'t> {
    ctx: &'t dyn Context<'t>,
    query_trees: Vec<Operation>,
    candidates: Option<RoaringBitmap>,
    bucket_candidates: Option<RoaringBitmap>,
    parent: Box<dyn Criterion + 't>,
    compute_candidates: bool,
}

impl<'t> Words<'t> {
    pub fn new(ctx: &'t dyn Context<'t>, parent: Box<dyn Criterion + 't>) -> Self {
        Words {
            ctx,
            query_trees: Vec::default(),
            candidates: None,
            bucket_candidates: None,
            parent,
            compute_candidates: false,
        }
    }
}

impl<'t> Criterion for Words<'t> {
    #[logging_timer::time("Words::{}")]
    fn next(&mut self, params: &mut CriterionParameters) -> anyhow::Result<Option<CriterionResult>> {
        // remove excluded candidates when next is called, instead of doing it in the loop.
        if let Some(candidates) = self.candidates.as_mut() {
            *candidates -= params.excluded_candidates;
        }

        loop {
            debug!("Words at iteration {} ({:?})", self.query_trees.len(), self.candidates);

            match self.query_trees.pop() {
                Some(query_tree) => {
                    let candidates = match self.candidates.as_mut() {
                        Some(allowed_candidates) if self.compute_candidates => {
                            let mut candidates = resolve_query_tree(self.ctx, &query_tree, params.wdcache)?;
                            candidates &= &*allowed_candidates;
                            *allowed_candidates -= &candidates;
                            Some(candidates)
                        },
                        candidates => candidates.cloned(),
                    };

                    let bucket_candidates = match self.bucket_candidates.as_mut() {
                        Some(bucket_candidates) => Some(take(bucket_candidates)),
                        None => None,
                    };

                    return Ok(Some(CriterionResult {
                        query_tree: Some(query_tree),
                        candidates,
                        bucket_candidates,
                    }));
                },
                None => {
                    match self.parent.next(params)? {
                        Some(CriterionResult { query_tree: Some(query_tree), candidates, bucket_candidates }) => {
                            self.query_trees = explode_query_tree(query_tree);
                            self.candidates = candidates;
                            self.compute_candidates = bucket_candidates.is_some();

                            self.bucket_candidates = match (self.bucket_candidates.take(), bucket_candidates) {
                                (Some(self_bc), Some(parent_bc)) => Some(self_bc | parent_bc),
                                (self_bc, parent_bc) => self_bc.or(parent_bc),
                            };
                        },
                        Some(CriterionResult { query_tree: None, candidates, bucket_candidates }) => {
                            return Ok(Some(CriterionResult {
                                query_tree: None,
                                candidates,
                                bucket_candidates,
                            }));
                        },
                        None => return Ok(None),
                    }
                },
            }
        }
    }
}

fn explode_query_tree(query_tree: Operation) -> Vec<Operation> {
    match query_tree {
        Operation::Or(true, ops) => ops,
        otherwise => vec![otherwise],
    }
}

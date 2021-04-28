use std::collections::HashMap;
use std::mem::take;

use log::debug;
use roaring::RoaringBitmap;

use crate::search::query_tree::Operation;
use super::{resolve_query_tree, Criterion, CriterionResult, Context, WordDerivationsCache};

pub struct Words<'t> {
    ctx: &'t dyn Context<'t>,
    query_trees: Vec<Operation>,
    candidates: Option<RoaringBitmap>,
    bucket_candidates: RoaringBitmap,
    parent: Box<dyn Criterion + 't>,
    candidates_cache: HashMap<(Operation, u8), RoaringBitmap>,
}

impl<'t> Words<'t> {
    pub fn new(ctx: &'t dyn Context<'t>, parent: Box<dyn Criterion + 't>) -> Self {
        Words {
            ctx,
            query_trees: Vec::default(),
            candidates: None,
            bucket_candidates: RoaringBitmap::new(),
            parent,
            candidates_cache: HashMap::default(),
        }
    }
}

impl<'t> Criterion for Words<'t> {
    #[logging_timer::time("Words::{}")]
    fn next(&mut self, wdcache: &mut WordDerivationsCache) -> anyhow::Result<Option<CriterionResult>> {
        loop {
            debug!("Words at iteration {} ({:?})", self.query_trees.len(), self.candidates);

            match (self.query_trees.pop(), &mut self.candidates) {
                (query_tree, Some(candidates)) if candidates.is_empty() => {
                    self.query_trees = Vec::new();
                    return Ok(Some(CriterionResult {
                        query_tree,
                        candidates: self.candidates.take(),
                        bucket_candidates: take(&mut self.bucket_candidates),
                    }));
                },
                (Some(qt), Some(candidates)) => {
                    let mut found_candidates = resolve_query_tree(self.ctx, &qt, &mut self.candidates_cache, wdcache)?;
                    found_candidates.intersect_with(&candidates);
                    candidates.difference_with(&found_candidates);

                    return Ok(Some(CriterionResult {
                        query_tree: Some(qt),
                        candidates: Some(found_candidates),
                        bucket_candidates: take(&mut self.bucket_candidates),
                    }));
                },
                (Some(qt), None) => {
                    return Ok(Some(CriterionResult {
                        query_tree: Some(qt),
                        candidates: None,
                        bucket_candidates: take(&mut self.bucket_candidates),
                    }));
                },
                (None, Some(_)) => {
                    let candidates = self.candidates.take();
                    return Ok(Some(CriterionResult {
                        query_tree: None,
                        candidates: candidates.clone(),
                        bucket_candidates: candidates.unwrap_or_default(),
                    }));
                },
                (None, None) => {
                    match self.parent.next(wdcache)? {
                        Some(CriterionResult { query_tree: None, candidates: None, bucket_candidates }) => {
                            return Ok(Some(CriterionResult {
                                query_tree: None,
                                candidates: None,
                                bucket_candidates,
                            }));
                        },
                        Some(CriterionResult { query_tree, candidates, bucket_candidates }) => {
                            self.query_trees = query_tree.map(explode_query_tree).unwrap_or_default();
                            self.candidates = candidates;
                            self.bucket_candidates.union_with(&bucket_candidates);
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

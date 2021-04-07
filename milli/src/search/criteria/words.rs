use std::collections::HashMap;
use std::mem::take;

use log::debug;
use roaring::RoaringBitmap;

use crate::search::query_tree::Operation;
use super::{resolve_query_tree, Criterion, CriterionResult, Context, CriterionContext};

pub struct Words<'t> {
    ctx: &'t dyn Context,
    query_trees: Vec<Operation>,
    candidates: Option<RoaringBitmap>,
    bucket_candidates: RoaringBitmap,
    parent: Option<Box<dyn Criterion + 't>>,
    candidates_cache: HashMap<(Operation, u8), RoaringBitmap>,
}

impl<'t> Words<'t> {
    pub fn initial(
        ctx: &'t dyn Context,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
    ) -> Self
    {
        Words {
            ctx,
            query_trees: query_tree.map(explode_query_tree).unwrap_or_default(),
            candidates,
            bucket_candidates: RoaringBitmap::new(),
            parent: None,
            candidates_cache: HashMap::default(),
        }
    }

    pub fn new(ctx: &'t dyn Context, parent: Box<dyn Criterion + 't>) -> Self {
        Words {
            ctx,
            query_trees: Vec::default(),
            candidates: None,
            bucket_candidates: RoaringBitmap::new(),
            parent: Some(parent),
            candidates_cache: HashMap::default(),
        }
    }
}

impl<'t> Criterion for Words<'t> {
    #[logging_timer::time("Words::{}")]
    fn next(&mut self, context: CriterionContext) -> anyhow::Result<Option<CriterionResult>> {
        let CriterionContext { word_cache, exclude } = context;
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
                    let mut found_candidates = resolve_query_tree(self.ctx, &qt, &mut self.candidates_cache, word_cache)?;
                    found_candidates.intersect_with(&candidates);
                    candidates.difference_with(&found_candidates);

                    let bucket_candidates = match self.parent {
                        Some(_) => take(&mut self.bucket_candidates),
                        None => found_candidates.clone(),
                    };

                    return Ok(Some(CriterionResult {
                        query_tree: Some(qt),
                        candidates: Some(found_candidates),
                        bucket_candidates,
                    }));
                },
                (Some(qt), None) => {
                    let bucket_candidates = match self.parent {
                        Some(_) => take(&mut self.bucket_candidates),
                        None => RoaringBitmap::new(),
                    };

                    return Ok(Some(CriterionResult {
                        query_tree: Some(qt),
                        candidates: None,
                        bucket_candidates,
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
                    match self.parent.as_mut() {
                        Some(parent) => {
                            match parent.next(CriterionContext { word_cache, exclude })? {
                                Some(CriterionResult { query_tree, candidates, bucket_candidates }) => {
                                    self.query_trees = query_tree.map(explode_query_tree).unwrap_or_default();
                                    self.candidates = candidates;
                                    self.bucket_candidates.union_with(&bucket_candidates);
                                },
                                None => return Ok(None),
                            }
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

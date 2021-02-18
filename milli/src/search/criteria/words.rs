use std::collections::HashMap;
use std::mem::take;

use roaring::RoaringBitmap;

use crate::search::query_tree::Operation;
use super::{Candidates, Criterion, CriterionResult, Context};

pub struct Words<'t> {
    ctx: &'t dyn Context,
    query_trees: Vec<Operation>,
    candidates: Candidates,
    bucket_candidates: RoaringBitmap,
    parent: Option<Box<dyn Criterion + 't>>,
    candidates_cache: HashMap<(Operation, u8), RoaringBitmap>,
}

impl<'t> Words<'t> {
    pub fn initial(
        ctx: &'t dyn Context,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Ok(Words {
            ctx,
            query_trees: query_tree.map(explode_query_tree).unwrap_or_default(),
            candidates: candidates.map_or_else(Candidates::default, Candidates::Allowed),
            bucket_candidates: RoaringBitmap::new(),
            parent: None,
            candidates_cache: HashMap::default(),
        })
    }

    pub fn new(
        ctx: &'t dyn Context,
        parent: Box<dyn Criterion + 't>,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Ok(Words {
            ctx,
            query_trees: Vec::default(),
            candidates: Candidates::default(),
            bucket_candidates: RoaringBitmap::new(),
            parent: Some(parent),
            candidates_cache: HashMap::default(),
        })
    }
}

impl<'t> Criterion for Words<'t> {
    fn next(&mut self) -> anyhow::Result<Option<CriterionResult>> {
        use Candidates::{Allowed, Forbidden};

        loop {
            match (self.query_trees.pop(), &mut self.candidates) {
                (_, Allowed(candidates)) if candidates.is_empty() => {
                    self.query_trees = Vec::new();
                    self.candidates = Candidates::default();
                },
                (Some(qt), Allowed(candidates)) => {
                    let bucket_candidates = match self.parent {
                        Some(_) => take(&mut self.bucket_candidates),
                        None => candidates.clone(),
                    };

                    return Ok(Some(CriterionResult {
                        query_tree: Some(qt),
                        candidates: candidates.clone(),
                        bucket_candidates,
                    }));
                },
                (Some(_qt), Forbidden(_candidates)) => {
                    todo!()
                },
                (None, Allowed(_)) => {
                    let candidates = take(&mut self.candidates).into_inner();
                    return Ok(Some(CriterionResult {
                        query_tree: None,
                        candidates: candidates.clone(),
                        bucket_candidates: candidates,
                    }));
                },
                (None, Forbidden(_)) => {
                    match self.parent.as_mut() {
                        Some(parent) => {
                            match parent.next()? {
                                Some(CriterionResult { query_tree, candidates, bucket_candidates }) => {
                                    self.query_trees = query_tree.map(explode_query_tree).unwrap_or_default();
                                    self.candidates = Candidates::Allowed(candidates);
                                    self.bucket_candidates = bucket_candidates;
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

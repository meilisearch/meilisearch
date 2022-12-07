use log::debug;
use roaring::RoaringBitmap;

use super::{resolve_query_tree, Context, Criterion, CriterionParameters, CriterionResult};
use crate::search::criteria::InitialCandidates;
use crate::search::query_tree::Operation;
use crate::Result;

pub struct Words<'t> {
    ctx: &'t dyn Context<'t>,
    query_trees: Vec<Operation>,
    candidates: Option<RoaringBitmap>,
    initial_candidates: Option<InitialCandidates>,
    filtered_candidates: Option<RoaringBitmap>,
    parent: Box<dyn Criterion + 't>,
}

impl<'t> Words<'t> {
    pub fn new(ctx: &'t dyn Context<'t>, parent: Box<dyn Criterion + 't>) -> Self {
        Words {
            ctx,
            query_trees: Vec::default(),
            candidates: None,
            initial_candidates: None,
            parent,
            filtered_candidates: None,
        }
    }
}

impl<'t> Criterion for Words<'t> {
    #[logging_timer::time("Words::{}")]
    fn next(&mut self, params: &mut CriterionParameters) -> Result<Option<CriterionResult>> {
        // remove excluded candidates when next is called, instead of doing it in the loop.
        if let Some(candidates) = self.candidates.as_mut() {
            *candidates -= params.excluded_candidates;
        }

        loop {
            debug!("Words at iteration {} ({:?})", self.query_trees.len(), self.candidates);

            match self.query_trees.pop() {
                Some(query_tree) => {
                    let candidates = match self.candidates.as_mut() {
                        Some(allowed_candidates) => {
                            let mut candidates =
                                resolve_query_tree(self.ctx, &query_tree, params.wdcache)?;
                            candidates &= &*allowed_candidates;
                            *allowed_candidates -= &candidates;
                            Some(candidates)
                        }
                        None => None,
                    };

                    let initial_candidates = self.initial_candidates.clone();

                    return Ok(Some(CriterionResult {
                        query_tree: Some(query_tree),
                        candidates,
                        filtered_candidates: self.filtered_candidates.clone(),
                        initial_candidates,
                    }));
                }
                None => match self.parent.next(params)? {
                    Some(CriterionResult {
                        query_tree: Some(query_tree),
                        candidates,
                        filtered_candidates,
                        initial_candidates,
                    }) => {
                        self.query_trees = explode_query_tree(query_tree);
                        self.candidates = candidates;
                        self.filtered_candidates = filtered_candidates;

                        self.initial_candidates =
                            match (self.initial_candidates.take(), initial_candidates) {
                                (Some(self_bc), Some(parent_bc)) => Some(self_bc | parent_bc),
                                (self_bc, parent_bc) => self_bc.or(parent_bc),
                            };
                    }
                    Some(CriterionResult {
                        query_tree: None,
                        candidates,
                        filtered_candidates,
                        initial_candidates,
                    }) => {
                        return Ok(Some(CriterionResult {
                            query_tree: None,
                            candidates,
                            filtered_candidates,
                            initial_candidates,
                        }));
                    }
                    None => return Ok(None),
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

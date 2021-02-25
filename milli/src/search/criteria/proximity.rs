use std::collections::HashMap;
use std::mem::take;

use roaring::RoaringBitmap;
use log::debug;

use crate::search::query_tree::{maximum_proximity, Operation, Query};
use super::{Candidates, Criterion, CriterionResult, Context, query_docids, query_pair_proximity_docids};

pub struct Proximity<'t> {
    ctx: &'t dyn Context,
    query_tree: Option<(usize, Operation)>,
    proximity: u8,
    candidates: Candidates,
    bucket_candidates: RoaringBitmap,
    parent: Option<Box<dyn Criterion + 't>>,
    candidates_cache: HashMap<(Operation, u8), Vec<(Query, Query, RoaringBitmap)>>,
}

impl<'t> Proximity<'t> {
    pub fn initial(
        ctx: &'t dyn Context,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Ok(Proximity {
            ctx,
            query_tree: query_tree.map(|op| (maximum_proximity(&op), op)),
            proximity: 0,
            candidates: candidates.map_or_else(Candidates::default, Candidates::Allowed),
            bucket_candidates: RoaringBitmap::new(),
            parent: None,
            candidates_cache: HashMap::new(),
        })
    }

    pub fn new(
        ctx: &'t dyn Context,
        parent: Box<dyn Criterion + 't>,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Ok(Proximity {
            ctx,
            query_tree: None,
            proximity: 0,
            candidates: Candidates::default(),
            bucket_candidates: RoaringBitmap::new(),
            parent: Some(parent),
            candidates_cache: HashMap::new(),
        })
    }
}

impl<'t> Criterion for Proximity<'t> {
    fn next(&mut self) -> anyhow::Result<Option<CriterionResult>> {
        use Candidates::{Allowed, Forbidden};
        loop {
            debug!("Proximity at iteration {} (max {:?}) ({:?})",
                self.proximity,
                self.query_tree.as_ref().map(|(mp, _)| mp),
                self.candidates,
            );

            match (&mut self.query_tree, &mut self.candidates) {
                (_, Allowed(candidates)) if candidates.is_empty() => {
                    self.query_tree = None;
                    self.candidates = Candidates::default();
                },
                (Some((max_prox, query_tree)), Allowed(candidates)) => {
                    if self.proximity as usize > *max_prox {
                        self.query_tree = None;
                        self.candidates = Candidates::default();
                    } else {
                        let mut new_candidates = resolve_candidates(
                            self.ctx,
                            &query_tree,
                            self.proximity,
                            &mut self.candidates_cache,
                        )?;

                        new_candidates.intersect_with(&candidates);
                        candidates.difference_with(&new_candidates);
                        self.proximity += 1;

                        let bucket_candidates = match self.parent {
                            Some(_) => take(&mut self.bucket_candidates),
                            None => new_candidates.clone(),
                        };

                        return Ok(Some(CriterionResult {
                            query_tree: Some(query_tree.clone()),
                            candidates: new_candidates,
                            bucket_candidates,
                        }));
                    }
                },
                (Some((max_prox, query_tree)), Forbidden(candidates)) => {
                    if self.proximity as usize > *max_prox {
                        self.query_tree = None;
                        self.candidates = Candidates::default();
                    } else {
                        let mut new_candidates = resolve_candidates(
                            self.ctx,
                            &query_tree,
                            self.proximity,
                            &mut self.candidates_cache,
                        )?;

                        new_candidates.difference_with(&candidates);
                        candidates.union_with(&new_candidates);
                        self.proximity += 1;

                        let bucket_candidates = match self.parent {
                            Some(_) => take(&mut self.bucket_candidates),
                            None => new_candidates.clone(),
                        };

                        return Ok(Some(CriterionResult {
                            query_tree: Some(query_tree.clone()),
                            candidates: new_candidates,
                            bucket_candidates,
                        }));
                    }
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
                                    self.query_tree = query_tree.map(|op| (maximum_proximity(&op), op));
                                    self.proximity = 0;
                                    self.candidates = Candidates::Allowed(candidates);
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

fn resolve_candidates<'t>(
    ctx: &'t dyn Context,
    query_tree: &Operation,
    proximity: u8,
    cache: &mut HashMap<(Operation, u8), Vec<(Query, Query, RoaringBitmap)>>,
) -> anyhow::Result<RoaringBitmap>
{
    fn resolve_operation<'t>(
        ctx: &'t dyn Context,
        query_tree: &Operation,
        proximity: u8,
        cache: &mut HashMap<(Operation, u8), Vec<(Query, Query, RoaringBitmap)>>,
    ) -> anyhow::Result<Vec<(Query, Query, RoaringBitmap)>>
    {
        use Operation::{And, Consecutive, Or, Query};

        let result = match query_tree {
            And(ops) => mdfs(ctx, ops, proximity, cache)?,
            Consecutive(ops) => if proximity == 0 {
                mdfs(ctx, ops, 0, cache)?
            } else {
                Default::default()
            },
            Or(_, ops) => {
                let mut output = Vec::new();
                for op in ops {
                    let result = resolve_operation(ctx, op, proximity, cache)?;
                    output.extend(result);
                }
                output
            },
            Query(q) => if proximity == 0 {
                let candidates = query_docids(ctx, q)?;
                vec![(q.clone(), q.clone(), candidates)]
            } else {
                Default::default()
            },
        };

        Ok(result)
    }

    fn mdfs_pair<'t>(
        ctx: &'t dyn Context,
        left: &Operation,
        right: &Operation,
        proximity: u8,
        cache: &mut HashMap<(Operation, u8), Vec<(Query, Query, RoaringBitmap)>>,
    ) -> anyhow::Result<Vec<(Query, Query, RoaringBitmap)>>
    {
        fn pair_combinations(mana: u8) -> impl Iterator<Item = (u8, u8)> {
            (0..=mana).map(move |m| (mana - m, m))
        }

        let mut output = Vec::new();

        for (pair_p, left_right_p) in pair_combinations(proximity) {
            for (left_p, right_p) in pair_combinations(left_right_p) {
                let left_key = (left.clone(), left_p);
                if !cache.contains_key(&left_key) {
                    let candidates = resolve_operation(ctx, left, left_p, cache)?;
                    cache.insert(left_key.clone(), candidates);
                }

                let right_key = (right.clone(), right_p);
                if !cache.contains_key(&right_key) {
                    let candidates = resolve_operation(ctx, right, right_p, cache)?;
                    cache.insert(right_key.clone(), candidates);
                }

                let lefts = cache.get(&left_key).unwrap();
                let rights = cache.get(&right_key).unwrap();

                for (ll, lr, lcandidates) in lefts {
                    for (rl, rr, rcandidates) in rights {
                        let mut candidates = query_pair_proximity_docids(ctx, lr, rl, pair_p + 1)?;
                        if lcandidates.len() < rcandidates.len() {
                            candidates.intersect_with(lcandidates);
                            candidates.intersect_with(rcandidates);
                        } else {
                            candidates.intersect_with(rcandidates);
                            candidates.intersect_with(lcandidates);
                        }
                        if !candidates.is_empty() {
                            output.push((ll.clone(), rr.clone(), candidates));
                        }
                    }
                }
            }
        }

        Ok(output)
    }

    fn mdfs<'t>(
        ctx: &'t dyn Context,
        branches: &[Operation],
        proximity: u8,
        cache: &mut HashMap<(Operation, u8), Vec<(Query, Query, RoaringBitmap)>>,
    ) -> anyhow::Result<Vec<(Query, Query, RoaringBitmap)>>
    {
        // Extract the first two elements but gives the tail
        // that is just after the first element.
        let next = branches.split_first().map(|(h1, t)| {
            (h1, t.split_first().map(|(h2, _)| (h2, t)))
        });

        match next {
            Some((head1, Some((head2, [_])))) => mdfs_pair(ctx, head1, head2, proximity, cache),
            Some((head1, Some((head2, tail)))) => {
                let mut output = Vec::new();
                for p in 0..=proximity {
                    for (lhead, _, head_candidates) in mdfs_pair(ctx, head1, head2, p, cache)? {
                        if !head_candidates.is_empty() {
                            for (_, rtail, mut candidates) in mdfs(ctx, tail, proximity - p, cache)? {
                                candidates.intersect_with(&head_candidates);
                                if !candidates.is_empty() {
                                    output.push((lhead.clone(), rtail, candidates));
                                }
                            }
                        }
                    }
                }
                Ok(output)
            },
            Some((head1, None)) => resolve_operation(ctx, head1, proximity, cache),
            None => return Ok(Default::default()),
        }
    }

    let mut candidates = RoaringBitmap::new();
    for (_, _, cds) in resolve_operation(ctx, query_tree, proximity, cache)? {
        candidates.union_with(&cds);
    }
    Ok(candidates)
}

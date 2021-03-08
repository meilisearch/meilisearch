use std::borrow::Cow;
use std::collections::btree_map::{self, BTreeMap};
use std::collections::hash_map::{HashMap, Entry};
use std::mem::take;

use roaring::RoaringBitmap;
use log::debug;

use crate::{DocumentId, Position, search::{query_tree::QueryKind, word_derivations}};
use crate::search::query_tree::{maximum_proximity, Operation, Query};
use crate::search::WordDerivationsCache;
use super::{Candidates, Criterion, CriterionResult, Context, query_docids, query_pair_proximity_docids};

pub struct Proximity<'t> {
    ctx: &'t dyn Context,
    query_tree: Option<(usize, Operation)>,
    proximity: u8,
    candidates: Candidates,
    bucket_candidates: RoaringBitmap,
    parent: Option<Box<dyn Criterion + 't>>,
    candidates_cache: HashMap<(Operation, u8), Vec<(Query, Query, RoaringBitmap)>>,
    plane_sweep_cache: Option<btree_map::IntoIter<u8, RoaringBitmap>>,
}

impl<'t> Proximity<'t> {
    pub fn initial(
        ctx: &'t dyn Context,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
    ) -> Self
    {
        Proximity {
            ctx,
            query_tree: query_tree.map(|op| (maximum_proximity(&op), op)),
            proximity: 0,
            candidates: candidates.map_or_else(Candidates::default, Candidates::Allowed),
            bucket_candidates: RoaringBitmap::new(),
            parent: None,
            candidates_cache: HashMap::new(),
            plane_sweep_cache: None,
        }
    }

    pub fn new(ctx: &'t dyn Context, parent: Box<dyn Criterion + 't>) -> Self {
        Proximity {
            ctx,
            query_tree: None,
            proximity: 0,
            candidates: Candidates::default(),
            bucket_candidates: RoaringBitmap::new(),
            parent: Some(parent),
            candidates_cache: HashMap::new(),
            plane_sweep_cache: None,
        }
    }
}

impl<'t> Criterion for Proximity<'t> {
    fn next(&mut self, wdcache: &mut WordDerivationsCache) -> anyhow::Result<Option<CriterionResult>> {
        use Candidates::{Allowed, Forbidden};
        loop {
            debug!("Proximity at iteration {} (max {:?}) ({:?})",
                self.proximity,
                self.query_tree.as_ref().map(|(mp, _)| mp),
                self.candidates,
            );

            match (&mut self.query_tree, &mut self.candidates) {
                (_, Allowed(candidates)) if candidates.is_empty() => {
                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.take().map(|(_, qt)| qt),
                        candidates: take(&mut self.candidates).into_inner(),
                        bucket_candidates: take(&mut self.bucket_candidates),
                    }));
                },
                (Some((max_prox, query_tree)), Allowed(candidates)) => {
                    if self.proximity as usize > *max_prox {
                        // reset state to (None, Forbidden(_))
                        self.query_tree = None;
                        self.candidates = Candidates::default();
                    } else {
                        let mut new_candidates = if candidates.len() <= 1000 {
                            if let Some(cache) = self.plane_sweep_cache.as_mut() {
                                match cache.next() {
                                    Some((p, candidates)) => {
                                        self.proximity = p;
                                        candidates
                                    },
                                    None => {
                                        // reset state to (None, Forbidden(_))
                                        self.query_tree = None;
                                        self.candidates = Candidates::default();
                                        continue
                                    },
                                }
                            } else {
                                let cache = resolve_plane_sweep_candidates(
                                    self.ctx,
                                    query_tree,
                                    candidates,
                                    wdcache,
                                )?;
                                self.plane_sweep_cache = Some(cache.into_iter());

                                continue
                            }
                        } else { // use set theory based algorithm
                            resolve_candidates(
                               self.ctx,
                               &query_tree,
                               self.proximity,
                               &mut self.candidates_cache,
                               wdcache,
                           )?
                        };

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
                            wdcache,
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
                            match parent.next(wdcache)? {
                                Some(CriterionResult { query_tree, candidates, bucket_candidates }) => {
                                    self.query_tree = query_tree.map(|op| (maximum_proximity(&op), op));
                                    self.proximity = 0;
                                    self.candidates = Candidates::Allowed(candidates);
                                    self.bucket_candidates.union_with(&bucket_candidates);
                                    self.plane_sweep_cache = None;
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
    wdcache: &mut WordDerivationsCache,
) -> anyhow::Result<RoaringBitmap>
{
    fn resolve_operation<'t>(
        ctx: &'t dyn Context,
        query_tree: &Operation,
        proximity: u8,
        cache: &mut HashMap<(Operation, u8), Vec<(Query, Query, RoaringBitmap)>>,
        wdcache: &mut WordDerivationsCache,
    ) -> anyhow::Result<Vec<(Query, Query, RoaringBitmap)>>
    {
        use Operation::{And, Consecutive, Or, Query};

        let result = match query_tree {
            And(ops) => mdfs(ctx, ops, proximity, cache, wdcache)?,
            Consecutive(ops) => if proximity == 0 {
                mdfs(ctx, ops, 0, cache, wdcache)?
            } else {
                Default::default()
            },
            Or(_, ops) => {
                let mut output = Vec::new();
                for op in ops {
                    let result = resolve_operation(ctx, op, proximity, cache, wdcache)?;
                    output.extend(result);
                }
                output
            },
            Query(q) => if proximity == 0 {
                let candidates = query_docids(ctx, q, wdcache)?;
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
        wdcache: &mut WordDerivationsCache,
    ) -> anyhow::Result<Vec<(Query, Query, RoaringBitmap)>>
    {
        fn pair_combinations(mana: u8, left_max: u8) -> impl Iterator<Item = (u8, u8)> {
            (0..=mana.min(left_max)).map(move |m| (m, mana - m))
        }

        let pair_max_proximity = 7;

        let mut output = Vec::new();

        for (pair_p, left_right_p) in pair_combinations(proximity, pair_max_proximity) {
            for (left_p, right_p) in pair_combinations(left_right_p, left_right_p) {
                let left_key = (left.clone(), left_p);
                if !cache.contains_key(&left_key) {
                    let candidates = resolve_operation(ctx, left, left_p, cache, wdcache)?;
                    cache.insert(left_key.clone(), candidates);
                }

                let right_key = (right.clone(), right_p);
                if !cache.contains_key(&right_key) {
                    let candidates = resolve_operation(ctx, right, right_p, cache, wdcache)?;
                    cache.insert(right_key.clone(), candidates);
                }

                let lefts = cache.get(&left_key).unwrap();
                let rights = cache.get(&right_key).unwrap();

                for (ll, lr, lcandidates) in lefts {
                    for (rl, rr, rcandidates) in rights {
                        let mut candidates = query_pair_proximity_docids(ctx, lr, rl, pair_p + 1, wdcache)?;
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
        wdcache: &mut WordDerivationsCache,
    ) -> anyhow::Result<Vec<(Query, Query, RoaringBitmap)>>
    {
        // Extract the first two elements but gives the tail
        // that is just after the first element.
        let next = branches.split_first().map(|(h1, t)| {
            (h1, t.split_first().map(|(h2, _)| (h2, t)))
        });

        match next {
            Some((head1, Some((head2, [_])))) => mdfs_pair(ctx, head1, head2, proximity, cache, wdcache),
            Some((head1, Some((head2, tail)))) => {
                let mut output = Vec::new();
                for p in 0..=proximity {
                    for (lhead, _, head_candidates) in mdfs_pair(ctx, head1, head2, p, cache, wdcache)? {
                        if !head_candidates.is_empty() {
                            for (_, rtail, mut candidates) in mdfs(ctx, tail, proximity - p, cache, wdcache)? {
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
            Some((head1, None)) => resolve_operation(ctx, head1, proximity, cache, wdcache),
            None => return Ok(Default::default()),
        }
    }

    let mut candidates = RoaringBitmap::new();
    for (_, _, cds) in resolve_operation(ctx, query_tree, proximity, cache, wdcache)? {
        candidates.union_with(&cds);
    }
    Ok(candidates)
}

fn resolve_plane_sweep_candidates(
    ctx: &dyn Context,
    query_tree: &Operation,
    allowed_candidates: &RoaringBitmap,
    wdcache: &mut WordDerivationsCache,
) -> anyhow::Result<BTreeMap<u8, RoaringBitmap>>
{
    /// FIXME may be buggy with query like "new new york"
    fn plane_sweep<'a>(
        ctx: &dyn Context,
        operations: &'a [Operation],
        docid: DocumentId,
        consecutive: bool,
        rocache: &mut HashMap<&'a Operation, Vec<(Position, u8, Position)>>,
        dwpcache: &mut HashMap<String, Option<RoaringBitmap>>,
        wdcache: &mut WordDerivationsCache,
    ) -> anyhow::Result<Vec<(Position, u8, Position)>>
    {
        fn compute_groups_proximity(
            groups: &[(usize, (Position, u8, Position))],
            consecutive: bool,
        ) -> Option<(Position, u8, Position)>
        {
            // take the inner proximity of the first group as initial
            let mut proximity = groups.first()?.1.1;
            let left_most_pos = groups.first()?.1.0;
            let right_most_pos = groups.last()?.1.2;

            for pair in groups.windows(2) {
                if let [(i1, (_, _, rpos1)), (i2, (lpos2, prox2, _))] = pair {
                    // if a pair overlap, meaning that they share at least a word, we return None
                    if rpos1 >= lpos2 { return None }
                    // if groups are in the good order (query order) we remove 1 to the proximity
                    // the proximity is clamped to 7
                    let pair_proximity = if i1 < i2 {
                        (*lpos2 - *rpos1 - 1).min(7)
                    } else {
                        (*lpos2 - *rpos1).min(7)
                    };

                    proximity += pair_proximity as u8 + prox2;
                }
            }

            // if groups should be consecutives, we will only accept groups with a proximity of 0
            if !consecutive || proximity == 0 {
                Some((left_most_pos, proximity, right_most_pos))
            } else {
                None
            }
        }

        let groups_len = operations.len();
        let mut groups_positions = Vec::with_capacity(groups_len);

        for operation in operations {
            let positions = resolve_operation(ctx, operation, docid, rocache, dwpcache, wdcache)?;
            groups_positions.push(positions.into_iter());
        }

        // Pop top elements of each list.
        let mut current = Vec::with_capacity(groups_len);
        for (i, positions) in groups_positions.iter_mut().enumerate() {
            match positions.next() {
                Some(p) => current.push((i, p)),
                // if a group return None, it means that the document does not contain all the words,
                // we return an empty result.
                None => return Ok(Vec::new()),
            }
        }

        // Sort k elements by their positions.
        current.sort_unstable_by_key(|(_, p)| *p);

        // Find leftmost and rightmost group and their positions.
        let mut leftmost = *current.first().unwrap();
        let mut rightmost = *current.last().unwrap();

        let mut output = Vec::new();
        loop {
            // Find the position p of the next elements of a list of the leftmost group.
            // If the list is empty, break the loop.
            let p = groups_positions[leftmost.0].next().map(|p| (leftmost.0, p));

            // let q be the position q of second group of the interval.
            let q = current[1];

            let mut leftmost_index = 0;

            // If p > r, then the interval [l, r] is minimal and
            // we insert it into the heap according to its size.
            if p.map_or(true, |p| p.1 > rightmost.1) {
                leftmost_index = current[0].0;
                if let Some(group) = compute_groups_proximity(&current, consecutive) {
                    output.push(group);
                }
            }

            // TODO not sure about breaking here or when the p list is found empty.
            let p = match p {
                Some(p) => p,
                None => break,
            };

            // Remove the leftmost group P in the interval,
            // and pop the same group from a list.
            current[leftmost_index] = p;

            if p.1 > rightmost.1 {
                // if [l, r] is minimal, let r = p and l = q.
                rightmost = p;
                leftmost = q;
            } else {
                // Ohterwise, let l = min{p,q}.
                leftmost = if p.1 < q.1 { p } else { q };
            }

            // Then update the interval and order of groups_positions in the interval.
            current.sort_unstable_by_key(|(_, p)| *p);
        }

        // Sort the list according to the size and the positions.
        output.sort_unstable();

        Ok(output)
    }

    fn resolve_operation<'a>(
        ctx: &dyn Context,
        query_tree: &'a Operation,
        docid: DocumentId,
        rocache: &mut HashMap<&'a Operation, Vec<(Position, u8, Position)>>,
        dwpcache: &mut HashMap<String, Option<RoaringBitmap>>,
        wdcache: &mut WordDerivationsCache,
    ) -> anyhow::Result<Vec<(Position, u8, Position)>>
    {
        use Operation::{And, Consecutive, Or};

        if let Some(result) = rocache.get(query_tree) {
            return Ok(result.clone());
        }

        let result = match query_tree {
            And(ops) => plane_sweep(ctx, ops, docid, false, rocache, dwpcache, wdcache)?,
            Consecutive(ops) => plane_sweep(ctx, ops, docid, true, rocache, dwpcache, wdcache)?,
            Or(_, ops) => {
                let mut result = Vec::new();
                for op in ops {
                    result.extend(resolve_operation(ctx, op, docid, rocache, dwpcache, wdcache)?)
                }

                result.sort_unstable();
                result
            },
            Operation::Query(Query {prefix, kind}) => {
                let fst = ctx.words_fst();
                let words = match kind {
                    QueryKind::Exact { word, .. } => {
                        if *prefix {
                            Cow::Borrowed(word_derivations(word, true, 0, fst, wdcache)?)
                        } else {
                            Cow::Owned(vec![(word.to_string(), 0)])
                        }
                    },
                    QueryKind::Tolerant { typo, word } => {
                        Cow::Borrowed(word_derivations(word, *prefix, *typo, fst, wdcache)?)
                    }
                };

                let mut result = Vec::new();
                for (word, _) in words.as_ref() {
                    let positions = match dwpcache.entry(word.to_string()) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            let positions = ctx.docid_word_positions(docid, word)?;
                            entry.insert(positions)
                        }
                    };

                    if let Some(positions) = positions {
                        let iter = positions.iter().map(|p| (p, 0, p));
                        result.extend(iter);
                    }
                }

                result.sort_unstable();
                result
            }
        };

        rocache.insert(query_tree, result.clone());
        Ok(result)
    }

    let mut word_positions_cache = HashMap::new();
    let mut resolve_operation_cache = HashMap::new();
    let mut candidates = BTreeMap::new();
    for docid in allowed_candidates {
        word_positions_cache.clear();
        resolve_operation_cache.clear();
        let positions =  resolve_operation(
            ctx,
            query_tree,
            docid,
            &mut resolve_operation_cache,
            &mut word_positions_cache,
            wdcache,
        )?;
        let best_proximity = positions.into_iter().min_by_key(|(_, proximity, _)| *proximity);
        let best_proximity = best_proximity.map(|(_, proximity, _)| proximity).unwrap_or(7);
        candidates.entry(best_proximity).or_insert_with(RoaringBitmap::new).insert(docid);
    }

    Ok(candidates)
}

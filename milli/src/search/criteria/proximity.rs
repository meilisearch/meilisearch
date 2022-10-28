use std::collections::btree_map::{self, BTreeMap};
use std::collections::hash_map::HashMap;
use std::mem::take;

use log::debug;
use roaring::RoaringBitmap;
use slice_group_by::GroupBy;

use super::{
    query_docids, query_pair_proximity_docids, resolve_phrase, resolve_query_tree, Context,
    Criterion, CriterionParameters, CriterionResult,
};
use crate::search::query_tree::{maximum_proximity, Operation, Query, QueryKind};
use crate::search::{build_dfa, WordDerivationsCache};
use crate::{Position, Result};

type Cache = HashMap<(Operation, u8), Vec<(Query, Query, RoaringBitmap)>>;

/// Threshold on the number of candidates that will make
/// the system choose between one algorithm or another.
const CANDIDATES_THRESHOLD: u64 = 1000;

/// Threshold on the number of proximity that will make
/// the system choose between one algorithm or another.
const PROXIMITY_THRESHOLD: u8 = 0;

pub struct Proximity<'t> {
    ctx: &'t dyn Context<'t>,
    /// (max_proximity, query_tree, allowed_candidates)
    state: Option<(u8, Operation, RoaringBitmap)>,
    proximity: u8,
    bucket_candidates: RoaringBitmap,
    parent: Box<dyn Criterion + 't>,
    candidates_cache: Cache,
    plane_sweep_cache: Option<btree_map::IntoIter<u8, RoaringBitmap>>,
}

impl<'t> Proximity<'t> {
    pub fn new(ctx: &'t dyn Context<'t>, parent: Box<dyn Criterion + 't>) -> Self {
        Proximity {
            ctx,
            state: None,
            proximity: 0,
            bucket_candidates: RoaringBitmap::new(),
            parent,
            candidates_cache: Cache::new(),
            plane_sweep_cache: None,
        }
    }
}

impl<'t> Criterion for Proximity<'t> {
    #[logging_timer::time("Proximity::{}")]
    fn next(&mut self, params: &mut CriterionParameters) -> Result<Option<CriterionResult>> {
        // remove excluded candidates when next is called, instead of doing it in the loop.
        if let Some((_, _, allowed_candidates)) = self.state.as_mut() {
            *allowed_candidates -= params.excluded_candidates;
        }

        loop {
            debug!(
                "Proximity at iteration {} (max prox {:?}) ({:?})",
                self.proximity,
                self.state.as_ref().map(|(mp, _, _)| mp),
                self.state.as_ref().map(|(_, _, cd)| cd),
            );

            match &mut self.state {
                Some((max_prox, _, allowed_candidates))
                    if allowed_candidates.is_empty() || self.proximity > *max_prox =>
                {
                    self.state = None; // reset state
                }
                Some((_, query_tree, allowed_candidates)) => {
                    let mut new_candidates = if allowed_candidates.len() <= CANDIDATES_THRESHOLD
                        && self.proximity > PROXIMITY_THRESHOLD
                    {
                        if let Some(cache) = self.plane_sweep_cache.as_mut() {
                            match cache.next() {
                                Some((p, candidates)) => {
                                    self.proximity = p;
                                    candidates
                                }
                                None => {
                                    self.state = None; // reset state
                                    continue;
                                }
                            }
                        } else {
                            let cache = resolve_plane_sweep_candidates(
                                self.ctx,
                                query_tree,
                                allowed_candidates,
                            )?;
                            self.plane_sweep_cache = Some(cache.into_iter());

                            continue;
                        }
                    } else {
                        // use set theory based algorithm
                        resolve_candidates(
                            self.ctx,
                            query_tree,
                            self.proximity,
                            &mut self.candidates_cache,
                            params.wdcache,
                        )?
                    };

                    new_candidates &= &*allowed_candidates;
                    *allowed_candidates -= &new_candidates;
                    self.proximity += 1;

                    return Ok(Some(CriterionResult {
                        query_tree: Some(query_tree.clone()),
                        candidates: Some(new_candidates),
                        filtered_candidates: None,
                        bucket_candidates: Some(take(&mut self.bucket_candidates)),
                    }));
                }
                None => match self.parent.next(params)? {
                    Some(CriterionResult {
                        query_tree: Some(query_tree),
                        candidates,
                        filtered_candidates,
                        bucket_candidates,
                    }) => {
                        let mut candidates = match candidates {
                            Some(candidates) => candidates,
                            None => {
                                resolve_query_tree(self.ctx, &query_tree, params.wdcache)?
                                    - params.excluded_candidates
                            }
                        };

                        if let Some(filtered_candidates) = filtered_candidates {
                            candidates &= filtered_candidates;
                        }

                        match bucket_candidates {
                            Some(bucket_candidates) => self.bucket_candidates |= bucket_candidates,
                            None => self.bucket_candidates |= &candidates,
                        }

                        let maximum_proximity = maximum_proximity(&query_tree);
                        self.state = Some((maximum_proximity as u8, query_tree, candidates));
                        self.proximity = 0;
                        self.plane_sweep_cache = None;
                    }
                    Some(CriterionResult {
                        query_tree: None,
                        candidates,
                        filtered_candidates,
                        bucket_candidates,
                    }) => {
                        return Ok(Some(CriterionResult {
                            query_tree: None,
                            candidates,
                            filtered_candidates,
                            bucket_candidates,
                        }));
                    }
                    None => return Ok(None),
                },
            }
        }
    }
}

fn resolve_candidates<'t>(
    ctx: &'t dyn Context,
    query_tree: &Operation,
    proximity: u8,
    cache: &mut Cache,
    wdcache: &mut WordDerivationsCache,
) -> Result<RoaringBitmap> {
    fn resolve_operation<'t>(
        ctx: &'t dyn Context,
        query_tree: &Operation,
        proximity: u8,
        cache: &mut Cache,
        wdcache: &mut WordDerivationsCache,
    ) -> Result<Vec<(Query, Query, RoaringBitmap)>> {
        use Operation::{And, Or, Phrase};

        let result = match query_tree {
            And(ops) => mdfs(ctx, ops, proximity, cache, wdcache)?,
            Phrase(words) => {
                if proximity == 0 {
                    let most_left = words
                        .iter()
                        .filter_map(|o| o.as_ref())
                        .next()
                        .map(|w| Query { prefix: false, kind: QueryKind::exact(w.clone()) });
                    let most_right = words
                        .iter()
                        .rev()
                        .filter_map(|o| o.as_ref())
                        .next()
                        .map(|w| Query { prefix: false, kind: QueryKind::exact(w.clone()) });

                    match (most_left, most_right) {
                        (Some(l), Some(r)) => vec![(l, r, resolve_phrase(ctx, words)?)],
                        _otherwise => Default::default(),
                    }
                } else {
                    Default::default()
                }
            }
            Or(_, ops) => {
                let mut output = Vec::new();
                for op in ops {
                    let result = resolve_operation(ctx, op, proximity, cache, wdcache)?;
                    output.extend(result);
                }
                output
            }
            Operation::Query(q) => {
                if proximity == 0 {
                    let candidates = query_docids(ctx, q, wdcache)?;
                    vec![(q.clone(), q.clone(), candidates)]
                } else {
                    Default::default()
                }
            }
        };

        Ok(result)
    }

    fn mdfs_pair<'t>(
        ctx: &'t dyn Context,
        left: &Operation,
        right: &Operation,
        proximity: u8,
        cache: &mut Cache,
        wdcache: &mut WordDerivationsCache,
    ) -> Result<Vec<(Query, Query, RoaringBitmap)>> {
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
                        let mut candidates =
                            query_pair_proximity_docids(ctx, lr, rl, pair_p + 1, wdcache)?;
                        if lcandidates.len() < rcandidates.len() {
                            candidates &= lcandidates;
                            candidates &= rcandidates;
                        } else {
                            candidates &= rcandidates;
                            candidates &= lcandidates;
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
        cache: &mut Cache,
        wdcache: &mut WordDerivationsCache,
    ) -> Result<Vec<(Query, Query, RoaringBitmap)>> {
        // Extract the first two elements but gives the tail
        // that is just after the first element.
        let next =
            branches.split_first().map(|(h1, t)| (h1, t.split_first().map(|(h2, _)| (h2, t))));

        match next {
            Some((head1, Some((head2, [_])))) => {
                mdfs_pair(ctx, head1, head2, proximity, cache, wdcache)
            }
            Some((head1, Some((head2, tail)))) => {
                let mut output = Vec::new();
                for p in 0..=proximity {
                    for (lhead, _, head_candidates) in
                        mdfs_pair(ctx, head1, head2, p, cache, wdcache)?
                    {
                        if !head_candidates.is_empty() {
                            for (_, rtail, mut candidates) in
                                mdfs(ctx, tail, proximity - p, cache, wdcache)?
                            {
                                candidates &= &head_candidates;
                                if !candidates.is_empty() {
                                    output.push((lhead.clone(), rtail, candidates));
                                }
                            }
                        }
                    }
                }
                Ok(output)
            }
            Some((head1, None)) => resolve_operation(ctx, head1, proximity, cache, wdcache),
            None => Ok(Default::default()),
        }
    }

    let mut candidates = RoaringBitmap::new();
    for (_, _, cds) in resolve_operation(ctx, query_tree, proximity, cache, wdcache)? {
        candidates |= cds;
    }
    Ok(candidates)
}

fn resolve_plane_sweep_candidates(
    ctx: &dyn Context,
    query_tree: &Operation,
    allowed_candidates: &RoaringBitmap,
) -> Result<BTreeMap<u8, RoaringBitmap>> {
    /// FIXME may be buggy with query like "new new york"
    fn plane_sweep(
        groups_positions: Vec<Vec<(Position, u8, Position)>>,
        consecutive: bool,
    ) -> Result<Vec<(Position, u8, Position)>> {
        fn compute_groups_proximity(
            groups: &[(usize, (Position, u8, Position))],
            consecutive: bool,
        ) -> Option<(Position, u8, Position)> {
            // take the inner proximity of the first group as initial
            let (_, (_, mut proximity, _)) = groups.first()?;
            let (_, (left_most_pos, _, _)) = groups.first()?;
            let (_, (_, _, right_most_pos)) =
                groups.iter().max_by_key(|(_, (_, _, right_most_pos))| right_most_pos)?;

            for pair in groups.windows(2) {
                if let [(i1, (lpos1, _, rpos1)), (i2, (lpos2, prox2, rpos2))] = pair {
                    // if two positions are equal, meaning that they share at least a word, we return None
                    if rpos1 == rpos2 || lpos1 == lpos2 || rpos1 == lpos2 || lpos1 == rpos2 {
                        return None;
                    }

                    let pair_proximity = {
                        // if intervals are disjoint [..].(..)
                        if lpos2 > rpos1 {
                            lpos2 - rpos1
                        }
                        // if the second interval is a subset of the first [.(..).]
                        else if rpos2 < rpos1 {
                            (lpos2 - lpos1).min(rpos1 - rpos2)
                        }
                        // if intervals overlaps [.(..].)
                        else {
                            (lpos2 - lpos1).min(rpos2 - rpos1)
                        }
                    };

                    // if groups are in the good order (query order) we remove 1 to the proximity
                    // the proximity is clamped to 7
                    let pair_proximity =
                        if i1 < i2 { (pair_proximity - 1).min(7) } else { pair_proximity.min(7) };

                    proximity += pair_proximity as u8 + prox2;
                }
            }

            // if groups should be consecutives, we will only accept groups with a proximity of 0
            if !consecutive || proximity == 0 {
                Some((*left_most_pos, proximity, *right_most_pos))
            } else {
                None
            }
        }

        let groups_len = groups_positions.len();

        let mut groups_positions: Vec<_> =
            groups_positions.into_iter().map(|pos| pos.into_iter()).collect();

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

            // If p > r, then the interval [l, r] is minimal and
            // we insert it into the heap according to its size.
            if p.map_or(true, |p| p.1 > rightmost.1) {
                if let Some(group) = compute_groups_proximity(&current, consecutive) {
                    output.push(group);
                }
            }

            let p = match p {
                Some(p) => p,
                None => break,
            };

            // Replace the leftmost group P in the interval.
            current[0] = p;

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
        query_tree: &'a Operation,
        rocache: &mut HashMap<&'a Operation, Vec<(Position, u8, Position)>>,
        words_positions: &HashMap<String, RoaringBitmap>,
    ) -> Result<Vec<(Position, u8, Position)>> {
        use Operation::{And, Or, Phrase};

        if let Some(result) = rocache.get(query_tree) {
            return Ok(result.clone());
        }

        let result = match query_tree {
            And(ops) => {
                let mut groups_positions = Vec::with_capacity(ops.len());
                for operation in ops {
                    let positions = resolve_operation(operation, rocache, words_positions)?;
                    groups_positions.push(positions);
                }
                plane_sweep(groups_positions, false)?
            }
            Phrase(words) => {
                let mut groups_positions = Vec::with_capacity(words.len());

                // group stop_words together.
                for words in words.linear_group_by_key(Option::is_none) {
                    // skip if it's a group of stop words.
                    if matches!(words.first(), None | Some(None)) {
                        continue;
                    }
                    // make a consecutive plane-sweep on the subgroup of words.
                    let mut subgroup = Vec::with_capacity(words.len());
                    for word in words.into_iter().map(|w| w.as_deref().unwrap()) {
                        match words_positions.get(word) {
                            Some(positions) => {
                                subgroup.push(positions.iter().map(|p| (p, 0, p)).collect())
                            }
                            None => return Ok(vec![]),
                        }
                    }
                    match subgroup.len() {
                        0 => {}
                        1 => groups_positions.push(subgroup.pop().unwrap()),
                        _ => groups_positions.push(plane_sweep(subgroup, true)?),
                    }
                }
                match groups_positions.len() {
                    0 => vec![],
                    1 => groups_positions.pop().unwrap(),
                    _ => plane_sweep(groups_positions, false)?,
                }
            }
            Or(_, ops) => {
                let mut result = Vec::new();
                for op in ops {
                    result.extend(resolve_operation(op, rocache, words_positions)?)
                }

                result.sort_unstable();
                result
            }
            Operation::Query(Query { prefix, kind }) => {
                let mut result = Vec::new();
                match kind {
                    QueryKind::Exact { word, .. } => {
                        if *prefix {
                            let iter = word_derivations(word, true, 0, words_positions)
                                .flat_map(|positions| positions.iter().map(|p| (p, 0, p)));
                            result.extend(iter);
                        } else if let Some(positions) = words_positions.get(word) {
                            result.extend(positions.iter().map(|p| (p, 0, p)));
                        }
                    }
                    QueryKind::Tolerant { typo, word } => {
                        let iter = word_derivations(word, *prefix, *typo, words_positions)
                            .flat_map(|positions| positions.iter().map(|p| (p, 0, p)));
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

    fn word_derivations<'a>(
        word: &str,
        is_prefix: bool,
        max_typo: u8,
        words_positions: &'a HashMap<String, RoaringBitmap>,
    ) -> impl Iterator<Item = &'a RoaringBitmap> {
        let dfa = build_dfa(word, max_typo, is_prefix);
        words_positions.iter().filter_map(move |(document_word, positions)| {
            use levenshtein_automata::Distance;
            match dfa.eval(document_word) {
                Distance::Exact(_) => Some(positions),
                Distance::AtLeast(_) => None,
            }
        })
    }

    let mut resolve_operation_cache = HashMap::new();
    let mut candidates = BTreeMap::new();
    for docid in allowed_candidates {
        let words_positions = ctx.docid_words_positions(docid)?;
        resolve_operation_cache.clear();
        let positions =
            resolve_operation(query_tree, &mut resolve_operation_cache, &words_positions)?;
        let best_proximity = positions.into_iter().min_by_key(|(_, proximity, _)| *proximity);
        let best_proximity = best_proximity.map(|(_, proximity, _)| proximity).unwrap_or(7);
        candidates.entry(best_proximity).or_insert_with(RoaringBitmap::new).insert(docid);
    }

    Ok(candidates)
}

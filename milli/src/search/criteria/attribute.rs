use std::{borrow::Cow, cmp::{self, Ordering}, collections::BinaryHeap};
use std::collections::{BTreeMap, HashMap, btree_map};
use std::mem::take;

use roaring::RoaringBitmap;

use crate::{TreeLevel, search::build_dfa};
use crate::search::criteria::Query;
use crate::search::query_tree::{Operation, QueryKind};
use crate::search::{word_derivations, WordDerivationsCache};
use super::{Criterion, CriterionResult, Context, resolve_query_tree};

pub struct Attribute<'t> {
    ctx: &'t dyn Context<'t>,
    query_tree: Option<Operation>,
    candidates: Option<RoaringBitmap>,
    bucket_candidates: RoaringBitmap,
    parent: Box<dyn Criterion + 't>,
    flattened_query_tree: Option<Vec<Vec<Vec<Query>>>>,
    current_buckets: Option<btree_map::IntoIter<u64, RoaringBitmap>>,
}

impl<'t> Attribute<'t> {
    pub fn new(ctx: &'t dyn Context<'t>, parent: Box<dyn Criterion + 't>) -> Self {
        Attribute {
            ctx,
            query_tree: None,
            candidates: None,
            bucket_candidates: RoaringBitmap::new(),
            parent,
            flattened_query_tree: None,
            current_buckets: None,
        }
    }
}

impl<'t> Criterion for Attribute<'t> {
    #[logging_timer::time("Attribute::{}")]
    fn next(&mut self, wdcache: &mut WordDerivationsCache) -> anyhow::Result<Option<CriterionResult>> {
        loop {
            match (&self.query_tree, &mut self.candidates) {
                (_, Some(candidates)) if candidates.is_empty() => {
                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.take(),
                        candidates: self.candidates.take(),
                        bucket_candidates: take(&mut self.bucket_candidates),
                    }));
                },
                (Some(qt), Some(candidates)) => {
                    let flattened_query_tree = self.flattened_query_tree.get_or_insert_with(|| {
                        flatten_query_tree(&qt)
                    });

                    let found_candidates = if candidates.len() < 1_000 {
                        let current_buckets = match self.current_buckets.as_mut() {
                            Some(current_buckets) => current_buckets,
                            None => {
                                let new_buckets = linear_compute_candidates(self.ctx, flattened_query_tree, candidates)?;
                                self.current_buckets.get_or_insert(new_buckets.into_iter())
                            },
                        };

                        match current_buckets.next() {
                            Some((_score, candidates)) => candidates,
                            None => {
                                return Ok(Some(CriterionResult {
                                    query_tree: self.query_tree.take(),
                                    candidates: self.candidates.take(),
                                    bucket_candidates: take(&mut self.bucket_candidates),
                                }));
                            },
                        }
                    } else {
                        let found_candidates = set_compute_candidates(self.ctx, flattened_query_tree, candidates, wdcache)?;

                        match found_candidates {
                            Some(candidates) => candidates,
                            None => {
                                return Ok(Some(CriterionResult {
                                    query_tree: self.query_tree.take(),
                                    candidates: self.candidates.take(),
                                    bucket_candidates: take(&mut self.bucket_candidates),
                                }));
                            },
                        }
                    };

                    candidates.difference_with(&found_candidates);

                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.clone(),
                        candidates: Some(found_candidates),
                        bucket_candidates: take(&mut self.bucket_candidates),
                    }));
                },
                (Some(qt), None) => {
                    let query_tree_candidates = resolve_query_tree(self.ctx, &qt, &mut HashMap::new(), wdcache)?;
                    self.bucket_candidates.union_with(&query_tree_candidates);
                    self.candidates = Some(query_tree_candidates);
                },
                (None, Some(_)) => {
                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.take(),
                        candidates: self.candidates.take(),
                        bucket_candidates: take(&mut self.bucket_candidates),
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
                            self.query_tree = query_tree;
                            self.candidates = candidates;
                            self.bucket_candidates.union_with(&bucket_candidates);
                            self.flattened_query_tree = None;
                            self.current_buckets = None;
                        },
                        None => return Ok(None),
                    }
                },
            }
        }
    }
}

struct WordLevelIterator<'t, 'q> {
    inner: Box<dyn Iterator<Item =heed::Result<((&'t str, TreeLevel, u32, u32), RoaringBitmap)>> + 't>,
    level: TreeLevel,
    interval_size: u32,
    word: Cow<'q, str>,
    in_prefix_cache: bool,
    inner_next: Option<(u32, u32, RoaringBitmap)>,
    current_interval: Option<(u32, u32)>,
}

impl<'t, 'q> WordLevelIterator<'t, 'q> {
    fn new(ctx: &'t dyn Context<'t>, word: Cow<'q, str>, in_prefix_cache: bool) -> heed::Result<Option<Self>> {
        match ctx.word_position_last_level(&word, in_prefix_cache)? {
            Some(level) =>  {
                let interval_size = 4u32.pow(Into::<u8>::into(level.clone()) as u32);
                let inner = ctx.word_position_iterator(&word, level, in_prefix_cache, None, None)?;
                Ok(Some(Self { inner, level, interval_size, word, in_prefix_cache, inner_next: None, current_interval: None }))
            },
            None => Ok(None),
        }
    }

    fn dig(&self, ctx: &'t dyn Context<'t>, level: &TreeLevel) -> heed::Result<Self> {
        let level = level.min(&self.level).clone();
        let interval_size = 4u32.pow(Into::<u8>::into(level.clone()) as u32);
        let word = self.word.clone();
        let in_prefix_cache = self.in_prefix_cache;
        // TODO try to dig starting from the current interval
        // let left = self.current_interval.map(|(left, _)| left);
        let inner = ctx.word_position_iterator(&word, level, in_prefix_cache, None, None)?;

        Ok(Self {inner, level, interval_size, word, in_prefix_cache, inner_next: None, current_interval: None})
    }

    fn next(&mut self) -> heed::Result<Option<(u32, u32, RoaringBitmap)>> {
        fn is_next_interval(last_right: u32, next_left: u32) -> bool { last_right + 1 == next_left }

        let inner_next = match self.inner_next.take() {
            Some(inner_next) => Some(inner_next),
            None => self.inner.next().transpose()?.map(|((_, _, left, right), docids)| (left, right, docids)),
        };

        match inner_next {
            Some((left, right, docids)) => {
                match self.current_interval {
                    Some((last_left, last_right)) if !is_next_interval(last_right, left) => {
                        let blank_left = last_left + self.interval_size;
                        let blank_right = last_right + self.interval_size;
                        self.current_interval = Some((blank_left, blank_right));
                        self.inner_next = Some((left, right, docids));
                        Ok(Some((blank_left, blank_right, RoaringBitmap::new())))
                    },
                    _ => {
                        self.current_interval = Some((left, right));
                        Ok(Some((left, right, docids)))
                    }
                }
            },
            None => Ok(None),
        }
    }
}

struct QueryLevelIterator<'t, 'q> {
    previous: Option<Box<QueryLevelIterator<'t, 'q>>>,
    inner: Vec<WordLevelIterator<'t, 'q>>,
    level: TreeLevel,
    accumulator: Vec<Option<(u32, u32, RoaringBitmap)>>,
    previous_accumulator: Vec<Option<(u32, u32, RoaringBitmap)>>,
}

impl<'t, 'q> QueryLevelIterator<'t, 'q> {
    fn new(ctx: &'t dyn Context<'t>, queries: &'q Vec<Query>, wdcache: &mut WordDerivationsCache) -> anyhow::Result<Option<Self>> {
        let mut inner = Vec::with_capacity(queries.len());
        for query in queries {
            match &query.kind {
                QueryKind::Exact { word, .. } => {
                    if !query.prefix || ctx.in_prefix_cache(&word) {
                        let word = Cow::Borrowed(query.kind.word());
                        if let Some(word_level_iterator) = WordLevelIterator::new(ctx, word, query.prefix)? {
                            inner.push(word_level_iterator);
                        }
                    } else {
                        for (word, _) in word_derivations(&word, true, 0, ctx.words_fst(), wdcache)? {
                            let word = Cow::Owned(word.to_owned());
                            if let Some(word_level_iterator) = WordLevelIterator::new(ctx, word, false)? {
                                inner.push(word_level_iterator);
                            }
                        }
                    }
                },
                QueryKind::Tolerant { typo, word } => {
                    for (word, _) in word_derivations(&word, query.prefix, *typo, ctx.words_fst(), wdcache)? {
                        let word = Cow::Owned(word.to_owned());
                        if let Some(word_level_iterator) = WordLevelIterator::new(ctx, word, false)? {
                            inner.push(word_level_iterator);
                        }
                    }
                }
            }
        }

        let highest = inner.iter().max_by_key(|wli| wli.level).map(|wli| wli.level.clone());
        match highest {
            Some(level) => Ok(Some(Self {
                previous: None,
                inner,
                level,
                accumulator: vec![],
                previous_accumulator: vec![],
            })),
            None => Ok(None),
        }
    }

    fn previous(&mut self, previous: QueryLevelIterator<'t, 'q>) -> &Self {
        self.previous = Some(Box::new(previous));
        self
    }

    fn dig(&self, ctx: &'t dyn Context<'t>) -> heed::Result<Self> {
        let (level, previous) = match &self.previous {
            Some(previous) => {
                let previous = previous.dig(ctx)?;
                (previous.level.min(self.level), Some(Box::new(previous)))
            },
            None => (self.level.saturating_sub(1), None),
        };

        let mut inner = Vec::with_capacity(self.inner.len());
        for word_level_iterator in self.inner.iter() {
            inner.push(word_level_iterator.dig(ctx, &level)?);
        }

        Ok(Self {previous, inner, level, accumulator: vec![], previous_accumulator: vec![]})
    }



    fn inner_next(&mut self, level: TreeLevel) -> heed::Result<Option<(u32, u32, RoaringBitmap)>> {
        let mut accumulated: Option<(u32, u32, RoaringBitmap)> = None;
        let u8_level = Into::<u8>::into(level);
        let interval_size = 4u32.pow(u8_level as u32);
        for wli in self.inner.iter_mut() {
            let wli_u8_level = Into::<u8>::into(wli.level.clone());
            let accumulated_count = 4u32.pow((u8_level - wli_u8_level) as u32);
            for _ in 0..accumulated_count {
                if let Some((next_left, _, next_docids)) =  wli.next()? {
                    accumulated = accumulated.take().map(
                        |(acc_left, acc_right, mut acc_docids)| {
                            acc_docids.union_with(&next_docids);
                            (acc_left, acc_right, acc_docids)
                        }
                    ).or_else(|| Some((next_left, next_left + interval_size, next_docids)));
                }
            }
        }

        Ok(accumulated)
    }

    fn next(&mut self) -> heed::Result<(TreeLevel, Option<(u32, u32, RoaringBitmap)>)> {
        let previous_result = match self.previous.as_mut() {
            Some(previous) => {
                Some(previous.next()?)
            },
            None => None,
        };

        match previous_result {
            Some((previous_level, previous_next)) => {
                let inner_next = self.inner_next(previous_level)?;
                self.accumulator.push(inner_next);
                self.previous_accumulator.push(previous_next);
                // TODO @many clean firsts intervals of both accumulators when both RoaringBitmap are empty,
                // WARNING the cleaned intervals count needs to be kept to skip at the end
                let mut merged_interval = None;
                for current in self.accumulator.iter().rev().zip(self.previous_accumulator.iter()) {
                    if let (Some((left_a, right_a, a)), Some((left_b, right_b, b))) = current {
                        let (_, _, merged_docids) = merged_interval.get_or_insert_with(|| (left_a + left_b, right_a + right_b, RoaringBitmap::new()));
                        merged_docids.union_with(&(a & b));
                    }
                }
                Ok((previous_level, merged_interval))
            },
            None => {
                let level = self.level.clone();
                let next_interval = self.inner_next(level.clone())?;
                self.accumulator = vec![next_interval.clone()];
                Ok((level, next_interval))
            }
        }
    }
}

struct Branch<'t, 'q> {
    query_level_iterator: QueryLevelIterator<'t, 'q>,
    last_result: Option<(u32, u32, RoaringBitmap)>,
    tree_level: TreeLevel,
    branch_size: u32,
}

impl<'t, 'q> Branch<'t, 'q> {
    fn cmp(&self, other: &Self) -> Ordering {
        fn compute_rank(left: u32, branch_size: u32) -> u32 { left.saturating_sub((0..branch_size).sum()) / branch_size }
        match (&self.last_result, &other.last_result) {
            (Some((s_left, _, _)), Some((o_left, _, _))) => {
                // we compute a rank from the left interval.
                let self_rank = compute_rank(*s_left, self.branch_size);
                let other_rank = compute_rank(*o_left, other.branch_size);
                let left_cmp = self_rank.cmp(&other_rank).reverse();
                // on level: higher is better,
                // we want to reduce highest levels first.
                let level_cmp = self.tree_level.cmp(&other.tree_level);

                left_cmp.then(level_cmp)
            },
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }
}

impl<'t, 'q> Ord for Branch<'t, 'q> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

impl<'t, 'q> PartialOrd for Branch<'t, 'q> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'t, 'q> PartialEq for Branch<'t, 'q> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'t, 'q> Eq for Branch<'t, 'q> {}

fn initialize_query_level_iterators<'t, 'q>(
    ctx: &'t dyn Context<'t>,
    branches: &'q Vec<Vec<Vec<Query>>>,
    wdcache: &mut WordDerivationsCache,
) -> anyhow::Result<BinaryHeap<Branch<'t, 'q>>> {

    let mut positions = BinaryHeap::with_capacity(branches.len());
    for branch in branches {
        let mut branch_positions = Vec::with_capacity(branch.len());
        for queries in  branch {
            match QueryLevelIterator::new(ctx, queries, wdcache)? {
                Some(qli) => branch_positions.push(qli),
                None => {
                    // the branch seems to be invalid, so we skip it.
                    branch_positions.clear();
                    break;
                },
            }
        }
        // QueryLevelIterator need to be sorted by level and folded in descending order.
        branch_positions.sort_unstable_by_key(|qli| qli.level);
        let folded_query_level_iterators =  branch_positions
            .into_iter()
            .rev()
            .fold(None, |fold: Option<QueryLevelIterator>, mut qli| match fold {
                Some(fold) => {
                    qli.previous(fold);
                    Some(qli)
                },
                None => Some(qli),
        });

        if let Some(mut folded_query_level_iterators) = folded_query_level_iterators {
            let (tree_level, last_result)  = folded_query_level_iterators.next()?;
            let branch = Branch {
                last_result,
                tree_level,
                query_level_iterator: folded_query_level_iterators,
                branch_size: branch.len() as u32,
            };
            positions.push(branch);
        }
    }

    Ok(positions)
}

fn set_compute_candidates<'t>(
    ctx: &'t dyn Context<'t>,
    branches: &Vec<Vec<Vec<Query>>>,
    allowed_candidates: &RoaringBitmap,
    wdcache: &mut WordDerivationsCache,
) -> anyhow::Result<Option<RoaringBitmap>>
{
    let mut branches_heap = initialize_query_level_iterators(ctx, branches, wdcache)?;
    let lowest_level = TreeLevel::min_value();
    let mut final_candidates = None;

    while let Some(mut branch) = branches_heap.peek_mut() {
        let is_lowest_level = branch.tree_level == lowest_level;
        match branch.last_result.as_mut() {
            Some((_, _, candidates)) => {
                candidates.intersect_with(&allowed_candidates);
                if candidates.len() > 0 && is_lowest_level {
                    // we have candidates, but we can't dig deeper, return candidates.
                    final_candidates = Some(std::mem::take(candidates));
                    break;
                } else if candidates.len() > 0 {
                    // we have candidates, lets dig deeper in levels.
                    let mut query_level_iterator = branch.query_level_iterator.dig(ctx)?;
                    let (tree_level, last_result) = query_level_iterator.next()?;
                    branch.query_level_iterator = query_level_iterator;
                    branch.tree_level = tree_level;
                    branch.last_result = last_result;
                } else {
                    // we don't have candidates, get next interval.
                    let (_, last_result) = branch.query_level_iterator.next()?;
                    branch.last_result = last_result;
                }
            },
            // None = no candidates to find.
            None => break,
        }

    }

    Ok(final_candidates)
}

fn linear_compute_candidates(
    ctx: &dyn Context,
    branches: &Vec<Vec<Vec<Query>>>,
    allowed_candidates: &RoaringBitmap,
) -> anyhow::Result<BTreeMap<u64, RoaringBitmap>>
{
    fn compute_candidate_rank(branches: &Vec<Vec<Vec<Query>>>, words_positions: HashMap<String, RoaringBitmap>) -> u64 {
        let mut min_rank = u64::max_value();
        for branch in branches {

            let branch_len = branch.len();
            let mut branch_rank = Vec::with_capacity(branch_len);
            for derivates in branch {
                let mut position = None;
                for Query { prefix, kind } in derivates {
                    // find the best position of the current word in the document.
                    let current_position = match kind {
                        QueryKind::Exact { word, .. } => {
                            if *prefix {
                                word_derivations(word, true, 0, &words_positions)
                                .flat_map(|positions| positions.iter().next()).min()
                            } else {
                                words_positions.get(word)
                                    .map(|positions| positions.iter().next())
                                    .flatten()
                            }
                        },
                        QueryKind::Tolerant { typo, word } => {
                            word_derivations(word, *prefix, *typo, &words_positions)
                                .flat_map(|positions| positions.iter().next()).min()
                        },
                    };

                    match (position, current_position) {
                        (Some(p), Some(cp)) => position = Some(cmp::min(p, cp)),
                        (None, Some(cp)) => position = Some(cp),
                        _ => (),
                    }
                }

                // if a position is found, we add it to the branch score,
                // otherwise the branch is considered as unfindable in this document and we break.
                if let Some(position) = position {
                    branch_rank.push(position as u64);
                } else {
                    branch_rank.clear();
                    break;
                }
            }

            if !branch_rank.is_empty() {
                branch_rank.sort_unstable();
                // because several words in same query can't match all a the position 0,
                // we substract the word index to the position.
                let branch_rank: u64 = branch_rank.into_iter().enumerate().map(|(i, r)| r - i as u64).sum();
                // here we do the means of the words of the branch
                min_rank = min_rank.min(branch_rank / branch_len as u64);
            }
        }

        min_rank
    }

    fn word_derivations<'a>(
        word: &str,
        is_prefix: bool,
        max_typo: u8,
        words_positions: &'a HashMap<String, RoaringBitmap>,
    ) -> impl Iterator<Item = &'a RoaringBitmap>
    {
        let dfa = build_dfa(word, max_typo, is_prefix);
        words_positions.iter().filter_map(move |(document_word, positions)| {
            use levenshtein_automata::Distance;
            match dfa.eval(document_word) {
                Distance::Exact(_) => Some(positions),
                Distance::AtLeast(_) => None,
            }
        })
    }

    let mut candidates = BTreeMap::new();
    for docid in allowed_candidates {
        let words_positions = ctx.docid_words_positions(docid)?;
        let rank = compute_candidate_rank(branches, words_positions);
        candidates.entry(rank).or_insert_with(RoaringBitmap::new).insert(docid);
    }

    Ok(candidates)
}

// TODO can we keep refs of Query
fn flatten_query_tree(query_tree: &Operation) -> Vec<Vec<Vec<Query>>> {
    use crate::search::criteria::Operation::{And, Or, Consecutive};

    fn and_recurse(head: &Operation, tail: &[Operation]) -> Vec<Vec<Vec<Query>>> {
        match tail.split_first() {
            Some((thead, tail)) => {
                let tail = and_recurse(thead, tail);
                let mut out = Vec::new();
                for array in recurse(head) {
                    for tail_array in &tail {
                        let mut array = array.clone();
                        array.extend(tail_array.iter().cloned());
                        out.push(array);
                    }
                }
                out
            },
            None => recurse(head),
        }
    }

    fn recurse(op: &Operation) -> Vec<Vec<Vec<Query>>> {
        match op {
            And(ops) | Consecutive(ops) => {
                ops.split_first().map_or_else(Vec::new, |(h, t)| and_recurse(h, t))
            },
            Or(_, ops) => if ops.iter().all(|op| op.query().is_some()) {
                vec![vec![ops.iter().flat_map(|op| op.query()).cloned().collect()]]
            } else {
                ops.into_iter().map(recurse).flatten().collect()
            },
            Operation::Query(query) => vec![vec![vec![query.clone()]]],
        }
    }

    recurse(query_tree)
}

#[cfg(test)]
mod tests {
    use big_s::S;

    use crate::search::criteria::QueryKind;
    use super::*;

    #[test]
    fn simple_flatten_query_tree() {
        let query_tree = Operation::Or(false, vec![
            Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("manythefish")) }),
            Operation::And(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("manythe")) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("fish")) }),
            ]),
            Operation::And(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("many")) }),
                Operation::Or(false, vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("thefish")) }),
                    Operation::And(vec![
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("the")) }),
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("fish")) }),
                    ]),
                ]),
            ]),
        ]);

        let expected = vec![
            vec![vec![Query { prefix: false, kind: QueryKind::exact(S("manythefish")) }]],
            vec![
                vec![Query { prefix: false, kind: QueryKind::exact(S("manythe")) }],
                vec![Query { prefix: false, kind: QueryKind::exact(S("fish")) }],
            ],
            vec![
                vec![Query { prefix: false, kind: QueryKind::exact(S("many")) }],
                vec![Query { prefix: false, kind: QueryKind::exact(S("thefish")) }],
            ],
            vec![
                vec![Query { prefix: false, kind: QueryKind::exact(S("many")) }],
                vec![Query { prefix: false, kind: QueryKind::exact(S("the")) }],
                vec![Query { prefix: false, kind: QueryKind::exact(S("fish")) }],
            ],
        ];

        let result = flatten_query_tree(&query_tree);
        assert_eq!(expected, result);
    }
}

use std::cmp::{self, Ordering};
use std::collections::binary_heap::PeekMut;
use std::collections::{btree_map, BTreeMap, BinaryHeap, HashMap};
use std::iter::Peekable;
use std::mem::take;

use roaring::RoaringBitmap;

use super::{resolve_query_tree, Context, Criterion, CriterionParameters, CriterionResult};
use crate::search::criteria::{InitialCandidates, Query};
use crate::search::query_tree::{Operation, QueryKind};
use crate::search::{
    build_dfa, word_derivations, CriterionImplementationStrategy, WordDerivationsCache,
};
use crate::Result;

/// To be able to divide integers by the number of words in the query
/// we want to find a multiplier that allow us to divide by any number between 1 and 10.
/// We chose the LCM of all numbers between 1 and 10 as the multiplier (https://en.wikipedia.org/wiki/Least_common_multiple).
const LCM_10_FIRST_NUMBERS: u32 = 2520;

/// Threshold on the number of candidates that will make
/// the system to choose between one algorithm or another.
const CANDIDATES_THRESHOLD: u64 = 500;

type FlattenedQueryTree = Vec<Vec<Vec<Query>>>;

pub struct Attribute<'t> {
    ctx: &'t dyn Context<'t>,
    state: Option<(Operation, FlattenedQueryTree, RoaringBitmap)>,
    initial_candidates: InitialCandidates,
    parent: Box<dyn Criterion + 't>,
    linear_buckets: Option<btree_map::IntoIter<u64, RoaringBitmap>>,
    set_buckets: Option<BinaryHeap<Branch<'t>>>,
    implementation_strategy: CriterionImplementationStrategy,
}

impl<'t> Attribute<'t> {
    pub fn new(
        ctx: &'t dyn Context<'t>,
        parent: Box<dyn Criterion + 't>,
        implementation_strategy: CriterionImplementationStrategy,
    ) -> Self {
        Attribute {
            ctx,
            state: None,
            initial_candidates: InitialCandidates::Estimated(RoaringBitmap::new()),
            parent,
            linear_buckets: None,
            set_buckets: None,
            implementation_strategy,
        }
    }
}

impl<'t> Criterion for Attribute<'t> {
    #[logging_timer::time("Attribute::{}")]
    fn next(&mut self, params: &mut CriterionParameters) -> Result<Option<CriterionResult>> {
        // remove excluded candidates when next is called, instead of doing it in the loop.
        if let Some((_, _, allowed_candidates)) = self.state.as_mut() {
            *allowed_candidates -= params.excluded_candidates;
        }

        loop {
            match self.state.take() {
                Some((query_tree, _, allowed_candidates)) if allowed_candidates.is_empty() => {
                    return Ok(Some(CriterionResult {
                        query_tree: Some(query_tree),
                        candidates: Some(RoaringBitmap::new()),
                        filtered_candidates: None,
                        initial_candidates: Some(self.initial_candidates.take()),
                    }));
                }
                Some((query_tree, flattened_query_tree, mut allowed_candidates)) => {
                    let found_candidates = if matches!(
                        self.implementation_strategy,
                        CriterionImplementationStrategy::OnlyIterative
                    ) || (matches!(
                        self.implementation_strategy,
                        CriterionImplementationStrategy::Dynamic
                    ) && allowed_candidates.len()
                        < CANDIDATES_THRESHOLD)
                    {
                        let linear_buckets = match self.linear_buckets.as_mut() {
                            Some(linear_buckets) => linear_buckets,
                            None => {
                                let new_buckets = initialize_linear_buckets(
                                    self.ctx,
                                    &flattened_query_tree,
                                    &allowed_candidates,
                                )?;
                                self.linear_buckets.get_or_insert(new_buckets.into_iter())
                            }
                        };

                        match linear_buckets.next() {
                            Some((_score, candidates)) => candidates,
                            None => {
                                return Ok(Some(CriterionResult {
                                    query_tree: Some(query_tree),
                                    candidates: Some(RoaringBitmap::new()),
                                    filtered_candidates: None,
                                    initial_candidates: Some(self.initial_candidates.take()),
                                }));
                            }
                        }
                    } else {
                        let set_buckets = match self.set_buckets.as_mut() {
                            Some(set_buckets) => set_buckets,
                            None => {
                                let new_buckets = initialize_set_buckets(
                                    self.ctx,
                                    &flattened_query_tree,
                                    &allowed_candidates,
                                    params.wdcache,
                                )?;
                                self.set_buckets.get_or_insert(new_buckets)
                            }
                        };

                        match set_compute_candidates(set_buckets, &allowed_candidates)? {
                            Some((_score, candidates)) => candidates,
                            None => {
                                return Ok(Some(CriterionResult {
                                    query_tree: Some(query_tree),
                                    candidates: Some(allowed_candidates),
                                    filtered_candidates: None,
                                    initial_candidates: Some(self.initial_candidates.take()),
                                }));
                            }
                        }
                    };

                    allowed_candidates -= &found_candidates;

                    self.state =
                        Some((query_tree.clone(), flattened_query_tree, allowed_candidates));

                    return Ok(Some(CriterionResult {
                        query_tree: Some(query_tree),
                        candidates: Some(found_candidates),
                        filtered_candidates: None,
                        initial_candidates: Some(self.initial_candidates.take()),
                    }));
                }
                None => match self.parent.next(params)? {
                    Some(CriterionResult {
                        query_tree: Some(query_tree),
                        candidates,
                        filtered_candidates,
                        initial_candidates,
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

                        let flattened_query_tree = flatten_query_tree(&query_tree);

                        match initial_candidates {
                            Some(initial_candidates) => {
                                self.initial_candidates |= initial_candidates
                            }
                            None => self.initial_candidates.map_inplace(|c| c | &candidates),
                        }

                        self.state = Some((query_tree, flattened_query_tree, candidates));
                        self.linear_buckets = None;
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

/// QueryPositionIterator is an Iterator over positions of a Query,
/// It contains iterators over words positions.
struct QueryPositionIterator<'t> {
    #[allow(clippy::type_complexity)]
    inner:
        Vec<Peekable<Box<dyn Iterator<Item = heed::Result<((&'t str, u32), RoaringBitmap)>> + 't>>>,
}

impl<'t> QueryPositionIterator<'t> {
    fn new(
        ctx: &'t dyn Context<'t>,
        queries: &[Query],
        wdcache: &mut WordDerivationsCache,
    ) -> Result<Self> {
        let mut inner = Vec::with_capacity(queries.len());
        for query in queries {
            let in_prefix_cache = query.prefix && ctx.in_prefix_cache(query.kind.word());
            match &query.kind {
                QueryKind::Exact { word, .. } => {
                    if !query.prefix || in_prefix_cache {
                        let word = query.kind.word();
                        let iter = ctx.word_position_iterator(word, in_prefix_cache)?;
                        inner.push(iter.peekable());
                    } else {
                        for (word, _) in word_derivations(word, true, 0, ctx.words_fst(), wdcache)?
                        {
                            let iter = ctx.word_position_iterator(word, in_prefix_cache)?;
                            inner.push(iter.peekable());
                        }
                    }
                }
                QueryKind::Tolerant { typo, word } => {
                    for (word, _) in
                        word_derivations(word, query.prefix, *typo, ctx.words_fst(), wdcache)?
                    {
                        let iter = ctx.word_position_iterator(word, in_prefix_cache)?;
                        inner.push(iter.peekable());
                    }
                }
            };
        }

        Ok(Self { inner })
    }
}

impl<'t> Iterator for QueryPositionIterator<'t> {
    type Item = heed::Result<(u32, RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        // sort inner words from the closest next position to the farthest next position.
        let expected_pos = self
            .inner
            .iter_mut()
            .filter_map(|wli| match wli.peek() {
                Some(Ok(((_, pos), _))) => Some(*pos),
                _ => None,
            })
            .min()?;

        let mut candidates = None;
        for wli in self.inner.iter_mut() {
            if let Some(Ok(((_, pos), _))) = wli.peek() {
                if *pos > expected_pos {
                    continue;
                }
            }

            match wli.next() {
                Some(Ok((_, docids))) => {
                    candidates = match candidates.take() {
                        Some(candidates) => Some(candidates | docids),
                        None => Some(docids),
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None => continue,
            }
        }

        candidates.map(|candidates| Ok((expected_pos, candidates)))
    }
}

/// A Branch is represent a possible alternative of the original query and is build with the Query Tree,
/// This branch allows us to iterate over meta-interval of positions.
struct Branch<'t> {
    query_level_iterator: Vec<(u32, RoaringBitmap, Peekable<QueryPositionIterator<'t>>)>,
    last_result: (u32, RoaringBitmap),
    branch_size: u32,
}

impl<'t> Branch<'t> {
    fn new(
        ctx: &'t dyn Context<'t>,
        flatten_branch: &[Vec<Query>],
        wdcache: &mut WordDerivationsCache,
        allowed_candidates: &RoaringBitmap,
    ) -> Result<Self> {
        let mut query_level_iterator = Vec::new();
        for queries in flatten_branch {
            let mut qli = QueryPositionIterator::new(ctx, queries, wdcache)?.peekable();
            let (pos, docids) = qli.next().transpose()?.unwrap_or((0, RoaringBitmap::new()));
            query_level_iterator.push((pos, docids & allowed_candidates, qli));
        }

        let mut branch = Self {
            query_level_iterator,
            last_result: (0, RoaringBitmap::new()),
            branch_size: flatten_branch.len() as u32,
        };

        branch.update_last_result();

        Ok(branch)
    }

    /// return the next meta-interval of the branch,
    /// and update inner interval in order to be ranked by the BinaryHeap.
    fn next(&mut self, allowed_candidates: &RoaringBitmap) -> heed::Result<bool> {
        // update the first query.
        let index = self.lowest_iterator_index();
        match self.query_level_iterator.get_mut(index) {
            Some((cur_pos, cur_docids, qli)) => match qli.next().transpose()? {
                Some((next_pos, next_docids)) => {
                    *cur_pos = next_pos;
                    *cur_docids |= next_docids & allowed_candidates;
                    self.update_last_result();
                    Ok(true)
                }
                None => Ok(false),
            },
            None => Ok(false),
        }
    }

    fn lowest_iterator_index(&mut self) -> usize {
        let (index, _) = self
            .query_level_iterator
            .iter_mut()
            .map(|(pos, docids, qli)| {
                if docids.is_empty() {
                    0
                } else {
                    match qli.peek() {
                        Some(result) => {
                            result.as_ref().map(|(next_pos, _)| *next_pos - *pos).unwrap_or(0)
                        }
                        None => u32::MAX,
                    }
                }
            })
            .enumerate()
            .min_by_key(|(_, diff)| *diff)
            .unwrap_or((0, 0));

        index
    }

    fn update_last_result(&mut self) {
        let mut result_pos = 0;
        let mut result_docids = None;

        for (pos, docids, _qli) in self.query_level_iterator.iter() {
            result_pos += pos;
            result_docids = result_docids
                .take()
                .map_or_else(|| Some(docids.clone()), |candidates| Some(candidates & docids));
        }

        // remove last result docids from inner iterators
        if let Some(docids) = result_docids.as_ref() {
            for (_, query_docids, _) in self.query_level_iterator.iter_mut() {
                *query_docids -= docids;
            }
        }

        self.last_result = (result_pos, result_docids.unwrap_or_default());
    }

    /// return the score of the current inner interval.
    fn compute_rank(&self) -> u32 {
        // we compute a rank from the position.
        let (pos, _) = self.last_result;
        pos.saturating_sub((0..self.branch_size).sum()) * LCM_10_FIRST_NUMBERS / self.branch_size
    }

    fn cmp(&self, other: &Self) -> Ordering {
        let self_rank = self.compute_rank();
        let other_rank = other.compute_rank();

        // lower rank is better, and because BinaryHeap give the higher ranked branch, we reverse it.
        self_rank.cmp(&other_rank).reverse()
    }
}

impl<'t> Ord for Branch<'t> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

impl<'t> PartialOrd for Branch<'t> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'t> PartialEq for Branch<'t> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'t> Eq for Branch<'t> {}

fn initialize_set_buckets<'t>(
    ctx: &'t dyn Context<'t>,
    branches: &FlattenedQueryTree,
    allowed_candidates: &RoaringBitmap,
    wdcache: &mut WordDerivationsCache,
) -> Result<BinaryHeap<Branch<'t>>> {
    let mut heap = BinaryHeap::new();
    for flatten_branch in branches {
        let branch = Branch::new(ctx, flatten_branch, wdcache, allowed_candidates)?;
        heap.push(branch);
    }

    Ok(heap)
}

fn set_compute_candidates(
    branches_heap: &mut BinaryHeap<Branch>,
    allowed_candidates: &RoaringBitmap,
) -> Result<Option<(u32, RoaringBitmap)>> {
    let mut final_candidates: Option<(u32, RoaringBitmap)> = None;
    let mut allowed_candidates = allowed_candidates.clone();

    while let Some(mut branch) = branches_heap.peek_mut() {
        // if current is worst than best we break to return
        // candidates that correspond to the best rank
        let branch_rank = branch.compute_rank();
        if let Some((best_rank, _)) = final_candidates {
            if branch_rank > best_rank {
                break;
            }
        }

        let candidates = take(&mut branch.last_result.1);
        if candidates.is_empty() {
            // we don't have candidates, get next interval.
            if !branch.next(&allowed_candidates)? {
                PeekMut::pop(branch);
            }
        } else {
            allowed_candidates -= &candidates;
            final_candidates = match final_candidates.take() {
                // we add current candidates to best candidates
                Some((best_rank, mut best_candidates)) => {
                    best_candidates |= candidates;
                    branch.next(&allowed_candidates)?;
                    Some((best_rank, best_candidates))
                }
                // we take current candidates as best candidates
                None => {
                    branch.next(&allowed_candidates)?;
                    Some((branch_rank, candidates))
                }
            };
        }
    }

    Ok(final_candidates)
}

fn initialize_linear_buckets(
    ctx: &dyn Context,
    branches: &FlattenedQueryTree,
    allowed_candidates: &RoaringBitmap,
) -> Result<BTreeMap<u64, RoaringBitmap>> {
    fn compute_candidate_rank(
        branches: &FlattenedQueryTree,
        words_positions: HashMap<String, RoaringBitmap>,
    ) -> u64 {
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
                                    .flat_map(|positions| positions.iter().next())
                                    .min()
                            } else {
                                words_positions
                                    .get(word)
                                    .and_then(|positions| positions.iter().next())
                            }
                        }
                        QueryKind::Tolerant { typo, word } => {
                            word_derivations(word, *prefix, *typo, &words_positions)
                                .flat_map(|positions| positions.iter().next())
                                .min()
                        }
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
                let branch_rank: u64 =
                    branch_rank.into_iter().enumerate().map(|(i, r)| r - i as u64).sum();
                // here we do the means of the words of the branch
                min_rank =
                    min_rank.min(branch_rank * LCM_10_FIRST_NUMBERS as u64 / branch_len as u64);
            }
        }

        min_rank
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

    let mut candidates = BTreeMap::new();
    for docid in allowed_candidates {
        let words_positions = ctx.docid_words_positions(docid)?;
        let rank = compute_candidate_rank(branches, words_positions);
        candidates.entry(rank).or_insert_with(RoaringBitmap::new).insert(docid);
    }

    Ok(candidates)
}

// TODO can we keep refs of Query
fn flatten_query_tree(query_tree: &Operation) -> FlattenedQueryTree {
    use crate::search::criteria::Operation::{And, Or, Phrase};

    fn and_recurse(head: &Operation, tail: &[Operation]) -> FlattenedQueryTree {
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
            }
            None => recurse(head),
        }
    }

    fn recurse(op: &Operation) -> FlattenedQueryTree {
        match op {
            And(ops) => ops.split_first().map_or_else(Vec::new, |(h, t)| and_recurse(h, t)),
            Or(_, ops) => {
                if ops.iter().all(|op| op.query().is_some()) {
                    vec![vec![ops.iter().flat_map(|op| op.query()).cloned().collect()]]
                } else {
                    ops.iter().flat_map(recurse).collect()
                }
            }
            Phrase(words) => {
                let queries = words
                    .iter()
                    .filter_map(|w| w.as_ref())
                    .map(|word| vec![Query { prefix: false, kind: QueryKind::exact(word.clone()) }])
                    .collect();
                vec![queries]
            }
            Operation::Query(query) => vec![vec![vec![query.clone()]]],
        }
    }

    recurse(query_tree)
}

#[cfg(test)]
mod tests {
    use big_s::S;

    use super::*;
    use crate::search::criteria::QueryKind;

    #[test]
    fn simple_flatten_query_tree() {
        let query_tree = Operation::Or(
            false,
            vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("manythefish")) }),
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("manythe")) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("fish")) }),
                ]),
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact(S("many")) }),
                    Operation::Or(
                        false,
                        vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact(S("thefish")),
                            }),
                            Operation::And(vec![
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact(S("the")),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact(S("fish")),
                                }),
                            ]),
                        ],
                    ),
                ]),
            ],
        );
        let result = flatten_query_tree(&query_tree);

        insta::assert_debug_snapshot!(result, @r###"
        [
            [
                [
                    Exact {
                        word: "manythefish",
                    },
                ],
            ],
            [
                [
                    Exact {
                        word: "manythe",
                    },
                ],
                [
                    Exact {
                        word: "fish",
                    },
                ],
            ],
            [
                [
                    Exact {
                        word: "many",
                    },
                ],
                [
                    Exact {
                        word: "thefish",
                    },
                ],
            ],
            [
                [
                    Exact {
                        word: "many",
                    },
                ],
                [
                    Exact {
                        word: "the",
                    },
                ],
                [
                    Exact {
                        word: "fish",
                    },
                ],
            ],
        ]
        "###);
    }
}

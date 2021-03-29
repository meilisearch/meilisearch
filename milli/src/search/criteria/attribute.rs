use std::cmp;
use std::collections::{BTreeMap, HashMap, btree_map};
use std::mem::take;

use roaring::RoaringBitmap;

use crate::{search::build_dfa};
use crate::search::criteria::Query;
use crate::search::query_tree::{Operation, QueryKind};
use crate::search::WordDerivationsCache;
use super::{Criterion, CriterionResult, Context, resolve_query_tree};

pub struct Attribute<'t> {
    ctx: &'t dyn Context,
    query_tree: Option<Operation>,
    candidates: Option<RoaringBitmap>,
    bucket_candidates: RoaringBitmap,
    parent: Box<dyn Criterion + 't>,
    flattened_query_tree: Option<Vec<Vec<Vec<Query>>>>,
    current_buckets: Option<btree_map::IntoIter<u64, RoaringBitmap>>,
}

impl<'t> Attribute<'t> {
    pub fn new(ctx: &'t dyn Context, parent: Box<dyn Criterion + 't>) -> Self {
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

                    let current_buckets = match self.current_buckets.as_mut() {
                        Some(current_buckets) => current_buckets,
                        None => {
                            let new_buckets = linear_compute_candidates(self.ctx, flattened_query_tree, candidates)?;
                            self.current_buckets.get_or_insert(new_buckets.into_iter())
                        },
                    };

                    let found_candidates = match current_buckets.next() {
                        Some((_score, candidates)) => candidates,
                        None => {
                            return Ok(Some(CriterionResult {
                                query_tree: self.query_tree.take(),
                                candidates: self.candidates.take(),
                                bucket_candidates: take(&mut self.bucket_candidates),
                            }));
                        },
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

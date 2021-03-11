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
    parent: Option<Box<dyn Criterion + 't>>,
    flattened_query_tree: Option<Vec<Vec<Query>>>,
    current_buckets: Option<btree_map::IntoIter<u64, RoaringBitmap>>,
}

impl<'t> Attribute<'t> {
    pub fn initial(
        ctx: &'t dyn Context,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
    ) -> Self
    {
        Attribute {
            ctx,
            query_tree,
            candidates,
            bucket_candidates: RoaringBitmap::new(),
            parent: None,
            flattened_query_tree: None,
            current_buckets: None,
        }
    }

    pub fn new(ctx: &'t dyn Context, parent: Box<dyn Criterion + 't>) -> Self {
        Attribute {
            ctx,
            query_tree: None,
            candidates: None,
            bucket_candidates: RoaringBitmap::new(),
            parent: Some(parent),
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
                    let flattened_query_tree = self.flattened_query_tree.get_or_insert_with(|| flatten_query_tree(&qt));
                    let current_buckets = if let Some(current_buckets) = self.current_buckets.as_mut() {
                        current_buckets
                    } else {
                        let new_buckets = linear_compute_candidates(self.ctx, flattened_query_tree, candidates)?;
                        self.current_buckets.get_or_insert(new_buckets.into_iter())
                    };

                    let found_candidates = if let Some((_score, candidates)) = current_buckets.next() {
                        candidates
                    } else {
                        return Ok(Some(CriterionResult {
                            query_tree: self.query_tree.take(),
                            candidates: self.candidates.take(),
                            bucket_candidates: take(&mut self.bucket_candidates),
                        }));
                    };
                    candidates.difference_with(&found_candidates);

                    let bucket_candidates = match self.parent {
                        Some(_) => take(&mut self.bucket_candidates),
                        None => found_candidates.clone(),
                    };

                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.clone(),
                        candidates: Some(found_candidates),
                        bucket_candidates: bucket_candidates,
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
                    match self.parent.as_mut() {
                        Some(parent) => {
                            match parent.next(wdcache)? {
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
                        None => return Ok(None),
                    }
                },
            }
        }
    }
}

fn linear_compute_candidates(
    ctx: &dyn Context,
    branches: &Vec<Vec<Query>>,
    allowed_candidates: &RoaringBitmap,
) -> anyhow::Result<BTreeMap<u64, RoaringBitmap>>
{
    fn compute_candidate_rank(branches: &Vec<Vec<Query>>, words_positions: HashMap<String, RoaringBitmap>) -> u64 {
        let mut min_rank = u64::max_value();
        for branch in branches {
            let mut branch_rank = 0;
            for Query { prefix, kind } in branch {
                // find the best position of the current word in the document.
                let position =  match kind {
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

                // if a position is found, we add it to the branch score,
                // otherwise the branch is considered as unfindable in this document and we break.
                if let Some(position) = position {
                    branch_rank += position as u64;
                } else {
                    branch_rank = u64::max_value();
                    break;
                }
            }
            min_rank = min_rank.min(branch_rank);
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
fn flatten_query_tree(query_tree: &Operation) -> Vec<Vec<Query>> {
    use crate::search::criteria::Operation::{And, Or, Consecutive};

    fn and_recurse(head: &Operation, tail: &[Operation]) -> Vec<Vec<Query>> {
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

    fn recurse(op: &Operation) -> Vec<Vec<Query>> {
        match op {
            And(ops) | Consecutive(ops) => {
                ops.split_first().map_or_else(Vec::new, |(h, t)| and_recurse(h, t))
            },
            Or(_, ops) => ops.into_iter().map(recurse).flatten().collect(),
            Operation::Query(query) => vec![vec![query.clone()]],
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
            vec![Query { prefix: false, kind: QueryKind::exact(S("manythefish")) }],
            vec![
                Query { prefix: false, kind: QueryKind::exact(S("manythe")) },
                Query { prefix: false, kind: QueryKind::exact(S("fish")) },
            ],
            vec![
                Query { prefix: false, kind: QueryKind::exact(S("many")) },
                Query { prefix: false, kind: QueryKind::exact(S("thefish")) },
            ],
            vec![
                Query { prefix: false, kind: QueryKind::exact(S("many")) },
                Query { prefix: false, kind: QueryKind::exact(S("the")) },
                Query { prefix: false, kind: QueryKind::exact(S("fish")) },
            ],
        ];

        let result = flatten_query_tree(&query_tree);
        assert_eq!(expected, result);
    }
}

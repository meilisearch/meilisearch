use std::{borrow::Cow, collections::HashMap, mem::take};

use anyhow::bail;
use roaring::RoaringBitmap;

use crate::search::query_tree::{maximum_typo, Operation, Query, QueryKind};
use crate::search::word_derivations;
use super::{Candidates, Criterion, CriterionResult, Context, query_docids, query_pair_proximity_docids};

pub struct Typo<'t> {
    ctx: &'t dyn Context,
    query_tree: Option<(usize, Operation)>,
    number_typos: u8,
    candidates: Candidates,
    bucket_candidates: RoaringBitmap,
    parent: Option<Box<dyn Criterion>>,
    candidates_cache: HashMap<(Operation, u8), RoaringBitmap>,
    typo_cache: HashMap<(String, bool, u8), Vec<(String, u8)>>,
}

impl<'t> Typo<'t> {
    pub fn initial(
        ctx: &'t dyn Context,
        query_tree: Option<Operation>,
        candidates: Option<RoaringBitmap>,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Ok(Typo {
            ctx,
            query_tree: query_tree.map(|op| (maximum_typo(&op), op)),
            number_typos: 0,
            candidates: candidates.map_or_else(Candidates::default, Candidates::Allowed),
            bucket_candidates: RoaringBitmap::new(),
            parent: None,
            candidates_cache: HashMap::new(),
            typo_cache: HashMap::new(),
        })
    }

    pub fn new(
        ctx: &'t dyn Context,
        parent: Box<dyn Criterion>,
    ) -> anyhow::Result<Self> where Self: Sized
    {
        Ok(Typo {
            ctx,
            query_tree: None,
            number_typos: 0,
            candidates: Candidates::default(),
            bucket_candidates: RoaringBitmap::new(),
            parent: Some(parent),
            candidates_cache: HashMap::new(),
            typo_cache: HashMap::new(),
        })
    }
}

impl<'t> Criterion for Typo<'t> {
    fn next(&mut self) -> anyhow::Result<Option<CriterionResult>> {
        use Candidates::{Allowed, Forbidden};
        loop {
            match (&mut self.query_tree, &mut self.candidates) {
                (_, Allowed(candidates)) if candidates.is_empty() => {
                    self.query_tree = None;
                    self.candidates = Candidates::default();
                },
                (Some((max_typos, query_tree)), Allowed(candidates)) => {
                    if self.number_typos as usize > *max_typos {
                        self.query_tree = None;
                        self.candidates = Candidates::default();
                    } else {
                        let fst = self.ctx.words_fst();
                        let new_query_tree = if self.number_typos < 2 {
                            alterate_query_tree(&fst, query_tree.clone(), self.number_typos, &mut self.typo_cache)?
                        } else if self.number_typos == 2 {
                            *query_tree = alterate_query_tree(&fst, query_tree.clone(), self.number_typos, &mut self.typo_cache)?;
                            query_tree.clone()
                        } else {
                            query_tree.clone()
                        };

                        let mut new_candidates = resolve_candidates(self.ctx, &new_query_tree, self.number_typos, &mut self.candidates_cache)?;
                        new_candidates.intersect_with(&candidates);
                        candidates.difference_with(&new_candidates);
                        self.number_typos += 1;

                        let bucket_candidates = match self.parent {
                            Some(_) => take(&mut self.bucket_candidates),
                            None => new_candidates.clone(),
                        };

                        return Ok(Some(CriterionResult {
                            query_tree: Some(new_query_tree),
                            candidates: new_candidates,
                            bucket_candidates,
                        }));
                    }
                },
                (Some((max_typos, query_tree)), Forbidden(candidates)) => {
                    if self.number_typos as usize > *max_typos {
                        self.query_tree = None;
                        self.candidates = Candidates::default();
                    } else {
                        let fst = self.ctx.words_fst();
                        let new_query_tree = if self.number_typos < 2 {
                            alterate_query_tree(&fst, query_tree.clone(), self.number_typos, &mut self.typo_cache)?
                        } else if self.number_typos == 2 {
                            *query_tree = alterate_query_tree(&fst, query_tree.clone(), self.number_typos, &mut self.typo_cache)?;
                            query_tree.clone()
                        } else {
                            query_tree.clone()
                        };

                        let mut new_candidates = resolve_candidates(self.ctx, &new_query_tree, self.number_typos, &mut self.candidates_cache)?;
                        new_candidates.difference_with(&candidates);
                        candidates.union_with(&new_candidates);
                        self.number_typos += 1;

                        let bucket_candidates = match self.parent {
                            Some(_) => take(&mut self.bucket_candidates),
                            None => new_candidates.clone(),
                        };

                        return Ok(Some(CriterionResult {
                            query_tree: Some(new_query_tree),
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
                                    self.query_tree = query_tree.map(|op| (maximum_typo(&op), op));
                                    self.number_typos = 0;
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

/// Modify the query tree by replacing every tolerant query by an Or operation
/// containing all of the corresponding exact words in the words FST. Each tolerant
/// query will only be replaced by exact query with up to `number_typos` maximum typos.
fn alterate_query_tree(
    words_fst: &fst::Set<Cow<[u8]>>,
    mut query_tree: Operation,
    number_typos: u8,
    typo_cache: &mut HashMap<(String, bool, u8), Vec<(String, u8)>>,
) -> anyhow::Result<Operation>
{
    fn recurse(
        words_fst: &fst::Set<Cow<[u8]>>,
        operation: &mut Operation,
        number_typos: u8,
        typo_cache: &mut HashMap<(String, bool, u8), Vec<(String, u8)>>,
    ) -> anyhow::Result<()>
    {
        use Operation::{And, Consecutive, Or};

        match operation {
            And(ops) | Consecutive(ops) | Or(_, ops) => {
                ops.iter_mut().try_for_each(|op| recurse(words_fst, op, number_typos, typo_cache))
            },
            Operation::Query(q) => {
                // TODO may be optimized when number_typos == 0
                if let QueryKind::Tolerant { typo, word } = &q.kind {
                    // if no typo is allowed we don't call word_derivations function,
                    // and directly create an Exact query
                    if number_typos == 0 {
                        *operation = Operation::Query(Query {
                            prefix: q.prefix,
                            kind: QueryKind::Exact { original_typo: 0, word: word.clone() },
                        });
                    } else {
                        let typo = *typo.min(&number_typos);
                        let cache_key = (word.clone(), q.prefix, typo);
                        let words = if let Some(derivations) = typo_cache.get(&cache_key) {
                            derivations.clone()
                        } else {
                            let derivations = word_derivations(word, q.prefix, typo, words_fst)?;
                            typo_cache.insert(cache_key, derivations.clone());
                            derivations
                        };

                        let queries = words.into_iter().map(|(word, typo)| {
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::Exact { original_typo: typo, word },
                            })
                        }).collect();

                        *operation = Operation::or(false, queries);
                    }
                }

                Ok(())
            },
        }
    }

    recurse(words_fst, &mut query_tree, number_typos, typo_cache)?;
    Ok(query_tree)
}

fn resolve_candidates<'t>(
    ctx: &'t dyn Context,
    query_tree: &Operation,
    number_typos: u8,
    cache: &mut HashMap<(Operation, u8), RoaringBitmap>,
) -> anyhow::Result<RoaringBitmap>
{
    fn resolve_operation<'t>(
        ctx: &'t dyn Context,
        query_tree: &Operation,
        number_typos: u8,
        cache: &mut HashMap<(Operation, u8), RoaringBitmap>,
    ) -> anyhow::Result<RoaringBitmap>
    {
        use Operation::{And, Consecutive, Or, Query};

        match query_tree {
            And(ops) => {
                mdfs(ctx, ops, number_typos, cache)
            },
            Consecutive(ops) => {
                let mut candidates = RoaringBitmap::new();
                let mut first_loop = true;
                for slice in ops.windows(2) {
                    match (&slice[0], &slice[1]) {
                        (Operation::Query(left), Operation::Query(right)) => {
                            match query_pair_proximity_docids(ctx, left, right, 1)? {
                                pair_docids if pair_docids.is_empty() => {
                                    return Ok(RoaringBitmap::new())
                                },
                                pair_docids if first_loop => {
                                    candidates = pair_docids;
                                    first_loop = false;
                                },
                                pair_docids => {
                                    candidates.intersect_with(&pair_docids);
                                },
                            }
                        },
                        _ => bail!("invalid consecutive query type"),
                    }
                }
                Ok(candidates)
            },
            Or(_, ops) => {
                let mut candidates = RoaringBitmap::new();
                for op in ops {
                    let docids = resolve_operation(ctx, op, number_typos, cache)?;
                    candidates.union_with(&docids);
                }
                Ok(candidates)
            },
            Query(q) => if q.kind.typo() == number_typos {
                Ok(query_docids(ctx, q)?)
            } else {
                Ok(RoaringBitmap::new())
            },
        }
    }

    /// FIXME Make this function generic and mutualize it between Typo and proximity criterion
    fn mdfs<'t>(
        ctx: &'t dyn Context,
        branches: &[Operation],
        mana: u8,
        cache: &mut HashMap<(Operation, u8), RoaringBitmap>,
    ) -> anyhow::Result<RoaringBitmap>
    {
        match branches.split_first() {
            Some((head, [])) => {
                let cache_key = (head.clone(), mana);
                if let Some(candidates) = cache.get(&cache_key) {
                    Ok(candidates.clone())
                } else {
                    let candidates = resolve_operation(ctx, head, mana, cache)?;
                    cache.insert(cache_key, candidates.clone());
                    Ok(candidates)
                }
            },
            Some((head, tail)) => {
                let mut candidates = RoaringBitmap::new();

                for m in 0..=mana {
                    let mut head_candidates = {
                        let cache_key = (head.clone(), m);
                        if let Some(candidates) = cache.get(&cache_key) {
                            candidates.clone()
                        } else {
                            let candidates = resolve_operation(ctx, head, m, cache)?;
                            cache.insert(cache_key, candidates.clone());
                            candidates
                        }
                    };
                    if !head_candidates.is_empty() {
                        let tail_candidates = mdfs(ctx, tail, mana - m, cache)?;
                        head_candidates.intersect_with(&tail_candidates);
                        candidates.union_with(&head_candidates);
                    }
                }

                Ok(candidates)
            },
            None => Ok(RoaringBitmap::new()),
        }
    }

    resolve_operation(ctx, query_tree, number_typos, cache)
}

#[cfg(test)]
mod test {

    use super::*;
    use super::super::test::TestContext;

    #[test]
    fn initial_placeholder_no_facets() {
        let context = TestContext::default();
        let query_tree = None;
        let facet_candidates = None;

        let mut criteria = Typo::initial(&context, query_tree, facet_candidates).unwrap();

        assert!(criteria.next().unwrap().is_none());
    }

    #[test]
    fn initial_query_tree_no_facets() {
        let context = TestContext::default();
        let query_tree = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("split".to_string()) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("this".to_string()) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "world".to_string()) }),
            ])
        ]);

        let facet_candidates = None;

        let mut criteria = Typo::initial(&context, Some(query_tree), facet_candidates).unwrap();

        let candidates_1 = context.word_docids("split").unwrap().unwrap()
            & context.word_docids("this").unwrap().unwrap()
            & context.word_docids("world").unwrap().unwrap();
        let expected_1 = CriterionResult {
            query_tree: Some(Operation::Or(false, vec![
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("split".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("this".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("world".to_string()) }),
                ]),
            ])),
            candidates: candidates_1.clone(),
            bucket_candidates: candidates_1,
        };

        assert_eq!(criteria.next().unwrap(), Some(expected_1));

        let candidates_2 = (
                context.word_docids("split").unwrap().unwrap()
                & context.word_docids("this").unwrap().unwrap()
                & context.word_docids("word").unwrap().unwrap()
            ) - context.word_docids("world").unwrap().unwrap();
        let expected_2 = CriterionResult {
            query_tree: Some(Operation::Or(false, vec![
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("split".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("this".to_string()) }),
                    Operation::Or(false, vec![
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact_with_typo(1, "word".to_string()) }),
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("world".to_string()) }),
                    ]),
                ]),
            ])),
            candidates: candidates_2.clone(),
            bucket_candidates: candidates_2,
        };

        assert_eq!(criteria.next().unwrap(), Some(expected_2));
    }

    #[test]
    fn initial_placeholder_with_facets() {
        let context = TestContext::default();
        let query_tree = None;
        let facet_candidates = context.word_docids("earth").unwrap().unwrap();

        let mut criteria = Typo::initial(&context, query_tree, Some(facet_candidates.clone())).unwrap();

        let expected = CriterionResult {
            query_tree: None,
            candidates: facet_candidates.clone(),
            bucket_candidates: facet_candidates,
        };

        // first iteration, returns the facet candidates
        assert_eq!(criteria.next().unwrap(), Some(expected));

        // second iteration, returns None because there is no more things to do
        assert!(criteria.next().unwrap().is_none());
    }

    #[test]
    fn initial_query_tree_with_facets() {
        let context = TestContext::default();
        let query_tree = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("split".to_string()) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("this".to_string()) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "world".to_string()) }),
            ])
        ]);

        let facet_candidates = context.word_docids("earth").unwrap().unwrap();

        let mut criteria = Typo::initial(&context, Some(query_tree), Some(facet_candidates.clone())).unwrap();

        let candidates_1 = context.word_docids("split").unwrap().unwrap()
            & context.word_docids("this").unwrap().unwrap()
            & context.word_docids("world").unwrap().unwrap();
        let expected_1 = CriterionResult {
            query_tree: Some(Operation::Or(false, vec![
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("split".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("this".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("world".to_string()) }),
                ]),
            ])),
            candidates: &candidates_1 & &facet_candidates,
            bucket_candidates: candidates_1 & &facet_candidates,
        };

        assert_eq!(criteria.next().unwrap(), Some(expected_1));

        let candidates_2 = (
                context.word_docids("split").unwrap().unwrap()
                & context.word_docids("this").unwrap().unwrap()
                & context.word_docids("word").unwrap().unwrap()
            ) - context.word_docids("world").unwrap().unwrap();
        let expected_2 = CriterionResult {
            query_tree: Some(Operation::Or(false, vec![
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("split".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("this".to_string()) }),
                    Operation::Or(false, vec![
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact_with_typo(1, "word".to_string()) }),
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("world".to_string()) }),
                    ]),
                ]),
            ])),
            candidates: &candidates_2 & &facet_candidates,
            bucket_candidates: candidates_2 & &facet_candidates,
        };

        assert_eq!(criteria.next().unwrap(), Some(expected_2));
    }

}

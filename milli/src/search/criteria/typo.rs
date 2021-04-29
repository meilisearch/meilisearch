use std::{borrow::Cow, collections::HashMap, mem::take};

use anyhow::bail;
use log::debug;
use roaring::RoaringBitmap;

use crate::search::query_tree::{maximum_typo, Operation, Query, QueryKind};
use crate::search::{word_derivations, WordDerivationsCache};
use super::{
    Candidates,
    Context,
    Criterion,
    CriterionParameters,
    CriterionResult,
    query_docids,
    query_pair_proximity_docids
};

pub struct Typo<'t> {
    ctx: &'t dyn Context<'t>,
    query_tree: Option<(usize, Operation)>,
    number_typos: u8,
    candidates: Candidates,
    bucket_candidates: RoaringBitmap,
    parent: Box<dyn Criterion + 't>,
    candidates_cache: HashMap<(Operation, u8), RoaringBitmap>,
}

impl<'t> Typo<'t> {
    pub fn new(ctx: &'t dyn Context<'t>, parent: Box<dyn Criterion + 't>) -> Self {
        Typo {
            ctx,
            query_tree: None,
            number_typos: 0,
            candidates: Candidates::default(),
            bucket_candidates: RoaringBitmap::new(),
            parent,
            candidates_cache: HashMap::new(),
        }
    }
}

impl<'t> Criterion for Typo<'t> {
    #[logging_timer::time("Typo::{}")]
    fn next(&mut self, params: &mut CriterionParameters) -> anyhow::Result<Option<CriterionResult>> {
        use Candidates::{Allowed, Forbidden};
        // remove excluded candidates when next is called, instead of doing it in the loop.
        match &mut self.candidates {
            Allowed(candidates) => *candidates -= params.excluded_candidates,
            Forbidden(candidates) => *candidates |= params.excluded_candidates,
        }

        loop {
            debug!("Typo at iteration {} ({:?})", self.number_typos, self.candidates);

            match (&mut self.query_tree, &mut self.candidates) {
                (_, Allowed(candidates)) if candidates.is_empty() => {
                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.take().map(|(_, qt)| qt),
                        candidates: Some(take(&mut self.candidates).into_inner()),
                        bucket_candidates: take(&mut self.bucket_candidates),
                    }));
                },
                (Some((max_typos, query_tree)), Allowed(candidates)) => {
                    if self.number_typos as usize > *max_typos {
                        self.query_tree = None;
                        self.candidates = Candidates::default();
                    } else {
                        let fst = self.ctx.words_fst();
                        let new_query_tree = if self.number_typos < 2 {
                            alterate_query_tree(&fst, query_tree.clone(), self.number_typos, params.wdcache)?
                        } else if self.number_typos == 2 {
                            *query_tree = alterate_query_tree(&fst, query_tree.clone(), self.number_typos, params.wdcache)?;
                            query_tree.clone()
                        } else {
                            query_tree.clone()
                        };

                        let mut new_candidates = resolve_candidates(
                            self.ctx,
                            &new_query_tree,
                            self.number_typos,
                            &mut self.candidates_cache,
                            params.wdcache,
                        )?;
                        new_candidates.intersect_with(&candidates);
                        candidates.difference_with(&new_candidates);
                        self.number_typos += 1;

                        return Ok(Some(CriterionResult {
                            query_tree: Some(new_query_tree),
                            candidates: Some(new_candidates),
                            bucket_candidates: take(&mut self.bucket_candidates),
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
                            alterate_query_tree(&fst, query_tree.clone(), self.number_typos, params.wdcache)?
                        } else if self.number_typos == 2 {
                            *query_tree = alterate_query_tree(&fst, query_tree.clone(), self.number_typos, params.wdcache)?;
                            query_tree.clone()
                        } else {
                            query_tree.clone()
                        };

                        let mut new_candidates = resolve_candidates(
                            self.ctx,
                            &new_query_tree,
                            self.number_typos,
                            &mut self.candidates_cache,
                            params.wdcache,
                        )?;
                        new_candidates.difference_with(&candidates);
                        candidates.union_with(&new_candidates);
                        self.number_typos += 1;
                        self.bucket_candidates.union_with(&new_candidates);

                        return Ok(Some(CriterionResult {
                            query_tree: Some(new_query_tree),
                            candidates: Some(new_candidates),
                            bucket_candidates: take(&mut self.bucket_candidates),
                        }));
                    }
                },
                (None, Allowed(_)) => {
                    let candidates = take(&mut self.candidates).into_inner();
                    return Ok(Some(CriterionResult {
                        query_tree: None,
                        candidates: Some(candidates.clone()),
                        bucket_candidates: candidates,
                    }));
                },
                (None, Forbidden(_)) => {
                    match self.parent.next(params)? {
                        Some(CriterionResult { query_tree: None, candidates: None, bucket_candidates }) => {
                            return Ok(Some(CriterionResult {
                                query_tree: None,
                                candidates: None,
                                bucket_candidates,
                            }));
                        },
                        Some(CriterionResult { query_tree, candidates, bucket_candidates }) => {
                            self.query_tree = query_tree.map(|op| (maximum_typo(&op), op));
                            self.number_typos = 0;
                            self.candidates = candidates.map_or_else(|| {
                                Candidates::Forbidden(params.excluded_candidates.clone())
                            }, Candidates::Allowed);
                            self.bucket_candidates.union_with(&bucket_candidates);
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
    wdcache: &mut WordDerivationsCache,
) -> anyhow::Result<Operation>
{
    fn recurse(
        words_fst: &fst::Set<Cow<[u8]>>,
        operation: &mut Operation,
        number_typos: u8,
        wdcache: &mut WordDerivationsCache,
    ) -> anyhow::Result<()>
    {
        use Operation::{And, Consecutive, Or};

        match operation {
            And(ops) | Consecutive(ops) | Or(_, ops) => {
                ops.iter_mut().try_for_each(|op| recurse(words_fst, op, number_typos, wdcache))
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
                        let words = word_derivations(word, q.prefix, typo, words_fst, wdcache)?;
                        let queries = words.into_iter().map(|(word, typo)| {
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::Exact { original_typo: *typo, word: word.to_string() },
                            })
                        }).collect();

                        *operation = Operation::or(false, queries);
                    }
                }

                Ok(())
            },
        }
    }

    recurse(words_fst, &mut query_tree, number_typos, wdcache)?;
    Ok(query_tree)
}

fn resolve_candidates<'t>(
    ctx: &'t dyn Context,
    query_tree: &Operation,
    number_typos: u8,
    cache: &mut HashMap<(Operation, u8), RoaringBitmap>,
    wdcache: &mut WordDerivationsCache,
) -> anyhow::Result<RoaringBitmap>
{
    fn resolve_operation<'t>(
        ctx: &'t dyn Context,
        query_tree: &Operation,
        number_typos: u8,
        cache: &mut HashMap<(Operation, u8), RoaringBitmap>,
        wdcache: &mut WordDerivationsCache,
    ) -> anyhow::Result<RoaringBitmap>
    {
        use Operation::{And, Consecutive, Or, Query};

        match query_tree {
            And(ops) => {
                mdfs(ctx, ops, number_typos, cache, wdcache)
            },
            Consecutive(ops) => {
                let mut candidates = RoaringBitmap::new();
                let mut first_loop = true;
                for slice in ops.windows(2) {
                    match (&slice[0], &slice[1]) {
                        (Operation::Query(left), Operation::Query(right)) => {
                            match query_pair_proximity_docids(ctx, left, right, 1, wdcache)? {
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
                    let docids = resolve_operation(ctx, op, number_typos, cache, wdcache)?;
                    candidates.union_with(&docids);
                }
                Ok(candidates)
            },
            Query(q) => if q.kind.typo() == number_typos {
                Ok(query_docids(ctx, q, wdcache)?)
            } else {
                Ok(RoaringBitmap::new())
            },
        }
    }

    fn mdfs<'t>(
        ctx: &'t dyn Context,
        branches: &[Operation],
        mana: u8,
        cache: &mut HashMap<(Operation, u8), RoaringBitmap>,
        wdcache: &mut WordDerivationsCache,
    ) -> anyhow::Result<RoaringBitmap>
    {
        match branches.split_first() {
            Some((head, [])) => {
                let cache_key = (head.clone(), mana);
                if let Some(candidates) = cache.get(&cache_key) {
                    Ok(candidates.clone())
                } else {
                    let candidates = resolve_operation(ctx, head, mana, cache, wdcache)?;
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
                            let candidates = resolve_operation(ctx, head, m, cache, wdcache)?;
                            cache.insert(cache_key, candidates.clone());
                            candidates
                        }
                    };
                    if !head_candidates.is_empty() {
                        let tail_candidates = mdfs(ctx, tail, mana - m, cache, wdcache)?;
                        head_candidates.intersect_with(&tail_candidates);
                        candidates.union_with(&head_candidates);
                    }
                }

                Ok(candidates)
            },
            None => Ok(RoaringBitmap::new()),
        }
    }

    resolve_operation(ctx, query_tree, number_typos, cache, wdcache)
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::initial::Initial;
    use super::super::test::TestContext;

    #[test]
    fn initial_placeholder_no_facets() {
        let context = TestContext::default();
        let query_tree = None;
        let facet_candidates = None;

        let mut criterion_parameters = CriterionParameters {
            wdcache: &mut WordDerivationsCache::new(),
            excluded_candidates: &RoaringBitmap::new(),
        };

        let parent = Initial::new(query_tree, facet_candidates);
        let mut criteria = Typo::new(&context, Box::new(parent));

        assert!(criteria.next(&mut criterion_parameters).unwrap().unwrap().candidates.is_none());
        assert!(criteria.next(&mut criterion_parameters).unwrap().is_none());
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

        let mut criterion_parameters = CriterionParameters {
            wdcache: &mut WordDerivationsCache::new(),
            excluded_candidates: &RoaringBitmap::new(),
        };
        let parent = Initial::new(Some(query_tree), facet_candidates);
        let mut criteria = Typo::new(&context, Box::new(parent));

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
            candidates: Some(candidates_1.clone()),
            bucket_candidates: candidates_1,
        };

        assert_eq!(criteria.next(&mut criterion_parameters).unwrap(), Some(expected_1));

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
            candidates: Some(candidates_2.clone()),
            bucket_candidates: candidates_2,
        };

        assert_eq!(criteria.next(&mut criterion_parameters).unwrap(), Some(expected_2));
    }

    #[test]
    fn initial_placeholder_with_facets() {
        let context = TestContext::default();
        let query_tree = None;
        let facet_candidates = context.word_docids("earth").unwrap().unwrap();

        let mut criterion_parameters = CriterionParameters {
            wdcache: &mut WordDerivationsCache::new(),
            excluded_candidates: &RoaringBitmap::new(),
        };
        let parent = Initial::new(query_tree, Some(facet_candidates.clone()));
        let mut criteria = Typo::new(&context, Box::new(parent));

        let expected = CriterionResult {
            query_tree: None,
            candidates: Some(facet_candidates.clone()),
            bucket_candidates: facet_candidates,
        };

        // first iteration, returns the facet candidates
        assert_eq!(criteria.next(&mut criterion_parameters).unwrap(), Some(expected));

        // second iteration, returns None because there is no more things to do
        assert!(criteria.next(&mut criterion_parameters).unwrap().is_none());
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


        let mut criterion_parameters = CriterionParameters {
            wdcache: &mut WordDerivationsCache::new(),
            excluded_candidates: &RoaringBitmap::new(),
        };
        let parent = Initial::new(Some(query_tree), Some(facet_candidates.clone()));
        let mut criteria = Typo::new(&context, Box::new(parent));

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
            candidates: Some(&candidates_1 & &facet_candidates),
            bucket_candidates: facet_candidates.clone(),
        };

        assert_eq!(criteria.next(&mut criterion_parameters).unwrap(), Some(expected_1));

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
            candidates: Some(&candidates_2 & &facet_candidates),
            bucket_candidates: RoaringBitmap::new(),
        };

        assert_eq!(criteria.next(&mut criterion_parameters).unwrap(), Some(expected_2));
    }
}

use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::take;

use log::debug;
use roaring::RoaringBitmap;

use super::{
    query_docids, resolve_query_tree, Candidates, Context, Criterion, CriterionParameters,
    CriterionResult,
};
use crate::search::query_tree::{maximum_typo, Operation, Query, QueryKind};
use crate::search::{word_derivations, WordDerivationsCache};
use crate::Result;

/// Maximum number of typo for a word of any length.
const MAX_TYPOS_PER_WORD: u8 = 2;

pub struct Typo<'t> {
    ctx: &'t dyn Context<'t>,
    /// (max_typos, query_tree, candidates)
    state: Option<(u8, Operation, Candidates)>,
    typos: u8,
    bucket_candidates: Option<RoaringBitmap>,
    parent: Box<dyn Criterion + 't>,
    candidates_cache: HashMap<(Operation, u8), RoaringBitmap>,
}

impl<'t> Typo<'t> {
    pub fn new(ctx: &'t dyn Context<'t>, parent: Box<dyn Criterion + 't>) -> Self {
        Typo {
            ctx,
            state: None,
            typos: 0,
            bucket_candidates: None,
            parent,
            candidates_cache: HashMap::new(),
        }
    }
}

impl<'t> Criterion for Typo<'t> {
    #[logging_timer::time("Typo::{}")]
    fn next(&mut self, params: &mut CriterionParameters) -> Result<Option<CriterionResult>> {
        use Candidates::{Allowed, Forbidden};
        // remove excluded candidates when next is called, instead of doing it in the loop.
        match self.state.as_mut() {
            Some((_, _, Allowed(candidates))) => *candidates -= params.excluded_candidates,
            Some((_, _, Forbidden(candidates))) => *candidates |= params.excluded_candidates,
            None => (),
        }

        loop {
            debug!(
                "Typo at iteration {} (max typos {:?}) ({:?})",
                self.typos,
                self.state.as_ref().map(|(mt, _, _)| mt),
                self.state.as_ref().map(|(_, _, cd)| cd),
            );

            match self.state.as_mut() {
                Some((max_typos, _, _)) if self.typos > *max_typos => {
                    self.state = None; // reset state
                }
                Some((_, _, Allowed(allowed_candidates))) if allowed_candidates.is_empty() => {
                    self.state = None; // reset state
                }
                Some((_, query_tree, candidates_authorization)) => {
                    let fst = self.ctx.words_fst();
                    let new_query_tree = match self.typos {
                        typos if typos < MAX_TYPOS_PER_WORD => alterate_query_tree(
                            &fst,
                            query_tree.clone(),
                            self.typos,
                            params.wdcache,
                        )?,
                        MAX_TYPOS_PER_WORD => {
                            // When typos >= MAX_TYPOS_PER_WORD, no more alteration of the query tree is possible,
                            // we keep the altered query tree
                            *query_tree = alterate_query_tree(
                                &fst,
                                query_tree.clone(),
                                self.typos,
                                params.wdcache,
                            )?;
                            // we compute the allowed candidates
                            let query_tree_allowed_candidates =
                                resolve_query_tree(self.ctx, query_tree, params.wdcache)?;
                            // we assign the allowed candidates to the candidates authorization.
                            *candidates_authorization = match take(candidates_authorization) {
                                Allowed(allowed_candidates) => {
                                    Allowed(query_tree_allowed_candidates & allowed_candidates)
                                }
                                Forbidden(forbidden_candidates) => {
                                    Allowed(query_tree_allowed_candidates - forbidden_candidates)
                                }
                            };
                            query_tree.clone()
                        }
                        _otherwise => query_tree.clone(),
                    };

                    let mut candidates = resolve_candidates(
                        self.ctx,
                        &new_query_tree,
                        self.typos,
                        &mut self.candidates_cache,
                        params.wdcache,
                    )?;

                    match candidates_authorization {
                        Allowed(allowed_candidates) => {
                            candidates &= &*allowed_candidates;
                            *allowed_candidates -= &candidates;
                        }
                        Forbidden(forbidden_candidates) => {
                            candidates -= &*forbidden_candidates;
                            *forbidden_candidates |= &candidates;
                        }
                    }

                    let bucket_candidates = match self.bucket_candidates.as_mut() {
                        Some(bucket_candidates) => take(bucket_candidates),
                        None => candidates.clone(),
                    };

                    self.typos += 1;

                    return Ok(Some(CriterionResult {
                        query_tree: Some(new_query_tree),
                        candidates: Some(candidates),
                        filtered_candidates: None,
                        bucket_candidates: Some(bucket_candidates),
                    }));
                }
                None => match self.parent.next(params)? {
                    Some(CriterionResult {
                        query_tree: Some(query_tree),
                        candidates,
                        filtered_candidates,
                        bucket_candidates,
                    }) => {
                        self.bucket_candidates =
                            match (self.bucket_candidates.take(), bucket_candidates) {
                                (Some(self_bc), Some(parent_bc)) => Some(self_bc | parent_bc),
                                (self_bc, parent_bc) => self_bc.or(parent_bc),
                            };

                        let candidates = match candidates.or(filtered_candidates) {
                            Some(candidates) => {
                                Candidates::Allowed(candidates - params.excluded_candidates)
                            }
                            None => Candidates::Forbidden(params.excluded_candidates.clone()),
                        };

                        let maximum_typos = maximum_typo(&query_tree) as u8;
                        self.state = Some((maximum_typos, query_tree, candidates));
                        self.typos = 0;
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

/// Modify the query tree by replacing every tolerant query by an Or operation
/// containing all of the corresponding exact words in the words FST. Each tolerant
/// query will only be replaced by exact query with up to `number_typos` maximum typos.
fn alterate_query_tree(
    words_fst: &fst::Set<Cow<[u8]>>,
    mut query_tree: Operation,
    number_typos: u8,
    wdcache: &mut WordDerivationsCache,
) -> Result<Operation> {
    fn recurse(
        words_fst: &fst::Set<Cow<[u8]>>,
        operation: &mut Operation,
        number_typos: u8,
        wdcache: &mut WordDerivationsCache,
    ) -> Result<()> {
        use Operation::{And, Or, Phrase};

        match operation {
            And(ops) | Or(_, ops) => {
                ops.iter_mut().try_for_each(|op| recurse(words_fst, op, number_typos, wdcache))
            }
            // Because Phrases don't allow typos, no alteration can be done.
            Phrase(_words) => return Ok(()),
            Operation::Query(q) => {
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
                        let queries = words
                            .iter()
                            .map(|(word, typo)| {
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::Exact {
                                        original_typo: *typo,
                                        word: word.to_string(),
                                    },
                                })
                            })
                            .collect();

                        *operation = Operation::or(false, queries);
                    }
                }

                Ok(())
            }
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
) -> Result<RoaringBitmap> {
    fn resolve_operation<'t>(
        ctx: &'t dyn Context,
        query_tree: &Operation,
        number_typos: u8,
        cache: &mut HashMap<(Operation, u8), RoaringBitmap>,
        wdcache: &mut WordDerivationsCache,
    ) -> Result<RoaringBitmap> {
        use Operation::{And, Or, Phrase, Query};

        match query_tree {
            And(ops) => mdfs(ctx, ops, number_typos, cache, wdcache),
            Phrase(words) => {
                let mut candidates = RoaringBitmap::new();
                let mut first_loop = true;
                for slice in words.windows(2) {
                    let (left, right) = (&slice[0], &slice[1]);
                    match ctx.word_pair_proximity_docids(left, right, 1)? {
                        Some(pair_docids) => {
                            if pair_docids.is_empty() {
                                return Ok(RoaringBitmap::new());
                            } else if first_loop {
                                candidates = pair_docids;
                                first_loop = false;
                            } else {
                                candidates &= pair_docids;
                            }
                        }
                        None => return Ok(RoaringBitmap::new()),
                    }
                }
                Ok(candidates)
            }
            Or(_, ops) => {
                let mut candidates = RoaringBitmap::new();
                for op in ops {
                    let docids = resolve_operation(ctx, op, number_typos, cache, wdcache)?;
                    candidates |= docids;
                }
                Ok(candidates)
            }
            Query(q) => {
                if q.kind.typo() == number_typos {
                    Ok(query_docids(ctx, q, wdcache)?)
                } else {
                    Ok(RoaringBitmap::new())
                }
            }
        }
    }

    fn mdfs<'t>(
        ctx: &'t dyn Context,
        branches: &[Operation],
        mana: u8,
        cache: &mut HashMap<(Operation, u8), RoaringBitmap>,
        wdcache: &mut WordDerivationsCache,
    ) -> Result<RoaringBitmap> {
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
            }
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
                        head_candidates &= tail_candidates;
                        candidates |= head_candidates;
                    }
                }

                Ok(candidates)
            }
            None => Ok(RoaringBitmap::new()),
        }
    }

    resolve_operation(ctx, query_tree, number_typos, cache, wdcache)
}

#[cfg(test)]
mod test {
    use super::super::initial::Initial;
    use super::super::test::TestContext;
    use super::*;
    use crate::search::NoopDistinct;

    fn display_criteria(mut criteria: Typo, mut parameters: CriterionParameters) -> String {
        let mut result = String::new();
        while let Some(criterion) = criteria.next(&mut parameters).unwrap() {
            result.push_str(&format!("{criterion:?}\n\n"));
        }
        result
    }

    #[test]
    fn initial_placeholder_no_facets() {
        let context = TestContext::default();
        let query_tree = None;
        let facet_candidates = None;

        let criterion_parameters = CriterionParameters {
            wdcache: &mut WordDerivationsCache::new(),
            excluded_candidates: &RoaringBitmap::new(),
        };

        let parent =
            Initial::<NoopDistinct>::new(&context, query_tree, facet_candidates, false, None);
        let criteria = Typo::new(&context, Box::new(parent));

        let result = display_criteria(criteria, criterion_parameters);
        insta::assert_snapshot!(result, @r###"
        CriterionResult { query_tree: None, candidates: None, filtered_candidates: None, bucket_candidates: None }

        "###);
    }

    #[test]
    fn initial_query_tree_no_facets() {
        let context = TestContext::default();
        let query_tree = Operation::Or(
            false,
            vec![Operation::And(vec![
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::exact("split".to_string()),
                }),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::exact("this".to_string()),
                }),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(1, "world".to_string()),
                }),
            ])],
        );

        let facet_candidates = None;

        let criterion_parameters = CriterionParameters {
            wdcache: &mut WordDerivationsCache::new(),
            excluded_candidates: &RoaringBitmap::new(),
        };
        let parent =
            Initial::<NoopDistinct>::new(&context, Some(query_tree), facet_candidates, false, None);
        let criteria = Typo::new(&context, Box::new(parent));

        let result = display_criteria(criteria, criterion_parameters);
        insta::assert_snapshot!(result, @r###"
        CriterionResult { query_tree: Some(OR
          AND
            Exact { word: "split" }
            Exact { word: "this" }
            Exact { word: "world" }
        ), candidates: Some(RoaringBitmap<[]>), filtered_candidates: None, bucket_candidates: Some(RoaringBitmap<[]>) }

        CriterionResult { query_tree: Some(OR
          AND
            Exact { word: "split" }
            Exact { word: "this" }
            OR
              Exact { word: "word" }
              Exact { word: "world" }
        ), candidates: Some(RoaringBitmap<[]>), filtered_candidates: None, bucket_candidates: Some(RoaringBitmap<[]>) }

        "###);
    }

    #[test]
    fn initial_placeholder_with_facets() {
        let context = TestContext::default();
        let query_tree = None;
        let facet_candidates = context.word_docids("earth").unwrap().unwrap();

        let criterion_parameters = CriterionParameters {
            wdcache: &mut WordDerivationsCache::new(),
            excluded_candidates: &RoaringBitmap::new(),
        };
        let parent = Initial::<NoopDistinct>::new(
            &context,
            query_tree,
            Some(facet_candidates.clone()),
            false,
            None,
        );
        let criteria = Typo::new(&context, Box::new(parent));

        let result = display_criteria(criteria, criterion_parameters);
        insta::assert_snapshot!(result, @r###"
        CriterionResult { query_tree: None, candidates: None, filtered_candidates: Some(RoaringBitmap<8000 values between 986424 and 4294786076>), bucket_candidates: None }

        "###);
    }

    #[test]
    fn initial_query_tree_with_facets() {
        let context = TestContext::default();
        let query_tree = Operation::Or(
            false,
            vec![Operation::And(vec![
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::exact("split".to_string()),
                }),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::exact("this".to_string()),
                }),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(1, "world".to_string()),
                }),
            ])],
        );

        let facet_candidates = context.word_docids("earth").unwrap().unwrap();

        let criterion_parameters = CriterionParameters {
            wdcache: &mut WordDerivationsCache::new(),
            excluded_candidates: &RoaringBitmap::new(),
        };
        let parent = Initial::<NoopDistinct>::new(
            &context,
            Some(query_tree),
            Some(facet_candidates.clone()),
            false,
            None,
        );
        let criteria = Typo::new(&context, Box::new(parent));

        let result = display_criteria(criteria, criterion_parameters);
        insta::assert_snapshot!(result, @r###"
        CriterionResult { query_tree: Some(OR
          AND
            Exact { word: "split" }
            Exact { word: "this" }
            Exact { word: "world" }
        ), candidates: Some(RoaringBitmap<[]>), filtered_candidates: None, bucket_candidates: Some(RoaringBitmap<[]>) }

        CriterionResult { query_tree: Some(OR
          AND
            Exact { word: "split" }
            Exact { word: "this" }
            OR
              Exact { word: "word" }
              Exact { word: "world" }
        ), candidates: Some(RoaringBitmap<[]>), filtered_candidates: None, bucket_candidates: Some(RoaringBitmap<[]>) }

        "###);
    }
}

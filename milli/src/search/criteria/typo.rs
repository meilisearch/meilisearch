use std::{borrow::Cow, mem::take};

use anyhow::bail;
use roaring::RoaringBitmap;

use crate::search::query_tree::{Operation, Query, QueryKind};
use crate::search::word_typos;
use super::{Candidates, Criterion, CriterionResult, Context, query_docids, query_pair_proximity_docids};

// FIXME we must stop when the number of typos is equal to
// the maximum number of typos for this query tree.
const MAX_NUM_TYPOS: u8 = 8;

pub struct Typo<'t> {
    ctx: &'t dyn Context,
    query_tree: Option<Operation>,
    number_typos: u8,
    candidates: Candidates,
    bucket_candidates: Option<RoaringBitmap>,
    parent: Option<Box<dyn Criterion>>,
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
            query_tree,
            number_typos: 0,
            candidates: candidates.map_or_else(Candidates::default, Candidates::Allowed),
            bucket_candidates: None,
            parent: None,
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
            bucket_candidates: None,
            parent: Some(parent),
        })
    }
}

impl<'t> Criterion for Typo<'t> {
    fn next(&mut self) -> anyhow::Result<Option<CriterionResult>> {
        use Candidates::{Allowed, Forbidden};
        while self.number_typos < MAX_NUM_TYPOS {
            match (&mut self.query_tree, &mut self.candidates) {
                (_, Allowed(candidates)) if candidates.is_empty() => {
                    self.query_tree = None;
                    self.candidates = Candidates::default();
                },
                (Some(query_tree), Allowed(candidates)) => {
                    let fst = self.ctx.words_fst();
                    let new_query_tree = if self.number_typos < 2 {
                        alterate_query_tree(&fst, query_tree.clone(), self.number_typos)?
                    } else if self.number_typos == 2 {
                        *query_tree = alterate_query_tree(&fst, query_tree.clone(), self.number_typos)?;
                        query_tree.clone()
                    } else {
                        query_tree.clone()
                    };

                    let mut new_candidates = resolve_candidates(self.ctx, &new_query_tree, self.number_typos)?;
                    new_candidates.intersect_with(&candidates);
                    candidates.difference_with(&new_candidates);
                    self.number_typos += 1;

                    let bucket_candidates = match self.parent {
                        Some(_) => self.bucket_candidates.take(),
                        None => Some(new_candidates.clone()),
                    };

                    return Ok(Some(CriterionResult {
                        query_tree: Some(new_query_tree),
                        candidates: new_candidates,
                        bucket_candidates,
                    }));
                },
                (Some(query_tree), Forbidden(candidates)) => {
                    let fst = self.ctx.words_fst();
                    let new_query_tree = if self.number_typos < 2 {
                        alterate_query_tree(&fst, query_tree.clone(), self.number_typos)?
                    } else if self.number_typos == 2 {
                        *query_tree = alterate_query_tree(&fst, query_tree.clone(), self.number_typos)?;
                        query_tree.clone()
                    } else {
                        query_tree.clone()
                    };

                    let mut new_candidates = resolve_candidates(self.ctx, &new_query_tree, self.number_typos)?;
                    new_candidates.difference_with(&candidates);
                    candidates.union_with(&new_candidates);
                    self.number_typos += 1;

                    let bucket_candidates = match self.parent {
                        Some(_) => self.bucket_candidates.take(),
                        None => Some(new_candidates.clone()),
                    };

                    return Ok(Some(CriterionResult {
                        query_tree: Some(new_query_tree),
                        candidates: new_candidates,
                        bucket_candidates,
                    }));
                },
                (None, Allowed(_)) => {
                    let candidates = take(&mut self.candidates).into_inner();
                    return Ok(Some(CriterionResult {
                        query_tree: None,
                        candidates: candidates.clone(),
                        bucket_candidates: Some(candidates),
                    }));
                },
                (None, Forbidden(_)) => {
                    match self.parent.as_mut() {
                        Some(parent) => {
                            match parent.next()? {
                                Some(CriterionResult { query_tree, candidates, bucket_candidates }) => {
                                    self.query_tree = query_tree;
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

        Ok(None)
    }
}

/// Modify the query tree by replacing every tolerant query by an Or operation
/// containing all of the corresponding exact words in the words FST. Each tolerant
/// query will only be replaced by exact query with up to `number_typos` maximum typos.
fn alterate_query_tree(
    words_fst: &fst::Set<Cow<[u8]>>,
    mut query_tree: Operation,
    number_typos: u8,
) -> anyhow::Result<Operation>
{
    fn recurse(
        words_fst: &fst::Set<Cow<[u8]>>,
        operation: &mut Operation,
        number_typos: u8,
    ) -> anyhow::Result<()>
    {
        use Operation::{And, Consecutive, Or};

        match operation {
            And(ops) | Consecutive(ops) | Or(_, ops) => {
                ops.iter_mut().try_for_each(|op| recurse(words_fst, op, number_typos))
            },
            Operation::Query(q) => {
                // TODO may be optimized when number_typos == 0
                if let QueryKind::Tolerant { typo, word } = &q.kind {
                    let typo = *typo.min(&number_typos);
                    let words = word_typos(word, q.prefix, typo, words_fst)?;

                    let queries = words.into_iter().map(|(word, _typo)| {
                        Operation::Query(Query {
                            prefix: false,
                            kind: QueryKind::Exact { original_typo: typo, word },
                        })
                    }).collect();

                    *operation = Operation::or(false, queries);
                }

                Ok(())
            },
        }
    }

    recurse(words_fst, &mut query_tree, number_typos)?;
    Ok(query_tree)
}

fn resolve_candidates<'t>(
    ctx: &'t dyn Context,
    query_tree: &Operation,
    number_typos: u8,
) -> anyhow::Result<RoaringBitmap>
{
    // FIXME add a cache
    // FIXME keep the cache between typos iterations
    // cache: HashMap<(&Operation, u8), RoaringBitmap>,

    fn resolve_operation<'t>(
        ctx: &'t dyn Context,
        query_tree: &Operation,
        number_typos: u8,
    ) -> anyhow::Result<RoaringBitmap>
    {
        use Operation::{And, Consecutive, Or, Query};

        match query_tree {
            And(ops) => {
                mdfs(ctx, ops, number_typos)
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
                    let docids = resolve_operation(ctx, op, number_typos)?;
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
    ) -> anyhow::Result<RoaringBitmap>
    {
        match branches.split_first() {
            Some((head, [])) => resolve_operation(ctx, head, mana),
            Some((head, tail)) => {
                let mut candidates = RoaringBitmap::new();

                for m in 0..=mana {
                    let mut head_candidates = resolve_operation(ctx, head, m)?;
                    if !head_candidates.is_empty() {
                        let tail_candidates = mdfs(ctx, tail, mana - m)?;
                        head_candidates.intersect_with(&tail_candidates);
                        candidates.union_with(&head_candidates);
                    }
                }

                Ok(candidates)
            },
            None => Ok(RoaringBitmap::new()),
        }
    }

    resolve_operation(ctx, query_tree, number_typos)
}

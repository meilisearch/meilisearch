use std::{borrow::Cow, mem::take};

use anyhow::bail;
use roaring::RoaringBitmap;

use crate::search::query_tree::{Operation, Query, QueryKind};
use crate::search::word_typos;
use super::{Candidates, Criterion, Context};

// FIXME we must stop when the number of typos is equal to
// the maximum number of typos for this query tree.
const MAX_NUM_TYPOS: u8 = 8;

pub struct Typo<'t> {
    ctx: &'t dyn Context,
    query_tree: Option<Operation>,
    number_typos: u8,
    candidates: Candidates,
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
            parent: Some(parent),
        })
    }
}

impl<'t> Criterion for Typo<'t> {
    fn next(&mut self) -> anyhow::Result<Option<(Option<Operation>, RoaringBitmap)>> {
        use Candidates::{Allowed, Forbidden};
        while self.number_typos < MAX_NUM_TYPOS {
            match (&mut self.query_tree, &mut self.candidates) {
                (_, Allowed(candidates)) if candidates.is_empty() => {
                    self.query_tree = None;
                    self.candidates = Candidates::default();
                },
                (Some(query_tree), Allowed(candidates)) => {
                    // TODO if number_typos >= 2 the generated query_tree will allways be the same,
                    // generate a new one on each iteration is a waste of time.
                    let new_query_tree = alterate_query_tree(&self.ctx.words_fst(), query_tree.clone(), self.number_typos)?;
                    let mut new_candidates = resolve_candidates(self.ctx, &new_query_tree, self.number_typos)?;
                    new_candidates.intersect_with(&candidates);
                    candidates.difference_with(&new_candidates);
                    self.number_typos += 1;

                    return Ok(Some((Some(new_query_tree), new_candidates)));
                },
                (Some(query_tree), Forbidden(candidates)) => {
                    // TODO if number_typos >= 2 the generated query_tree will allways be the same,
                    // generate a new one on each iteration is a waste of time.
                    let new_query_tree = alterate_query_tree(&self.ctx.words_fst(), query_tree.clone(), self.number_typos)?;
                    let mut new_candidates = resolve_candidates(self.ctx, &new_query_tree, self.number_typos)?;
                    new_candidates.difference_with(&candidates);
                    candidates.union_with(&new_candidates);
                    self.number_typos += 1;

                    return Ok(Some((Some(new_query_tree), new_candidates)));
                },
                (None, Allowed(_)) => {
                    return Ok(Some((None, take(&mut self.candidates).into_inner())));
                },
                (None, Forbidden(_)) => {
                    match self.parent.as_mut() {
                        Some(parent) => {
                            match parent.next()? {
                                Some((query_tree, candidates)) => {
                                    self.query_tree = query_tree;
                                    self.candidates = Candidates::Allowed(candidates);
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
                            match ctx.query_pair_proximity_docids(left, right, 1)? {
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
                Ok(ctx.query_docids(q)?)
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

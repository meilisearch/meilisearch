use log::debug;
use roaring::RoaringBitmap;

use crate::search::criteria::Query;
use crate::search::query_tree::Operation;
use crate::search::WordDerivationsCache;
use super::{Criterion, CriterionResult, Context};

pub struct Attribute<'t> {
    ctx: &'t dyn Context,
    query_tree: Option<Operation>,
    candidates: Option<RoaringBitmap>,
    bucket_candidates: RoaringBitmap,
    parent: Option<Box<dyn Criterion + 't>>,
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
        }
    }

    pub fn new(ctx: &'t dyn Context, parent: Box<dyn Criterion + 't>) -> Self {
        Attribute {
            ctx,
            query_tree: None,
            candidates: None,
            bucket_candidates: RoaringBitmap::new(),
            parent: Some(parent),
        }
    }
}

impl<'t> Criterion for Attribute<'t> {
    #[logging_timer::time("Attribute::{}")]
    fn next(&mut self, wdcache: &mut WordDerivationsCache) -> anyhow::Result<Option<CriterionResult>> {
        todo!("Attribute")
    }
}

// TODO can we keep refs of Query
fn explode_query_tree(query_tree: &Operation) -> Vec<Vec<Query>> {
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
    fn simple_explode_query_tree() {
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

        let result = explode_query_tree(&query_tree);
        assert_eq!(expected, result);
    }
}

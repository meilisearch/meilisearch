#![allow(clippy::too_many_arguments)]

use super::ProximityCondition;
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_graph::QueryNodeData;
use crate::search::new::query_term::LocatedQueryTerm;
use crate::search::new::{QueryNode, SearchContext};
use crate::Result;

pub fn build_edges<'ctx>(
    _ctx: &mut SearchContext<'ctx>,
    conditions_interner: &mut DedupInterner<ProximityCondition>,
    from_node: &QueryNode,
    to_node: &QueryNode,
) -> Result<Vec<(u8, Option<Interned<ProximityCondition>>)>> {
    let right_term = match &to_node.data {
        QueryNodeData::End => return Ok(vec![(0, None)]),
        QueryNodeData::Deleted | QueryNodeData::Start => return Ok(vec![]),
        QueryNodeData::Term(term) => term,
    };

    let LocatedQueryTerm { value: right_term_interned, positions: right_positions } = right_term;

    let (right_start_position, right_ngram_length) =
        (*right_positions.start(), right_positions.len());

    let (left_term_interned, left_end_position) = match &from_node.data {
        QueryNodeData::Term(LocatedQueryTerm { value, positions }) => (*value, *positions.end()),
        QueryNodeData::Deleted => return Ok(vec![]),
        QueryNodeData::Start => {
            return Ok(vec![(
                (right_ngram_length - 1) as u8,
                Some(
                    conditions_interner
                        .insert(ProximityCondition::Term { term: *right_term_interned }),
                ),
            )])
        }
        QueryNodeData::End => return Ok(vec![]),
    };

    if left_end_position + 1 != right_start_position {
        // We want to ignore this pair of terms
        // Unconditionally walk through the edge without computing the docids
        // This can happen when, in a query like `the sun flowers are beautiful`, the term
        // `flowers` is removed by the `words` ranking rule.
        // The remaining query graph represents `the sun .. are beautiful`
        // but `sun` and `are` have no proximity condition between them
        return Ok(vec![(
            (right_ngram_length - 1) as u8,
            Some(
                conditions_interner.insert(ProximityCondition::Term { term: *right_term_interned }),
            ),
        )]);
    }

    let mut conditions = vec![];
    for cost in right_ngram_length..(7 + right_ngram_length) {
        let cost = cost as u8;
        conditions.push((
            cost,
            Some(conditions_interner.insert(ProximityCondition::Uninit {
                left_term: left_term_interned,
                right_term: *right_term_interned,
                right_term_ngram_len: right_ngram_length as u8,
                cost,
            })),
        ))
    }

    conditions.push((
        (7 + right_ngram_length) as u8,
        Some(conditions_interner.insert(ProximityCondition::Term { term: *right_term_interned })),
    ));

    Ok(conditions)
}

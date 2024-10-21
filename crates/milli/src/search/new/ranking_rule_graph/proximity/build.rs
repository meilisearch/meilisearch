#![allow(clippy::too_many_arguments)]

use super::ProximityCondition;
use crate::proximity::MAX_DISTANCE;
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::SearchContext;
use crate::Result;

pub fn build_edges(
    _ctx: &mut SearchContext<'_>,
    conditions_interner: &mut DedupInterner<ProximityCondition>,
    left_term: Option<&LocatedQueryTermSubset>,
    right_term: &LocatedQueryTermSubset,
) -> Result<Vec<(u32, Interned<ProximityCondition>)>> {
    let right_ngram_max = right_term.term_ids.len().saturating_sub(1);

    let Some(left_term) = left_term else {
        return Ok(vec![(
            right_ngram_max as u32,
            conditions_interner.insert(ProximityCondition::Term { term: right_term.clone() }),
        )]);
    };

    if left_term.positions.end() + 1 != *right_term.positions.start() {
        // We want to ignore this pair of terms
        // Unconditionally walk through the edge without computing the docids
        // This can happen when, in a query like `the sun flowers are beautiful`, the term
        // `flowers` is removed by the `words` ranking rule.
        // The remaining query graph represents `the sun .. are beautiful`
        // but `sun` and `are` have no proximity condition between them
        return Ok(vec![(
            right_ngram_max as u32,
            conditions_interner.insert(ProximityCondition::Term { term: right_term.clone() }),
        )]);
    }

    let mut conditions = vec![];
    for cost in right_ngram_max..(((MAX_DISTANCE as usize) - 1) + right_ngram_max) {
        conditions.push((
            cost as u32,
            conditions_interner.insert(ProximityCondition::Uninit {
                left_term: left_term.clone(),
                right_term: right_term.clone(),
                cost: (cost + 1) as u8,
            }),
        ))
    }

    conditions.push((
        ((MAX_DISTANCE - 1) + (right_ngram_max as u32)),
        conditions_interner.insert(ProximityCondition::Term { term: right_term.clone() }),
    ));

    Ok(conditions)
}

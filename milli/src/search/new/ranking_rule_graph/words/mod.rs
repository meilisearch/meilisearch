use roaring::RoaringBitmap;

use super::{ComputedCondition, RankingRuleGraphTrait};
use crate::score_details::{self, Rank, ScoreDetails};
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids;
use crate::search::new::SearchContext;
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct WordsCondition {
    term: LocatedQueryTermSubset,
}

pub enum WordsGraph {}

impl RankingRuleGraphTrait for WordsGraph {
    type Condition = WordsCondition;

    fn resolve_condition(
        ctx: &mut SearchContext,
        condition: &Self::Condition,
        universe: &RoaringBitmap,
    ) -> Result<ComputedCondition> {
        let WordsCondition { term, .. } = condition;
        // maybe compute_query_term_subset_docids should accept a universe as argument
        let docids = compute_query_term_subset_docids(ctx, Some(universe), &term.term_subset)?;

        Ok(ComputedCondition {
            docids,
            universe_len: universe.len(),
            start_term_subset: None,
            end_term_subset: term.clone(),
        })
    }

    fn build_edges(
        _ctx: &mut SearchContext,
        conditions_interner: &mut DedupInterner<Self::Condition>,
        _from: Option<&LocatedQueryTermSubset>,
        to_term: &LocatedQueryTermSubset,
    ) -> Result<Vec<(u32, Interned<Self::Condition>)>> {
        Ok(vec![(0, conditions_interner.insert(WordsCondition { term: to_term.clone() }))])
    }

    fn rank_to_score(rank: Rank) -> ScoreDetails {
        ScoreDetails::Words(score_details::Words::from_rank(rank))
    }
}

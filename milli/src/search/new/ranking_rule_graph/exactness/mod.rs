use roaring::RoaringBitmap;

use super::{ComputedCondition, RankingRuleGraphTrait};
use crate::score_details::{self, Rank, ScoreDetails};
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::{ExactTerm, LocatedQueryTermSubset};
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids;
use crate::search::new::Word;
use crate::{Result, SearchContext};

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ExactnessCondition {
    ExactInAttribute(LocatedQueryTermSubset),
    Any(LocatedQueryTermSubset),
}

pub enum ExactnessGraph {}

fn compute_docids(
    ctx: &mut SearchContext,
    dest_node: &LocatedQueryTermSubset,
    universe: &RoaringBitmap,
) -> Result<RoaringBitmap> {
    let exact_term = if let Some(exact_term) = dest_node.term_subset.exact_term(ctx) {
        exact_term
    } else {
        return Ok(Default::default());
    };

    let candidates = match exact_term {
        // TODO I move the intersection here
        ExactTerm::Phrase(phrase) => ctx.get_phrase_docids(None, phrase)? & universe,
        ExactTerm::Word(word) => {
            ctx.word_docids(Some(universe), Word::Original(word))?.unwrap_or_default()
        }
    };

    Ok(candidates)
}

impl RankingRuleGraphTrait for ExactnessGraph {
    type Condition = ExactnessCondition;

    fn resolve_condition(
        ctx: &mut SearchContext,
        condition: &Self::Condition,
        universe: &RoaringBitmap,
    ) -> Result<ComputedCondition> {
        let (docids, end_term_subset) = match condition {
            ExactnessCondition::ExactInAttribute(dest_node) => {
                let mut end_term_subset = dest_node.clone();
                end_term_subset.term_subset.keep_only_exact_term(ctx);
                end_term_subset.term_subset.make_mandatory();
                (compute_docids(ctx, dest_node, universe)?, end_term_subset)
            }
            ExactnessCondition::Any(dest_node) => {
                let docids =
                    compute_query_term_subset_docids(ctx, Some(universe), &dest_node.term_subset)?;
                (docids, dest_node.clone())
            }
        };

        Ok(ComputedCondition {
            docids,
            universe_len: universe.len(),
            start_term_subset: None,
            end_term_subset,
        })
    }

    fn build_edges(
        _ctx: &mut SearchContext,
        conditions_interner: &mut DedupInterner<Self::Condition>,
        _source_node: Option<&LocatedQueryTermSubset>,
        dest_node: &LocatedQueryTermSubset,
    ) -> Result<Vec<(u32, Interned<Self::Condition>)>> {
        let exact_condition = ExactnessCondition::ExactInAttribute(dest_node.clone());
        let exact_condition = conditions_interner.insert(exact_condition);

        let skip_condition = ExactnessCondition::Any(dest_node.clone());
        let skip_condition = conditions_interner.insert(skip_condition);

        Ok(vec![(0, exact_condition), (dest_node.term_ids.len() as u32, skip_condition)])
    }

    fn rank_to_score(rank: Rank) -> ScoreDetails {
        ScoreDetails::ExactWords(score_details::ExactWords::from_rank(rank))
    }
}

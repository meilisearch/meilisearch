use fxhash::FxHashSet;
use roaring::RoaringBitmap;

use super::{ComputedCondition, RankingRuleGraphTrait};
use crate::score_details::{Rank, ScoreDetails};
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids_within_field_id;
use crate::search::new::SearchContext;
use crate::{FieldId, InternalError, Result};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FidCondition {
    term: LocatedQueryTermSubset,
    fid: Option<FieldId>,
}

pub enum FidGraph {}

impl RankingRuleGraphTrait for FidGraph {
    type Condition = FidCondition;

    fn resolve_condition(
        ctx: &mut SearchContext,
        condition: &Self::Condition,
        universe: &RoaringBitmap,
    ) -> Result<ComputedCondition> {
        let FidCondition { term, .. } = condition;

        let docids = if let Some(fid) = condition.fid {
            compute_query_term_subset_docids_within_field_id(
                ctx,
                Some(universe),
                &term.term_subset,
                fid,
            )?
        } else {
            RoaringBitmap::new()
        };

        Ok(ComputedCondition {
            docids,
            universe_len: universe.len(),
            start_term_subset: None,
            end_term_subset: term.clone(),
        })
    }

    fn build_edges(
        ctx: &mut SearchContext,
        conditions_interner: &mut DedupInterner<Self::Condition>,
        _from: Option<&LocatedQueryTermSubset>,
        to_term: &LocatedQueryTermSubset,
    ) -> Result<Vec<(u32, Interned<Self::Condition>)>> {
        let term = to_term;

        let mut all_fields = FxHashSet::default();
        for word in term.term_subset.all_single_words_except_prefix_db(ctx)? {
            let fields = ctx.get_db_word_fids(word.interned())?;
            all_fields.extend(fields);
        }

        for phrase in term.term_subset.all_phrases(ctx)? {
            for &word in phrase.words(ctx).iter().flatten() {
                let fields = ctx.get_db_word_fids(word)?;
                all_fields.extend(fields);
            }
        }

        if let Some(word_prefix) = term.term_subset.use_prefix_db(ctx) {
            let fields = ctx.get_db_word_prefix_fids(word_prefix.interned())?;
            all_fields.extend(fields);
        }

        let weights_map = ctx.index.fieldids_weights_map(ctx.txn)?;

        let mut edges = vec![];
        for fid in all_fields.iter().copied() {
            let weight = weights_map
                .weight(fid)
                .ok_or(InternalError::FieldidsWeightsMapMissingEntry { key: fid })?;
            edges.push((
                weight as u32 * term.term_ids.len() as u32,
                conditions_interner.insert(FidCondition { term: term.clone(), fid: Some(fid) }),
            ));
        }

        // always lookup the max_fid if we don't already and add an artificial condition for max scoring
        let max_weight: Option<u16> = weights_map.max_weight();

        if let Some(max_weight) = max_weight {
            if !all_fields.contains(&max_weight) {
                edges.push((
                    max_weight as u32 * term.term_ids.len() as u32, // TODO improve the fid score i.e. fid^10.
                    conditions_interner.insert(FidCondition {
                        term: term.clone(), // TODO remove this ugly clone
                        fid: None,
                    }),
                ));
            }
        }

        Ok(edges)
    }

    fn rank_to_score(rank: Rank) -> ScoreDetails {
        ScoreDetails::Fid(rank)
    }
}

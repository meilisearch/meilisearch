use fxhash::FxHashSet;
use roaring::RoaringBitmap;

use super::{ComputedCondition, RankingRuleGraphTrait};
use crate::score_details::{Rank, ScoreDetails};
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids_within_field_id;
use crate::search::new::SearchContext;
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FidCondition {
    term: LocatedQueryTermSubset,
    fid: u16,
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
        // maybe compute_query_term_subset_docids_within_field_id should accept a universe as argument
        let mut docids = compute_query_term_subset_docids_within_field_id(
            ctx,
            &term.term_subset,
            condition.fid,
        )?;
        docids &= universe;

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

        let mut edges = vec![];
        for fid in all_fields.iter().copied() {
            edges.push((
                fid as u32 * term.term_ids.len() as u32,
                conditions_interner.insert(FidCondition { term: term.clone(), fid }),
            ));
        }

        // always lookup the max_fid if we don't already and add an artificial condition for max scoring
        let max_fid: Option<u16> = {
            if let Some(max_fid) = ctx
                .index
                .searchable_fields_ids(ctx.txn)?
                .map(|field_ids| field_ids.into_iter().max())
            {
                max_fid
            } else {
                ctx.index.fields_ids_map(ctx.txn)?.ids().max()
            }
        };

        if let Some(max_fid) = max_fid {
            if !all_fields.contains(&max_fid) {
                edges.push((
                    max_fid as u32 * term.term_ids.len() as u32, // TODO improve the fid score i.e. fid^10.
                    conditions_interner.insert(FidCondition {
                        term: term.clone(), // TODO remove this ugly clone
                        fid: max_fid,
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

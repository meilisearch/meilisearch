use fxhash::FxHashSet;
use roaring::RoaringBitmap;

use super::{ComputedCondition, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::resolve_query_graph::{
    compute_query_term_subset_docids, compute_query_term_subset_docids_within_field_id,
};
use crate::search::new::SearchContext;
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AttributeCondition {
    term: LocatedQueryTermSubset,
    fid: u16,
}

pub enum AttributeGraph {}

impl RankingRuleGraphTrait for AttributeGraph {
    type Condition = AttributeCondition;

    fn resolve_condition(
        ctx: &mut SearchContext,
        condition: &Self::Condition,
        universe: &RoaringBitmap,
    ) -> Result<ComputedCondition> {
        let AttributeCondition { term, .. } = condition;
        // maybe compute_query_term_subset_docids should accept a universe as argument
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
            let fields = ctx.get_db_word_fids(word)?;
            all_fields.extend(fields);
        }

        for phrase in term.term_subset.all_phrases(ctx)? {
            for &word in phrase.words(ctx).iter().flatten() {
                let fields = ctx.get_db_word_fids(word)?;
                all_fields.extend(fields);
            }
        }

        if let Some(word_prefix) = term.term_subset.use_prefix_db(ctx) {
            let fields = ctx.get_db_word_prefix_fids(word_prefix)?;
            all_fields.extend(fields);
        }

        let mut edges = vec![];
        for fid in all_fields {
            // TODO: We can improve performances and relevancy by storing
            //       the term subsets associated to each field ids fetched.
            edges.push((
                fid as u32 * term.term_ids.len() as u32, // TODO improve the fid score i.e. fid^10.
                conditions_interner.insert(AttributeCondition {
                    term: term.clone(), // TODO remove this ugly clone
                    fid,
                }),
            ));
        }

        Ok(edges)
    }
}

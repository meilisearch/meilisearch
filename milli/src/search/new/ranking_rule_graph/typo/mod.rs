use roaring::RoaringBitmap;

use super::{ComputedCondition, DeadEndsCache, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interned, MappedInterner};
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids;
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TypoCondition {
    term: LocatedQueryTermSubset,
    nbr_typos: u8,
}

pub enum TypoGraph {}

impl RankingRuleGraphTrait for TypoGraph {
    type Condition = TypoCondition;

    fn resolve_condition(
        ctx: &mut SearchContext,
        condition: &Self::Condition,
        universe: &RoaringBitmap,
    ) -> Result<ComputedCondition> {
        let TypoCondition { term, .. } = condition;
        // maybe compute_query_term_subset_docids should accept a universe as argument
        let mut docids = compute_query_term_subset_docids(ctx, &term.term_subset)?;
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

        let mut edges = vec![];
        // Ngrams have a base typo cost
        // 2-gram -> equivalent to 1 typo
        // 3-gram -> equivalent to 2 typos
        let base_cost = if term.term_ids.len() == 1 { 0 } else { term.term_ids.len() as u32 };

        for nbr_typos in 0..=term.term_subset.max_nbr_typos(ctx) {
            let mut term = term.clone();
            match nbr_typos {
                0 => {
                    term.term_subset.clear_one_typo_subset();
                    term.term_subset.clear_two_typo_subset();
                }
                1 => {
                    term.term_subset.clear_zero_typo_subset();
                    term.term_subset.clear_two_typo_subset();
                }
                2 => {
                    term.term_subset.clear_zero_typo_subset();
                    term.term_subset.clear_one_typo_subset();
                }
                _ => panic!(),
            };

            edges.push((
                nbr_typos as u32 + base_cost,
                conditions_interner.insert(TypoCondition { term, nbr_typos }),
            ));
        }
        Ok(edges)
    }

    fn log_state(
        graph: &RankingRuleGraph<Self>,
        paths: &[Vec<Interned<TypoCondition>>],
        dead_ends_cache: &DeadEndsCache<TypoCondition>,
        universe: &RoaringBitmap,
        distances: &MappedInterner<QueryNode, Vec<u64>>,
        cost: u64,
        logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        logger.log_typo_state(graph, paths, dead_ends_cache, universe, distances, cost);
    }

    fn label_for_condition(ctx: &mut SearchContext, condition: &Self::Condition) -> Result<String> {
        let TypoCondition { term, nbr_typos } = condition;
        Ok(format!("{}: {nbr_typos}", term.term_subset.description(ctx)))
    }
}

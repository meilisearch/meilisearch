// #[cfg(test)]
pub mod detailed;

use std::any::Any;

use roaring::RoaringBitmap;

use super::graph_based_ranking_rule::Typo;
use super::ranking_rules::BoxRankingRule;
use super::sort::Sort;
use super::words::Words;
use super::{RankingRule, RankingRuleQueryTrait};

/// Trait for structure logging the execution of a search query.
pub trait SearchLogger<Q: RankingRuleQueryTrait> {
    /// Logs the initial query
    fn initial_query(&mut self, query: &Q);

    /// Logs the value of the initial set of all candidates
    fn initial_universe(&mut self, universe: &RoaringBitmap);

    /// Logs the query that was used to compute the set of all candidates
    fn query_for_initial_universe(&mut self, query: &Q);

    /// Logs the ranking rules used to perform the search query
    fn ranking_rules(&mut self, rr: &[BoxRankingRule<Q>]);

    /// Logs the start of a ranking rule's iteration.
    fn start_iteration_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<Q>,
        query: &Q,
        universe: &RoaringBitmap,
    );
    /// Logs the end of the computation of a ranking rule bucket
    fn next_bucket_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<Q>,
        universe: &RoaringBitmap,
        candidates: &RoaringBitmap,
    );
    /// Logs the skipping of a ranking rule bucket
    fn skip_bucket_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<Q>,
        candidates: &RoaringBitmap,
    );
    /// Logs the end of a ranking rule's iteration.
    fn end_iteration_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<Q>,
        universe: &RoaringBitmap,
    );
    /// Logs the addition of document ids to the final results
    fn add_to_results(&mut self, docids: &[u32]);

    /// Logs the internal state of the ranking rule
    fn log_ranking_rule_state<'ctx>(&mut self, rr: &(dyn Any + 'ctx)) {
        if let Some(_words) = rr.downcast_ref::<Words>() {
        } else if let Some(_sort) = rr.downcast_ref::<Sort<'ctx, Q>>() {
        } else if let Some(_typo) = rr.downcast_ref::<Typo>() {
        }
    }
}

/// A dummy [`SearchLogger`] which does nothing.
pub struct DefaultSearchLogger;

impl<Q: RankingRuleQueryTrait> SearchLogger<Q> for DefaultSearchLogger {
    fn initial_query(&mut self, _query: &Q) {}

    fn query_for_initial_universe(&mut self, _query: &Q) {}

    fn initial_universe(&mut self, _universe: &RoaringBitmap) {}

    fn ranking_rules(&mut self, _rr: &[BoxRankingRule<Q>]) {}

    fn start_iteration_ranking_rule(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<Q>,
        _query: &Q,
        _universe: &RoaringBitmap,
    ) {
    }

    fn next_bucket_ranking_rule(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<Q>,
        _universe: &RoaringBitmap,
        _candidates: &RoaringBitmap,
    ) {
    }
    fn skip_bucket_ranking_rule(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<Q>,
        _candidates: &RoaringBitmap,
    ) {
    }

    fn end_iteration_ranking_rule(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<Q>,
        _universe: &RoaringBitmap,
    ) {
    }

    fn add_to_results(&mut self, _docids: &[u32]) {}
}

// #[cfg(test)]
pub mod visual;

use std::any::Any;

use roaring::RoaringBitmap;

use super::ranking_rules::BoxRankingRule;
use super::{RankingRule, RankingRuleQueryTrait};

/// Trait for structure logging the execution of a search query.
pub trait SearchLogger<Q: RankingRuleQueryTrait> {
    /// Logs the initial query
    fn initial_query(&mut self, _query: &Q);

    /// Logs the value of the initial set of all candidates
    fn initial_universe(&mut self, _universe: &RoaringBitmap);

    /// Logs the query that was used to compute the set of all candidates
    fn query_for_initial_universe(&mut self, _query: &Q);

    /// Logs the ranking rules used to perform the search query
    fn ranking_rules(&mut self, _rr: &[BoxRankingRule<Q>]);

    /// Logs the start of a ranking rule's iteration.
    fn start_iteration_ranking_rule(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<Q>,
        _query: &Q,
        _universe: &RoaringBitmap,
    ) {
    }
    /// Logs the end of the computation of a ranking rule bucket
    fn next_bucket_ranking_rule(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<Q>,
        _universe: &RoaringBitmap,
        _candidates: &RoaringBitmap,
    ) {
    }
    /// Logs the skipping of a ranking rule bucket
    fn skip_bucket_ranking_rule(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<Q>,
        _candidates: &RoaringBitmap,
    ) {
    }
    /// Logs the end of a ranking rule's iteration.
    fn end_iteration_ranking_rule(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<Q>,
        _universe: &RoaringBitmap,
    ) {
    }
    /// Logs the addition of document ids to the final results
    fn add_to_results(&mut self, _docids: &[u32]);

    /// Logs an internal state in the search algorithms
    fn log_internal_state(&mut self, _rr: &dyn Any);
}

/// A dummy [`SearchLogger`] which does nothing.
pub struct DefaultSearchLogger;

impl<Q: RankingRuleQueryTrait> SearchLogger<Q> for DefaultSearchLogger {
    fn initial_query(&mut self, _query: &Q) {}

    fn initial_universe(&mut self, _universe: &RoaringBitmap) {}

    fn query_for_initial_universe(&mut self, _query: &Q) {}

    fn ranking_rules(&mut self, _rr: &[BoxRankingRule<Q>]) {}

    fn add_to_results(&mut self, _docids: &[u32]) {}

    fn log_internal_state(&mut self, _rr: &dyn Any) {}
}

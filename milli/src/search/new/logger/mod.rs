// #[cfg(test)]
pub mod detailed;

use roaring::RoaringBitmap;

use super::interner::{Interned, MappedInterner};
use super::query_graph::QueryNode;
use super::ranking_rule_graph::{
    DeadEndsCache, ProximityCondition, ProximityGraph, RankingRuleGraph, TypoCondition, TypoGraph,
};
use super::{RankingRule, RankingRuleQueryTrait};

/// Trait for structure logging the execution of a search query.
pub trait SearchLogger<Q: RankingRuleQueryTrait> {
    /// Logs the initial query
    fn initial_query(&mut self, query: &Q);

    /// Logs the query that was used to compute the set of all candidates
    fn query_for_universe(&mut self, query: &Q);

    /// Logs the value of the initial set of all candidates
    fn initial_universe(&mut self, universe: &RoaringBitmap);

    /// Logs the ranking rules used to perform the search query
    fn ranking_rules(&mut self, rr: &[Box<dyn RankingRule<Q>>]);

    /// Logs the start of a ranking rule's iteration.
    fn start_iteration_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        query: &Q,
        universe: &RoaringBitmap,
    );
    /// Logs the end of the computation of a ranking rule bucket
    fn next_bucket_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        universe: &RoaringBitmap,
        candidates: &RoaringBitmap,
    );
    /// Logs the skipping of a ranking rule bucket
    fn skip_bucket_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        candidates: &RoaringBitmap,
    );
    /// Logs the end of a ranking rule's iteration.
    fn end_iteration_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        universe: &RoaringBitmap,
    );
    /// Logs the addition of document ids to the final results
    fn add_to_results(&mut self, docids: &[u32]);

    /// Logs the internal state of the words ranking rule
    fn log_words_state(&mut self, query_graph: &Q);

    /// Logs the internal state of the proximity ranking rule
    fn log_proximity_state(
        &mut self,
        query_graph: &RankingRuleGraph<ProximityGraph>,
        paths: &[Vec<Interned<ProximityCondition>>],
        dead_ends_cache: &DeadEndsCache<ProximityCondition>,
        universe: &RoaringBitmap,
        distances: &MappedInterner<Vec<u16>, QueryNode>,
        cost: u16,
    );

    /// Logs the internal state of the typo ranking rule
    fn log_typo_state(
        &mut self,
        query_graph: &RankingRuleGraph<TypoGraph>,
        paths: &[Vec<Interned<TypoCondition>>],
        dead_ends_cache: &DeadEndsCache<TypoCondition>,
        universe: &RoaringBitmap,
        distances: &MappedInterner<Vec<u16>, QueryNode>,
        cost: u16,
    );
}

/// A dummy [`SearchLogger`] which does nothing.
pub struct DefaultSearchLogger;

impl<Q: RankingRuleQueryTrait> SearchLogger<Q> for DefaultSearchLogger {
    fn initial_query(&mut self, _query: &Q) {}

    fn query_for_universe(&mut self, _query: &Q) {}

    fn initial_universe(&mut self, _universe: &RoaringBitmap) {}

    fn ranking_rules(&mut self, _rr: &[Box<dyn RankingRule<Q>>]) {}

    fn start_iteration_ranking_rule<'transaction>(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<'transaction, Q>,
        _query: &Q,
        _universe: &RoaringBitmap,
    ) {
    }

    fn next_bucket_ranking_rule<'transaction>(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<'transaction, Q>,
        _universe: &RoaringBitmap,
        _candidates: &RoaringBitmap,
    ) {
    }
    fn skip_bucket_ranking_rule<'transaction>(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<'transaction, Q>,
        _candidates: &RoaringBitmap,
    ) {
    }

    fn end_iteration_ranking_rule<'transaction>(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<'transaction, Q>,
        _universe: &RoaringBitmap,
    ) {
    }

    fn add_to_results(&mut self, _docids: &[u32]) {}

    fn log_words_state(&mut self, _query_graph: &Q) {}

    fn log_proximity_state(
        &mut self,
        _query_graph: &RankingRuleGraph<ProximityGraph>,
        _paths_map: &[Vec<Interned<ProximityCondition>>],
        _dead_ends_cache: &DeadEndsCache<ProximityCondition>,
        _universe: &RoaringBitmap,
        _distances: &MappedInterner<Vec<u16>, QueryNode>,
        _cost: u16,
    ) {
    }

    fn log_typo_state(
        &mut self,
        _query_graph: &RankingRuleGraph<TypoGraph>,
        _paths: &[Vec<Interned<TypoCondition>>],
        _dead_ends_cache: &DeadEndsCache<TypoCondition>,
        _universe: &RoaringBitmap,
        _distances: &MappedInterner<Vec<u16>, QueryNode>,
        _cost: u16,
    ) {
    }
}

#[cfg(test)]
pub mod detailed;

use roaring::RoaringBitmap;
use std::time::Instant;

use super::{
    ranking_rule_graph::{
        empty_paths_cache::EmptyPathsCache, proximity::ProximityGraph, typo::TypoGraph,
        RankingRuleGraph,
    },
    RankingRule, RankingRuleQueryTrait,
};

pub struct DefaultSearchLogger;
impl<Q: RankingRuleQueryTrait> SearchLogger<Q> for DefaultSearchLogger {
    fn initial_query(&mut self, _query: &Q, _time: Instant) {}

    fn initial_universe(&mut self, _universe: &RoaringBitmap) {}

    fn ranking_rules(&mut self, _rr: &[&mut dyn RankingRule<Q>]) {}

    fn start_iteration_ranking_rule<'transaction>(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<'transaction, Q>,
        _query: &Q,
        _universe: &RoaringBitmap,
        _time: Instant,
    ) {
    }

    fn next_bucket_ranking_rule<'transaction>(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<'transaction, Q>,
        _universe: &RoaringBitmap,
        _candidates: &RoaringBitmap,
        _time: Instant,
    ) {
    }
    fn skip_bucket_ranking_rule<'transaction>(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<'transaction, Q>,
        _candidates: &RoaringBitmap,
        _time: Instant,
    ) {
    }

    fn end_iteration_ranking_rule<'transaction>(
        &mut self,
        _ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<'transaction, Q>,
        _universe: &RoaringBitmap,
        _time: Instant,
    ) {
    }

    fn add_to_results(&mut self, _docids: &[u32]) {}

    fn log_words_state(&mut self, _query_graph: &Q) {}

    fn log_proximity_state(
        &mut self,
        _query_graph: &RankingRuleGraph<ProximityGraph>,
        _paths_map: &[Vec<u32>],
        _empty_paths_cache: &EmptyPathsCache,
        _universe: &RoaringBitmap,
        _distances: Vec<Vec<u64>>,
        _cost: u64,
    ) {
    }

    fn log_typo_state(
        &mut self,
        _query_graph: &RankingRuleGraph<TypoGraph>,
        _paths: &[Vec<u32>],
        _empty_paths_cache: &EmptyPathsCache,
        _universe: &RoaringBitmap,
        _distances: Vec<Vec<u64>>,
        _cost: u64,
    ) {
    }
}

pub trait SearchLogger<Q: RankingRuleQueryTrait> {
    fn initial_query(&mut self, query: &Q, time: Instant);
    fn initial_universe(&mut self, universe: &RoaringBitmap);

    fn ranking_rules(&mut self, rr: &[&mut dyn RankingRule<Q>]);

    fn start_iteration_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        query: &Q,
        universe: &RoaringBitmap,
        time: Instant,
    );
    fn next_bucket_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        universe: &RoaringBitmap,
        candidates: &RoaringBitmap,
        time: Instant,
    );
    fn skip_bucket_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        candidates: &RoaringBitmap,
        time: Instant,
    );
    fn end_iteration_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        universe: &RoaringBitmap,
        time: Instant,
    );
    fn add_to_results(&mut self, docids: &[u32]);

    fn log_words_state(&mut self, query_graph: &Q);

    fn log_proximity_state(
        &mut self,
        query_graph: &RankingRuleGraph<ProximityGraph>,
        paths: &[Vec<u32>],
        empty_paths_cache: &EmptyPathsCache,
        universe: &RoaringBitmap,
        _distances: Vec<Vec<u64>>,
        cost: u64,
    );

    fn log_typo_state(
        &mut self,
        query_graph: &RankingRuleGraph<TypoGraph>,
        paths: &[Vec<u32>],
        empty_paths_cache: &EmptyPathsCache,
        universe: &RoaringBitmap,
        _distances: Vec<Vec<u64>>,
        cost: u64,
    );
}

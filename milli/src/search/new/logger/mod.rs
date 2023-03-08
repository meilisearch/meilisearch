#[cfg(test)]
pub mod detailed;

use roaring::RoaringBitmap;

use super::{
    ranking_rule_graph::{EmptyPathsCache, ProximityGraph, RankingRuleGraph, TypoGraph},
    small_bitmap::SmallBitmap,
    RankingRule, RankingRuleQueryTrait,
};

pub struct DefaultSearchLogger;
impl<Q: RankingRuleQueryTrait> SearchLogger<Q> for DefaultSearchLogger {
    fn initial_query(&mut self, _query: &Q) {}

    fn query_for_universe(&mut self, _query: &Q) {}

    fn initial_universe(&mut self, _universe: &RoaringBitmap) {}

    fn ranking_rules(&mut self, _rr: &[&mut dyn RankingRule<Q>]) {}

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
        _paths_map: &[Vec<u16>],
        _empty_paths_cache: &EmptyPathsCache,
        _universe: &RoaringBitmap,
        _distances: Vec<Vec<(u16, SmallBitmap)>>,
        _cost: u16,
    ) {
    }

    fn log_typo_state(
        &mut self,
        _query_graph: &RankingRuleGraph<TypoGraph>,
        _paths: &[Vec<u16>],
        _empty_paths_cache: &EmptyPathsCache,
        _universe: &RoaringBitmap,
        _distances: Vec<Vec<(u16, SmallBitmap)>>,
        _cost: u16,
    ) {
    }
}

pub trait SearchLogger<Q: RankingRuleQueryTrait> {
    fn initial_query(&mut self, query: &Q);

    fn query_for_universe(&mut self, query: &Q);

    fn initial_universe(&mut self, universe: &RoaringBitmap);

    fn ranking_rules(&mut self, rr: &[&mut dyn RankingRule<Q>]);

    fn start_iteration_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        query: &Q,
        universe: &RoaringBitmap,
    );
    fn next_bucket_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        universe: &RoaringBitmap,
        candidates: &RoaringBitmap,
    );
    fn skip_bucket_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        candidates: &RoaringBitmap,
    );
    fn end_iteration_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, Q>,
        universe: &RoaringBitmap,
    );
    fn add_to_results(&mut self, docids: &[u32]);

    fn log_words_state(&mut self, query_graph: &Q);

    fn log_proximity_state(
        &mut self,
        query_graph: &RankingRuleGraph<ProximityGraph>,
        paths: &[Vec<u16>],
        empty_paths_cache: &EmptyPathsCache,
        universe: &RoaringBitmap,
        distances: Vec<Vec<(u16, SmallBitmap)>>,
        cost: u16,
    );

    fn log_typo_state(
        &mut self,
        query_graph: &RankingRuleGraph<TypoGraph>,
        paths: &[Vec<u16>],
        empty_paths_cache: &EmptyPathsCache,
        universe: &RoaringBitmap,
        distances: Vec<Vec<(u16, SmallBitmap)>>,
        cost: u16,
    );
}

#[cfg(test)]
pub mod detailed;

use roaring::RoaringBitmap;

use super::{
    ranking_rule_graph::{
        empty_paths_cache::EmptyPathsCache, paths_map::PathsMap, proximity::ProximityGraph,
        RankingRuleGraph,
    },
    RankingRule, RankingRuleQueryTrait,
};

pub struct DefaultSearchLogger;
impl<Q: RankingRuleQueryTrait> SearchLogger<Q> for DefaultSearchLogger {
    fn initial_query(&mut self, _query: &Q) {}

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
        _paths_map: &PathsMap<u64>,
        _empty_paths_cache: &EmptyPathsCache,
    ) {
    }
}

pub trait SearchLogger<Q: RankingRuleQueryTrait> {
    fn initial_query(&mut self, query: &Q);
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
        paths: &PathsMap<u64>,
        empty_paths_cache: &EmptyPathsCache,
    );
}

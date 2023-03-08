use roaring::RoaringBitmap;

use super::logger::SearchLogger;
use super::{QueryGraph, SearchContext};
use crate::search::new::graph_based_ranking_rule::GraphBasedRankingRule;
use crate::search::new::ranking_rule_graph::{ProximityGraph, TypoGraph};
use crate::search::new::words::Words;
// use crate::search::new::sort::Sort;
use crate::{Result, TermsMatchingStrategy};

pub trait RankingRuleOutputIter<'search, Query> {
    fn next_bucket(&mut self) -> Result<Option<RankingRuleOutput<Query>>>;
}

pub struct RankingRuleOutputIterWrapper<'search, Query> {
    iter: Box<dyn Iterator<Item = Result<RankingRuleOutput<Query>>> + 'search>,
}
impl<'search, Query> RankingRuleOutputIterWrapper<'search, Query> {
    pub fn new(iter: Box<dyn Iterator<Item = Result<RankingRuleOutput<Query>>> + 'search>) -> Self {
        Self { iter }
    }
}
impl<'search, Query> RankingRuleOutputIter<'search, Query>
    for RankingRuleOutputIterWrapper<'search, Query>
{
    fn next_bucket(&mut self) -> Result<Option<RankingRuleOutput<Query>>> {
        match self.iter.next() {
            Some(x) => x.map(Some),
            None => Ok(None),
        }
    }
}

pub trait RankingRuleQueryTrait: Sized + Clone + 'static {}

#[derive(Clone)]
pub struct PlaceholderQuery;
impl RankingRuleQueryTrait for PlaceholderQuery {}
impl RankingRuleQueryTrait for QueryGraph {}

pub trait RankingRule<'search, Query: RankingRuleQueryTrait> {
    fn id(&self) -> String;

    /// Prepare the ranking rule such that it can start iterating over its
    /// buckets using [`next_bucket`](RankingRule::next_bucket).
    ///
    /// The given universe is the universe that will be given to [`next_bucket`](RankingRule::next_bucket).
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'search>,
        logger: &mut dyn SearchLogger<Query>,
        universe: &RoaringBitmap,
        query: &Query,
    ) -> Result<()>;

    /// Return the next bucket of this ranking rule.
    ///
    /// The returned candidates MUST be a subset of the given universe.
    ///
    /// The universe given as argument is either:
    /// - a subset of the universe given to the previous call to [`next_bucket`](RankingRule::next_bucket); OR
    /// - the universe given to [`start_iteration`](RankingRule::start_iteration)
    fn next_bucket(
        &mut self,
        ctx: &mut SearchContext<'search>,
        logger: &mut dyn SearchLogger<Query>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Query>>>;

    /// Finish iterating over the buckets, which yields control to the parent ranking rule
    /// The next call to this ranking rule, if any, will be [`start_iteration`](RankingRule::start_iteration).
    fn end_iteration(
        &mut self,
        ctx: &mut SearchContext<'search>,
        logger: &mut dyn SearchLogger<Query>,
    );
}

#[derive(Debug)]
pub struct RankingRuleOutput<Q> {
    /// The query corresponding to the current bucket for the child ranking rule
    pub query: Q,
    /// The allowed candidates for the child ranking rule
    pub candidates: RoaringBitmap,
}

// TODO: can make it generic over the query type (either query graph or placeholder) fairly easily
#[allow(clippy::too_many_arguments)]
pub fn apply_ranking_rules<'search>(
    ctx: &mut SearchContext<'search>,
    // TODO: ranking rules parameter
    query_graph: &QueryGraph,
    universe: &RoaringBitmap,
    from: usize,
    length: usize,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<Vec<u32>> {
    logger.initial_query(query_graph);
    let words = &mut Words::new(TermsMatchingStrategy::Last);
    // let sort = &mut Sort::new(index, txn, "release_date".to_owned(), true)?;
    let proximity = &mut GraphBasedRankingRule::<ProximityGraph>::new("proximity".to_owned());
    let typo = &mut GraphBasedRankingRule::<TypoGraph>::new("typo".to_owned());
    // TODO: ranking rules given as argument
    let mut ranking_rules: Vec<&mut dyn RankingRule<'search, QueryGraph>> =
        vec![words, typo, proximity /*sort*/];

    logger.ranking_rules(&ranking_rules);

    if universe.len() < from as u64 {
        return Ok(vec![]);
    }

    let ranking_rules_len = ranking_rules.len();
    logger.start_iteration_ranking_rule(0, ranking_rules[0], query_graph, universe);
    ranking_rules[0].start_iteration(ctx, logger, universe, query_graph)?;

    let mut candidates: Vec<RoaringBitmap> = vec![RoaringBitmap::default(); ranking_rules_len];
    candidates[0] = universe.clone();

    let mut cur_ranking_rule_index = 0;

    macro_rules! back {
        () => {
            assert!(candidates[cur_ranking_rule_index].is_empty());
            logger.end_iteration_ranking_rule(
                cur_ranking_rule_index,
                ranking_rules[cur_ranking_rule_index],
                &candidates[cur_ranking_rule_index],
            );
            candidates[cur_ranking_rule_index].clear();
            ranking_rules[cur_ranking_rule_index].end_iteration(ctx, logger);
            if cur_ranking_rule_index == 0 {
                break;
            } else {
                cur_ranking_rule_index -= 1;
            }
        };
    }

    let mut results = vec![];
    let mut cur_offset = 0usize;

    // Add the candidates to the results. Take the `from`, `limit`, and `cur_offset`
    // into account and inform the logger.
    macro_rules! maybe_add_to_results {
        ($candidates:expr) => {
            let candidates = $candidates;
            let len = candidates.len();
            // if the candidates are empty, there is nothing to do;
            if !candidates.is_empty() {
                if cur_offset < from {
                    if cur_offset + (candidates.len() as usize) < from {
                        logger.skip_bucket_ranking_rule(
                            cur_ranking_rule_index,
                            ranking_rules[cur_ranking_rule_index],
                            &candidates,
                        );
                    } else {
                        let all_candidates = candidates.iter().collect::<Vec<_>>();
                        let (skipped_candidates, candidates) =
                            all_candidates.split_at(from - cur_offset);
                        logger.skip_bucket_ranking_rule(
                            cur_ranking_rule_index,
                            ranking_rules[cur_ranking_rule_index],
                            &skipped_candidates.into_iter().collect(),
                        );
                        let candidates = candidates
                            .iter()
                            .take(length - results.len())
                            .copied()
                            .collect::<Vec<_>>();
                        logger.add_to_results(&candidates);
                        results.extend(&candidates);
                    }
                } else {
                    let candidates =
                        candidates.iter().take(length - results.len()).collect::<Vec<u32>>();
                    logger.add_to_results(&candidates);
                    results.extend(&candidates);
                }
            }
            cur_offset += len as usize;
        };
    }
    while results.len() < length {
        // The universe for this bucket is zero or one element, so we don't need to sort
        // anything, just extend the results and go back to the parent ranking rule.
        if candidates[cur_ranking_rule_index].len() <= 1 {
            maybe_add_to_results!(&candidates[cur_ranking_rule_index]);
            candidates[cur_ranking_rule_index].clear();
            back!();
            continue;
        }

        let Some(next_bucket) = ranking_rules[cur_ranking_rule_index].next_bucket(ctx, logger, &candidates[cur_ranking_rule_index])? else {
            // TODO: add remaining candidates automatically here?
            back!();
            continue;
        };

        logger.next_bucket_ranking_rule(
            cur_ranking_rule_index,
            ranking_rules[cur_ranking_rule_index],
            &candidates[cur_ranking_rule_index],
            &next_bucket.candidates,
        );

        assert!(candidates[cur_ranking_rule_index].is_superset(&next_bucket.candidates));
        candidates[cur_ranking_rule_index] -= &next_bucket.candidates;

        if cur_ranking_rule_index == ranking_rules_len - 1
            || next_bucket.candidates.len() <= 1
            || cur_offset + (next_bucket.candidates.len() as usize) < from
        {
            maybe_add_to_results!(&next_bucket.candidates);
            continue;
        }

        cur_ranking_rule_index += 1;
        candidates[cur_ranking_rule_index] = next_bucket.candidates.clone();
        logger.start_iteration_ranking_rule(
            cur_ranking_rule_index,
            ranking_rules[cur_ranking_rule_index],
            &next_bucket.query,
            &candidates[cur_ranking_rule_index],
        );
        ranking_rules[cur_ranking_rule_index].start_iteration(
            ctx,
            logger,
            &next_bucket.candidates,
            &next_bucket.query,
        )?;
    }

    Ok(results)
}

use roaring::RoaringBitmap;

use super::logger::SearchLogger;
use super::{QueryGraph, SearchContext};
use crate::score_details::ScoreDetails;
use crate::Result;

/// An internal trait implemented by only [`PlaceholderQuery`] and [`QueryGraph`]
pub trait RankingRuleQueryTrait: Sized + Clone + 'static {}

/// A type describing a placeholder search
#[derive(Clone)]
pub struct PlaceholderQuery;
impl RankingRuleQueryTrait for PlaceholderQuery {}
impl RankingRuleQueryTrait for QueryGraph {}

pub type BoxRankingRule<'ctx, Query> = Box<dyn RankingRule<'ctx, Query> + 'ctx>;

/// A trait that must be implemented by all ranking rules.
///
/// It is generic over `'ctx`, the lifetime of the search context
/// (i.e. the read transaction and the cache) and over `Query`, which
/// can be either [`PlaceholderQuery`] or [`QueryGraph`].
pub trait RankingRule<'ctx, Query: RankingRuleQueryTrait> {
    fn id(&self) -> String;

    /// Prepare the ranking rule such that it can start iterating over its
    /// buckets using [`next_bucket`](RankingRule::next_bucket).
    ///
    /// The given universe is the universe that will be given to [`next_bucket`](RankingRule::next_bucket).
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
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
        ctx: &mut SearchContext<'ctx>,
        logger: &mut dyn SearchLogger<Query>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Query>>>;

    /// Finish iterating over the buckets, which yields control to the parent ranking rule
    /// The next call to this ranking rule, if any, will be [`start_iteration`](RankingRule::start_iteration).
    fn end_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        logger: &mut dyn SearchLogger<Query>,
    );
}

/// Output of a ranking rule, consisting of the query to be used
/// by the child ranking rule and a set of document ids.
#[derive(Debug)]
pub struct RankingRuleOutput<Q> {
    /// The query corresponding to the current bucket for the child ranking rule
    pub query: Q,
    /// The allowed candidates for the child ranking rule
    pub candidates: RoaringBitmap,
    /// The score for the candidates of the current bucket
    pub score: ScoreDetails,
}

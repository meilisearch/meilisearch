use roaring::RoaringBitmap;

use super::logger::SearchLogger;
use super::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait, SearchContext};
use crate::score_details::{self, ScoreDetails};
use crate::{Filter, Result};

pub struct FilterBoosting<'f, Query> {
    filter: Filter<'f>,
    original_query: Option<Query>,
    matching: Option<RankingRuleOutput<Query>>,
    non_matching: Option<RankingRuleOutput<Query>>,
}

impl<'f, Query> FilterBoosting<'f, Query> {
    pub fn new(filter: Filter<'f>) -> Result<Self> {
        Ok(Self { filter, original_query: None, matching: None, non_matching: None })
    }
}

impl<'ctx, 'f, Query: RankingRuleQueryTrait> RankingRule<'ctx, Query>
    for FilterBoosting<'f, Query>
{
    fn id(&self) -> String {
        // TODO improve this
        let Self { filter: original_expression, .. } = self;
        format!("boost:{original_expression:?}")
    }

    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Query>,
        parent_candidates: &RoaringBitmap,
        parent_query: &Query,
    ) -> Result<()> {
        let universe_matching = match self.filter.evaluate(ctx.txn, ctx.index) {
            Ok(documents) => documents,
            Err(e) => return Err(e), // TODO manage the invalid_search_boosting_filter
        };
        let matching = parent_candidates & universe_matching;
        let non_matching = parent_candidates - &matching;

        self.original_query = Some(parent_query.clone());

        self.matching = Some(RankingRuleOutput {
            query: parent_query.clone(),
            candidates: matching,
            score: ScoreDetails::FilterBoosting(score_details::FilterBoosting { matching: true }),
        });

        self.non_matching = Some(RankingRuleOutput {
            query: parent_query.clone(),
            candidates: non_matching,
            score: ScoreDetails::FilterBoosting(score_details::FilterBoosting { matching: false }),
        });

        Ok(())
    }

    fn next_bucket(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Query>,
        _universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Query>>> {
        Ok(self.matching.take().or_else(|| self.non_matching.take()))
    }

    fn end_iteration(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Query>,
    ) {
        self.original_query = None;
        self.matching = None;
        self.non_matching = None;
    }
}

use filter_parser::FilterConstraints;
use time::OffsetDateTime;

use crate::Result;

use super::{
    ConditionOutcomes, FilterConditionOutcome, QueryEmptyConditionOutcome,
    QueryWordsConditionOutcome, RulePreview, TimeConditionOutcome,
};

impl RulePreview {
    /// Determine if the preview rule applies according to its conditions and the passed query.
    ///
    /// Returns detailed information about whether which conditions were met or not.
    pub fn evaluate_conditions(
        &self,
        _query_terms: &[&str],
        _filter_constraints: &FilterConstraints,
        _target_time: OffsetDateTime,
    ) -> Result<ConditionOutcomes> {
        Ok(ConditionOutcomes {
            satisfies_active_condition: false,
            satisfies_time_condition: TimeConditionOutcome::NoConstraint,
            satisfies_query_empty_condition: QueryEmptyConditionOutcome::NoConstraint,
            satisfies_query_words_condition: QueryWordsConditionOutcome::NoConstraint,
            satisfies_filter_condition: FilterConditionOutcome::NoConstraint,
        })
    }
}

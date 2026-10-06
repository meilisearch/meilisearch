use std::collections::BTreeMap;

use time::OffsetDateTime;

use crate::{
    dynamic_search_rules::{RuleActions, RuleId},
    Precedence,
};

#[cfg(not(feature = "enterprise"))]
mod community_edition;
#[cfg(feature = "enterprise")]
mod enterprise_edition;

/// Preview of a DSR.
pub struct RulePreview {
    /// UID of the DSR
    pub uid: String,
    /// Whether the DSR is active.
    ///
    /// Inactive DSRs will never be applied.
    pub active: bool,
    /// Precedence of the DSR:
    ///
    /// - if multiple rules are active, the actions of the rule with the lowest precedence will be examined first
    /// - if multiple pinning actions are applicable to the same position,
    ///   the action of the rule with the lowest precedence will be examined first
    pub precedence: Precedence,
    /// Conditions on the query for the rule to apply
    pub conditions: PreviewConditions,
    /// Actions to execute if the rule applies
    pub actions: RuleActions,
}

pub(super) struct RulePreviewWithId<'a> {
    pub preview: &'a RulePreview,
    pub preview_id: RuleId,
}

/// Outcome of applying conditions to a query.
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ConditionOutcomes {
    /// Whether the rule was active
    pub satisfies_active_condition: bool,
    /// Whether the current time falls in the range of time specified by the condition
    #[serde(default, skip_serializing_if = "TimeConditionOutcome::no_constraint")]
    pub satisfies_time_condition: TimeConditionOutcome,
    /// Whether the query emptiness condition aligns with the actual query
    #[serde(default, skip_serializing_if = "QueryEmptyConditionOutcome::no_constraint")]
    pub satisfies_query_empty_condition: QueryEmptyConditionOutcome,
    /// Whether the query words meet the condition
    #[serde(default, skip_serializing_if = "QueryWordsConditionOutcome::no_constraint")]
    pub satisfies_query_words_condition: QueryWordsConditionOutcome,
    /// Whether the filter meets the condition
    #[serde(default, skip_serializing_if = "FilterConditionOutcome::no_constraint")]
    pub satisfies_filter_condition: FilterConditionOutcome,
}

/// Outcome of the time condition
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
    Default,
)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum TimeConditionOutcome {
    /// Current time falls in range
    Satisfied,
    #[default]
    /// No time condition
    NoConstraint,
    /// Current time is before the beginning of the range
    TooEarly,
    /// Current time is after the end of the range
    TooLate,
}

impl TimeConditionOutcome {
    /// `true` if there is no time condition, or if it is met
    pub fn is_enabled(&self) -> bool {
        match self {
            TimeConditionOutcome::Satisfied | TimeConditionOutcome::NoConstraint => true,
            TimeConditionOutcome::TooEarly | TimeConditionOutcome::TooLate => false,
        }
    }

    /// `true` if there is no such condition
    pub fn no_constraint(&self) -> bool {
        matches!(self, Self::NoConstraint)
    }
}

/// Outcome of the query empty condition
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
    Default,
)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum QueryEmptyConditionOutcome {
    /// Query emptiness agrees with the condition
    Satisfied,
    #[default]
    /// No query empty condition
    NoConstraint,
    /// The query is not empty, but the rule requires an empty query
    QueryNotEmpty,
    /// The query is empty, but the rule requires a non-empty query
    QueryEmpty,
}

impl QueryEmptyConditionOutcome {
    /// `true` if there is no query empty condition or if it is met
    pub fn is_enabled(&self) -> bool {
        match self {
            QueryEmptyConditionOutcome::Satisfied | QueryEmptyConditionOutcome::NoConstraint => {
                true
            }
            QueryEmptyConditionOutcome::QueryEmpty | QueryEmptyConditionOutcome::QueryNotEmpty => {
                false
            }
        }
    }

    /// `true` if there is no such condition
    pub fn no_constraint(&self) -> bool {
        matches!(self, Self::NoConstraint)
    }
}

/// Outcome of the query words condition
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema, Default,
)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum QueryWordsConditionOutcome {
    /// The query contains all the words of the condition
    Satisfied,
    #[default]
    /// No query words condition
    NoConstraint,
    /// At least the specified word is missing in the query
    MissingWord {
        /// Missing word in query
        word: String,
    },
}

impl QueryWordsConditionOutcome {
    /// `true` if there is no query words condition or if it is met
    pub fn is_enabled(&self) -> bool {
        match self {
            QueryWordsConditionOutcome::Satisfied | QueryWordsConditionOutcome::NoConstraint => {
                true
            }
            QueryWordsConditionOutcome::MissingWord { word: _ } => false,
        }
    }

    /// `true` if there is no such condition
    pub fn no_constraint(&self) -> bool {
        matches!(self, Self::NoConstraint)
    }
}

/// Outcome of the filter condition
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize, utoipa::ToSchema, Default,
)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum FilterConditionOutcome {
    /// The filter satifies all constraints in the condition
    Satisfied,
    #[default]
    /// No filter condition
    NoConstraint,
    /// At least one field from the condition is missing from the filter
    MissingConstraintOnField {
        /// Missing field in filter
        field: String,
    },
    /// At least one field has a condition not met in the filter
    UnmetConstraintOnField {
        /// Field with unmet constraint in filter
        field: String,
    },
    /// The filter does not constraint enough field to satisfy the condition
    NotEnoughFields {
        /// Number of fields in the filter
        field_count_in_filter: usize,
        /// Number of fields in the condition
        field_count_in_condition: usize,
    },
}

impl FilterConditionOutcome {
    /// `true` if there is no condition or if the condition is met
    pub fn is_enabled(&self) -> bool {
        match self {
            FilterConditionOutcome::Satisfied | FilterConditionOutcome::NoConstraint => true,
            FilterConditionOutcome::MissingConstraintOnField { field: _ }
            | FilterConditionOutcome::UnmetConstraintOnField { field: _ }
            | Self::NotEnoughFields { field_count_in_filter: _, field_count_in_condition: _ } => {
                false
            }
        }
    }

    /// `true` if there is no such condition
    pub fn no_constraint(&self) -> bool {
        matches!(self, Self::NoConstraint)
    }
}

impl ConditionOutcomes {
    /// `true` if all conditions are enabled
    pub fn is_enabled(&self) -> bool {
        self.satisfies_active_condition
            && self.satisfies_time_condition.is_enabled()
            && self.satisfies_query_empty_condition.is_enabled()
            && self.satisfies_query_words_condition.is_enabled()
            && self.satisfies_filter_condition.is_enabled()
    }
}

/// Conditions for a preview rule
///
/// They should mirror the conditions of an actual rule
// This type is redefined due to complexity around Deserr
pub struct PreviewConditions {
    /// Time condition, if any
    pub time: Option<TimeCondition>,
    /// Query condition, if any
    pub query: Option<QueryCondition>,
    /// Filter condition, if any
    pub filter: Option<FilterCondition>,
}

#[derive(Clone, Copy)]
/// Duplicate implementation of the equivalent meilisearch-types struct due to deserr shenanigans
pub struct TimeCondition {
    /// Beginning of the range
    pub start: Option<OffsetDateTime>,
    /// End of the range
    pub end: Option<OffsetDateTime>,
}

/// Duplicate implementation of the equivalent meilisearch-types struct due to deserr shenanigans
pub struct QueryCondition {
    /// Whether the query should be empty
    pub is_empty: Option<bool>,
    /// Words that the query must contain
    pub words: Option<String>,
}
/// Duplicate implementation of the equivalent meilisearch-types struct due to deserr shenanigans
pub struct FilterCondition {
    /// Constrained values
    pub values: BTreeMap<String, serde_json::Value>,
}

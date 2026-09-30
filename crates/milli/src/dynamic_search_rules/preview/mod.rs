use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
};

use charabia::TokenKind;
use filter_parser::{ConstraintCondition, ConstraintConditionKind, FilterConstraints};
use time::OffsetDateTime;

use crate::{
    dynamic_search_rules::{RuleActions, RuleId},
    search::facet::value_bounds::{to_str_bounds, ValueBounds},
    Precedence, Result,
};

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

impl RulePreview {
    /// Determine if the preview rule applies according to its conditions and the passed query.
    ///
    /// Returns detailed information about whether which conditions were met or not.
    pub fn evaluate_conditions(
        &self,
        query_terms: &[&str],
        filter_constraints: &FilterConstraints,
        target_time: OffsetDateTime,
    ) -> Result<ConditionOutcomes> {
        let satisfies_active_condition = self.active;
        let satisfies_time_condition = self.apply_time_conditions(target_time);
        let (satisfies_query_empty_condition, satisfies_query_words_condition) =
            self.apply_query_conditions(query_terms)?;
        let satisfies_filter_condition = self.apply_filter_conditions(filter_constraints);

        Ok(ConditionOutcomes {
            satisfies_active_condition,
            satisfies_time_condition,
            satisfies_query_empty_condition,
            satisfies_query_words_condition,
            satisfies_filter_condition,
        })
    }

    fn apply_time_conditions(&self, target_time: OffsetDateTime) -> TimeConditionOutcome {
        let Some(time_condition) = self.conditions.time else {
            return TimeConditionOutcome::NoConstraint;
        };

        if let Some(start) = time_condition.start {
            if target_time < start {
                return TimeConditionOutcome::TooEarly;
            }
        }

        if let Some(end) = time_condition.end {
            if target_time > end {
                return TimeConditionOutcome::TooLate;
            }
        }
        TimeConditionOutcome::Satisfied
    }

    fn apply_query_conditions(
        &self,
        query_terms: &[&str],
    ) -> Result<(QueryEmptyConditionOutcome, QueryWordsConditionOutcome)> {
        let Some(query) = &self.conditions.query else {
            return Ok((
                QueryEmptyConditionOutcome::NoConstraint,
                QueryWordsConditionOutcome::NoConstraint,
            ));
        };

        let mut empty_condition = QueryEmptyConditionOutcome::NoConstraint;
        let mut words_condition = QueryWordsConditionOutcome::NoConstraint;

        if let Some(constraint_is_empty) = query.is_empty {
            empty_condition = match (constraint_is_empty, query_terms.is_empty()) {
                (true, false) => QueryEmptyConditionOutcome::QueryNotEmpty,
                (false, true) => QueryEmptyConditionOutcome::QueryEmpty,
                (_, _) => QueryEmptyConditionOutcome::Satisfied,
            };
        }

        if let Some(words) = query.words.as_deref() {
            words_condition = QueryWordsConditionOutcome::Satisfied;

            let mut builder = crate::update::new::tokenizer_builder(None, None, None);
            let tokenizer = builder.build();
            for token in tokenizer.tokenize_with_allow_list(words, None) {
                if !matches!(token.kind(), TokenKind::Word) {
                    continue;
                }

                let lemma = token.lemma().trim();
                if lemma.is_empty() {
                    continue;
                }

                if query_terms.binary_search(&lemma).is_err() {
                    words_condition =
                        QueryWordsConditionOutcome::MissingWord { word: lemma.to_owned() };
                    return Ok((empty_condition, words_condition));
                }
            }
        }

        Ok((empty_condition, words_condition))
    }

    fn apply_filter_conditions(
        &self,
        filter_constraints: &FilterConstraints,
    ) -> FilterConditionOutcome {
        let Some(filter_condition) = self.conditions.filter.as_ref() else {
            return FilterConditionOutcome::NoConstraint;
        };

        let nb_constraints = filter_condition.values.len();
        let max_nb_constraints = filter_constraints.max_number_of_constraints();
        if nb_constraints > max_nb_constraints {
            return FilterConditionOutcome::NotEnoughFields {
                field_count_in_filter: max_nb_constraints,
                field_count_in_condition: nb_constraints,
            };
        }

        let mut field_values = BTreeMap::new();
        let mut field_name = String::new();
        for (name, value) in &filter_condition.values {
            field_name.clear();
            field_name.push_str(name);
            find_field_values(&mut field_name, &mut field_values, value);
        }

        let mut filter_condition = FilterConditionOutcome::Satisfied;

        'or_groups: for constraints in &filter_constraints.constraints {
            'field: for (field, values) in &field_values {
                let Some(conditions) = constraints.get(field.as_str()) else {
                    // we have a constraint for this rule that doesn't appear in this OR group, ...
                    // try next OR group
                    filter_condition =
                        FilterConditionOutcome::MissingConstraintOnField { field: field.clone() };
                    continue 'or_groups;
                };

                for value in values {
                    if resolve_constraints(conditions, value) {
                        // we found a permissible value for this field
                        continue 'field;
                    }
                }
                // we tried all permissible values for this constraint but none worked
                // let's try the next OR group
                filter_condition =
                    FilterConditionOutcome::UnmetConstraintOnField { field: field.clone() };
                continue 'or_groups;
            }
            // we found a permissible value for all fields
            return FilterConditionOutcome::Satisfied;
        }

        // we tried all OR groups but none found a permissible value for all fields
        filter_condition
    }
}

fn find_field_values(
    field_name: &mut String,
    field_values: &mut BTreeMap<String, Vec<either::Either<String, f64>>>,
    value: &serde_json::Value,
) {
    match value {
        serde_json::Value::Null => (),
        serde_json::Value::Bool(value) => {
            field_values
                .entry(field_name.clone())
                .or_default()
                .push(either::Left(format!("{value}")));
        }
        serde_json::Value::Number(value) => {
            let Some(value) = value.as_f64() else {
                return;
            };
            field_values.entry(field_name.clone()).or_default().push(either::Right(value));
        }
        serde_json::Value::String(value) => {
            let normalized = crate::normalize_facet(value);
            field_values.entry(field_name.clone()).or_default().push(either::Left(normalized));
        }
        serde_json::Value::Array(values) => {
            for value in values {
                find_field_values(field_name, field_values, value);
            }
        }
        serde_json::Value::Object(map) => {
            for (name, value) in map {
                let previous_size = field_name.len();
                field_name.push('.');
                field_name.push_str(name);
                find_field_values(field_name, field_values, value);
                field_name.truncate(previous_size);
            }
        }
    }
}

/// Outcome of applying conditions to a query.
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ConditionOutcomes {
    /// Whether the rule was active
    pub satisfies_active_condition: bool,
    /// Whether the current time falls in the range of time specified by the condition
    #[serde(skip_serializing_if = "TimeConditionOutcome::no_constraint")]
    pub satisfies_time_condition: TimeConditionOutcome,
    /// Whether the query emptiness condition aligns with the actual query
    #[serde(skip_serializing_if = "QueryEmptyConditionOutcome::no_constraint")]
    pub satisfies_query_empty_condition: QueryEmptyConditionOutcome,
    /// Whether the query words meet the condition
    #[serde(skip_serializing_if = "QueryWordsConditionOutcome::no_constraint")]
    pub satisfies_query_words_condition: QueryWordsConditionOutcome,
    /// Whether the filter meets the condition
    #[serde(skip_serializing_if = "FilterConditionOutcome::no_constraint")]
    pub satisfies_filter_condition: FilterConditionOutcome,
}

/// Outcome of the time condition
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum TimeConditionOutcome {
    /// Current time falls in range
    Satisfied,
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
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum QueryEmptyConditionOutcome {
    /// Query emptiness agrees with the condition
    Satisfied,
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum QueryWordsConditionOutcome {
    /// The query contains all the words of the condition
    Satisfied,
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum FilterConditionOutcome {
    /// The filter satifies all constraints in the condition
    Satisfied,
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

fn resolve_constraints(
    constraints: &[ConstraintCondition],
    value: &either::Either<String, f64>,
) -> bool {
    for constraint in constraints {
        let mut polarity = constraint.polarity;
        let evaluated = match &constraint.kind {
            ConstraintConditionKind::Condition { condition } => {
                match ValueBounds::new(condition) {
                    ValueBounds::Range { normalized, number } => match (value, number) {
                        (either::Either::Left(str), _) => {
                            <(Bound<&str>, Bound<&str>) as RangeBounds<str>>::contains(
                                &to_str_bounds(&normalized),
                                str.as_str(),
                            )
                        }
                        (either::Either::Right(number), Some(number_range)) => {
                            number_range.contains(number)
                        }
                        _ => false,
                    },
                    // no effect if polarity = false, removes everything otherwise
                    ValueBounds::FieldIsEmpty | ValueBounds::FieldIsNull => false,
                    // no effect if polarity = true, removes everything otherwise
                    ValueBounds::FieldExists => true,
                    ValueBounds::Equal { normalized, number } => {
                        evaluate_equal(value, normalized, number)
                    }
                    ValueBounds::NotEqual { normalized, number } => {
                        polarity = !polarity;
                        evaluate_equal(value, normalized, number)
                    }
                    ValueBounds::Contains { normalized: _ }
                    | ValueBounds::StartsWith { normalized: _ } => return false,
                }
            }
            // always unsupported, considered unsatisfiable
            ConstraintConditionKind::VectorExists { .. }
            | ConstraintConditionKind::GeoLowerThan { .. }
            | ConstraintConditionKind::GeoBoundingBox { .. }
            | ConstraintConditionKind::GeoPolygon { .. } => return false,
        };
        if polarity {
            // exclude rules that were evaluated to 0
            if !evaluated {
                return false;
            }
        } else {
            if evaluated {
                return false;
            }
        }
    }
    true
}

fn evaluate_equal(
    value: &either::Either<String, f64>,
    normalized: String,
    number: Option<f64>,
) -> bool {
    match (value, number) {
        (either::Either::Left(str), _) => str == normalized.as_str(),
        (either::Either::Right(left), Some(right)) => *left == right,
        _ => false,
    }
}

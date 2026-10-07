// Copyright © 2026 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
};

use charabia::TokenKind;
use filter_parser::{ConstraintCondition, ConstraintConditionKind, FilterConstraints};
use time::OffsetDateTime;

use super::{
    ConditionOutcomes, FilterConditionOutcome, QueryEmptyConditionOutcome,
    QueryWordsConditionOutcome, RulePreview, TimeConditionOutcome,
};

use crate::{
    dynamic_search_rules::{DynamicSearchRulesView, RuleId},
    search::facet::value_bounds::{to_str_bounds, ValueBounds},
    update::AvailableIds,
    Result,
};

impl RulePreview {
    pub(in crate::dynamic_search_rules) fn find_id(
        &self,
        dsrs: Option<DynamicSearchRulesView>,
    ) -> Result<Option<RuleId>> {
        let Some(dsrs) = dsrs else { return Ok(Some(0)) };

        match dsrs.get_id(&self.uid)? {
            Some(existing_id) => Ok(Some(existing_id)),
            None => {
                let existing_rules = dsrs.index.documents_ids(dsrs.rtxn)?;
                let mut available_ids = AvailableIds::new(&existing_rules);

                if let Some(id) = available_ids.next() {
                    Ok(Some(id))
                } else {
                    tracing::warn!(
                        "Ignoring preview rule because {} rules are already defined",
                        u32::MAX
                    );
                    Ok(None)
                }
            }
        }
    }

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

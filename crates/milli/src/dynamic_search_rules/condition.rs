use std::ops::{Bound, ControlFlow};

use filter_parser::{
    ConstraintCondition, ConstraintConditionKind, ConstraintTarget, FilterConstraints,
};
use itertools::Itertools as _;
use roaring::RoaringBitmap;
use time::format_description::well_known::Rfc3339;

use crate::dynamic_search_rules::{fields, DsrFuel, DynamicSearchRulesView};
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::search::facet::facet_range_search::find_docids_of_facet_within_bounds;
use crate::search::facet::value_bounds::{evaluate_equal, to_str_bounds, ValueBounds};
use crate::search::new::LocatedQueryTerm;
use crate::{FieldId, IndexFilter, Result, SearchContext, MAX_COUNTED_WORDS};

impl<'a> DynamicSearchRulesView<'a> {
    pub(super) fn active_rules_for_query(
        &self,
        query_terms: &[LocatedQueryTerm],
        filter: Option<&IndexFilter>,
        search_context: &SearchContext,
        fuel: DsrFuel,
    ) -> Result<RoaringBitmap> {
        let mut active_rules = self.active_rule_ids(true)?;
        let target_time = search_context.before_search.format(&Rfc3339).unwrap();
        self.apply_time_conditions(&mut active_rules, target_time.as_str())?;
        self.apply_query_conditions(&mut active_rules, query_terms, search_context, fuel)?;
        self.apply_filter_conditions(&mut active_rules, filter, fuel)?;

        Ok(active_rules)
    }

    fn apply_time_conditions(
        &self,
        active_rules: &mut RoaringBitmap,
        target_time: &str,
    ) -> Result<(), crate::Error> {
        let db = self.index.facet_id_string_docids;
        if let Some(time_start_fid) = self.db_fields_ids_map.id(fields::CONDITIONS_TIME_START) {
            let mut time_start_after_now = RoaringBitmap::new();

            // looking for all rules whose time.start is AFTER target_time
            // so ]target_time, ..]
            let left = Bound::Excluded(target_time);
            let right = Bound::Unbounded;
            find_docids_of_facet_within_bounds(
                self.rtxn,
                db,
                time_start_fid,
                &left,
                &right,
                Some(&*active_rules),
                &mut time_start_after_now,
            )?;
            *active_rules -= time_start_after_now;
        }
        if let Some(time_end_fid) = self.db_fields_ids_map.id(fields::CONDITIONS_TIME_END) {
            let mut time_end_before_now = RoaringBitmap::new();

            // looking for all rules whose time.end is BEFORE target_time
            // so ].., target_time]
            let left = Bound::Unbounded;
            let right = Bound::Excluded(target_time);
            find_docids_of_facet_within_bounds(
                self.rtxn,
                db,
                time_end_fid,
                &left,
                &right,
                Some(&*active_rules),
                &mut time_end_before_now,
            )?;
            *active_rules -= time_end_before_now;
        }
        Ok(())
    }

    fn apply_query_conditions(
        &self,
        active_rules: &mut RoaringBitmap,
        query_terms: &[LocatedQueryTerm],
        search_context: &SearchContext<'_>,
        mut fuel: DsrFuel,
    ) -> Result<(), crate::Error> {
        // 1. exclude rules that have a different query emptiness condition
        let is_query_empty = query_terms.is_empty();
        if let Some(is_query_empty_fid) =
            self.db_fields_ids_map.id(fields::CONDITIONS_QUERY_IS_EMPTY)
        {
            let left_bound = if is_query_empty { "false" } else { "true" };
            let is_not_query_empty_key =
                FacetGroupKey { field_id: is_query_empty_fid, level: 0, left_bound };

            if let Some(FacetGroupValue { size: _, bitmap: is_not_query_empty_rules }) =
                self.index.facet_id_string_docids.get(self.rtxn, &is_not_query_empty_key)?
            {
                *active_rules -= is_not_query_empty_rules;
            }
        };
        let mut query_terms: Vec<&str> = query_terms
            .iter()
            .filter_map(|word| {
                word.value
                    .original_single_word(search_context)
                    .map(|word| search_context.word_interner.get(word).as_str())
            })
            .collect();
        query_terms.sort_unstable();
        query_terms.dedup();
        let words_count =
            query_terms.len().min(MAX_COUNTED_WORDS).min(fuel.max_counted_words()) as u8;
        if let Some(query_words_fid) = self.db_fields_ids_map.id(fields::CONDITIONS_QUERY_WORDS) {
            let word_count_db = &self.index.field_id_word_count_docids;

            // 2. exclude words with more word constraints than present in the query
            if let Some(words_count_plus_one) = words_count.checked_add(1) {
                for res in word_count_db.range(
                    self.rtxn,
                    &((query_words_fid, words_count_plus_one)..=(query_words_fid, u8::MAX)),
                )? {
                    let ((_, _constraint_count), more_constraints_than_query_rules) = res?;
                    *active_rules -= more_constraints_than_query_rules;
                }
            }

            let mut words_rules = Vec::new();
            for word in query_terms.iter().take(words_count.into()) {
                let Some(mut word_rules) =
                    self.index.word_fid_docids.get(self.rtxn, &(word, query_words_fid))?
                else {
                    continue;
                };

                word_rules &= &*active_rules;

                if word_rules.is_empty() {
                    continue;
                }

                words_rules.push(word_rules);
            }

            for constraint_count in 0..=words_count {
                let Some(constraint_count_rules) =
                    word_count_db.get(self.rtxn, &(query_words_fid, constraint_count))?
                else {
                    continue;
                };

                if constraint_count_rules.is_empty() {
                    continue;
                }

                let mut verifying_constraints_rules = RoaringBitmap::new();

                match constraint_count {
                    0 => {
                        verifying_constraints_rules |= &constraint_count_rules;
                    }
                    1 => {
                        for word_rules in words_rules.iter() {
                            verifying_constraints_rules |= &constraint_count_rules & word_rules;
                        }
                    }
                    k => {
                        for word_rules in words_rules.iter().combinations(k.into()) {
                            verifying_constraints_rules |= roaring::MultiOps::intersection(
                                std::iter::once(&constraint_count_rules).chain(word_rules),
                            );
                            if fuel.consume_word_combination().is_break() {
                                break;
                            }
                        }
                    }
                }
                // 3. exclude rules that have that number of word constraints but don't verify the constraints
                match fuel.consume_word_combination() {
                    ControlFlow::Continue(()) => {
                        *active_rules -= constraint_count_rules - verifying_constraints_rules
                    }
                    // no more fuel, we have to remove all rules because we couldn't complete `verifying_constraints_rules`
                    ControlFlow::Break(()) => *active_rules -= constraint_count_rules,
                }
            }
        }
        Ok(())
    }

    fn apply_filter_conditions(
        &self,
        active_rules: &mut RoaringBitmap,
        filter: Option<&IndexFilter>,
        mut fuel: DsrFuel,
    ) -> Result<(), crate::Error> {
        let constraints = filter
            .map(|filter| {
                FilterConstraints::new(&filter.condition, &mut fuel.filter_constraint_fuel)
            })
            .unwrap_or_default();

        let Some(nb_constraints_fid) =
            self.db_fields_ids_map.id(fields::CONDITIONS_FILTER_NB_CONSTRAINTS)
        else {
            return Ok(());
        };

        active_rules.len();

        let max_constraints = constraints.max_number_of_constraints();

        // 1. exclude rules that have more constraints than max_constraints
        let mut too_many_constraints = Default::default();
        find_docids_of_facet_within_bounds(
            self.rtxn,
            self.index.facet_id_f64_docids,
            nb_constraints_fid,
            &Bound::Excluded(max_constraints as f64),
            &Bound::Unbounded,
            Some(active_rules),
            &mut too_many_constraints,
        )?;

        *active_rules -= too_many_constraints;

        if max_constraints == 0 {
            return Ok(());
        }

        // solve all constraints
        let mut solved_constraints = Vec::new();

        for constraints in &constraints.constraints {
            let mut solved_constraint = Vec::new();
            for (target, constraints) in constraints {
                let matching = match target {
                    ConstraintTarget::Fid(fid) => {
                        let facet_value_name = format!(
                            "{}.{}",
                            fields::CONDITIONS_FILTER_VALUES,
                            fid.original_fragment()
                        );
                        match self.db_fields_ids_map.id(&facet_value_name) {
                            Some(fid) => {
                                self.resolve_constraints(fid, constraints, active_rules)?
                            }
                            None => RoaringBitmap::new(),
                        }
                    }
                    ConstraintTarget::Vector { .. } | ConstraintTarget::Geo => {
                        // not solving for vector or geo currently
                        RoaringBitmap::default()
                    }
                };
                if !matching.is_empty() {
                    solved_constraint.push(matching);
                }
            }
            solved_constraints.push(solved_constraint);
        }

        // exclude rules with k constraints that don't verify k constraints
        for constraint_count in 1..=max_constraints {
            let key = FacetGroupKey {
                field_id: nb_constraints_fid,
                level: 0,
                left_bound: constraint_count as f64,
            };
            let Some(FacetGroupValue { size: _, bitmap: constraint_count_rules }) =
                self.index.facet_id_f64_docids.get(self.rtxn, &key)?
            else {
                continue;
            };
            let mut verifying_constraints_rules = RoaringBitmap::new();

            if constraint_count_rules.is_empty() {
                continue;
            }

            for solved_constraint in &solved_constraints {
                for combination in solved_constraint.iter().combinations(constraint_count) {
                    if fuel.consume_filter_combination().is_break() {
                        break;
                    }
                    verifying_constraints_rules |= roaring::MultiOps::intersection(
                        std::iter::once(&constraint_count_rules).chain(combination),
                    );
                }
            }
            match fuel.consume_filter_combination() {
                ControlFlow::Continue(()) => {
                    *active_rules -= constraint_count_rules - verifying_constraints_rules;
                }
                // no more fuel, we have to remove all rules because the computation might be incomplete
                ControlFlow::Break(()) => *active_rules -= constraint_count_rules,
            }
        }

        Ok(())
    }

    fn resolve_constraints(
        &self,
        fid: FieldId,
        constraints: &[ConstraintCondition],
        active_rules: &RoaringBitmap,
    ) -> Result<RoaringBitmap> {
        let mut matching = active_rules.clone();

        for constraint in constraints {
            let mut polarity = constraint.polarity;
            let evaluated = match &constraint.kind {
                ConstraintConditionKind::Condition { condition } => {
                    match ValueBounds::new(condition) {
                        ValueBounds::Range { normalized, number } => {
                            let mut evaluated = RoaringBitmap::new();

                            {
                                let (left, right) = to_str_bounds(&normalized);
                                let db = self.index.facet_id_string_docids;
                                find_docids_of_facet_within_bounds(
                                    self.rtxn,
                                    db,
                                    fid,
                                    &left,
                                    &right,
                                    Some(active_rules),
                                    &mut evaluated,
                                )?;
                            };

                            if let Some((left, right)) = number {
                                let db = self.index.facet_id_f64_docids;
                                find_docids_of_facet_within_bounds(
                                    self.rtxn,
                                    db,
                                    fid,
                                    &left,
                                    &right,
                                    Some(active_rules),
                                    &mut evaluated,
                                )?;
                            }
                            evaluated
                        }
                        // no effect if polarity = false, removes everything otherwise
                        ValueBounds::FieldIsEmpty | ValueBounds::FieldIsNull => {
                            RoaringBitmap::new()
                        }
                        // no effect if polarity = true, removes everything otherwise
                        ValueBounds::FieldExists => active_rules.clone(),
                        ValueBounds::Equal { normalized, number } => evaluate_equal(
                            self.rtxn,
                            fid,
                            self.index.facet_id_f64_docids,
                            self.index.facet_id_string_docids,
                            normalized,
                            number,
                        )?,
                        ValueBounds::NotEqual { normalized, number } => {
                            polarity = !polarity;
                            evaluate_equal(
                                self.rtxn,
                                fid,
                                self.index.facet_id_f64_docids,
                                self.index.facet_id_string_docids,
                                normalized,
                                number,
                            )?
                        }
                        ValueBounds::Contains { normalized: _ }
                        | ValueBounds::StartsWith { normalized: _ } => {
                            return Ok(Default::default())
                        }
                    }
                }
                // always unsupported, considered unsatisfiable
                ConstraintConditionKind::VectorExists { .. }
                | ConstraintConditionKind::GeoLowerThan { .. }
                | ConstraintConditionKind::GeoBoundingBox { .. }
                | ConstraintConditionKind::GeoPolygon { .. } => return Ok(Default::default()),
            };
            if polarity {
                // exclude rules that were evaluated to 0
                matching &= evaluated;
            } else {
                // exclude rules that were evaluated to 1
                matching -= evaluated;
            }
            if matching.is_empty() {
                break;
            }
        }
        Ok(matching)
    }
}

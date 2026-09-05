use std::num::Saturating;
use std::ops::{Bound, ControlFlow, Not as _};

use filter_parser::{
    ConstraintCondition, ConstraintConditionKind, ConstraintTarget, FilterConstraintFuel,
    FilterConstraints,
};
use heed::{RoTxn, WithoutTls};
use itertools::Itertools as _;
use roaring::RoaringBitmap;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::search::facet::ascending_facet_sort;
use crate::search::facet::facet_range_search::find_docids_of_facet_within_bounds;
use crate::search::facet::value_bounds::{evaluate_equal, to_str_bounds, ValueBounds};
use crate::search::new::LocatedQueryTerm;
use crate::search::{Pin, Precedence, ScaleDocs};
use crate::update::new::document::DocumentFromDb;
use crate::{
    AscDesc, DocumentId, FieldId, FieldsIdsMap, Filter, Index, IndexFilter, PinDoc, Result,
    SearchContext, SearchResult, UserError, MAX_COUNTED_WORDS,
};

pub type RuleId = u32;

/// Wrapper around the DSR index, allowing to search for active rules
pub struct DynamicSearchRules {
    index: Index,
    rtxn: RoTxn<'static, WithoutTls>,
    db_fields_ids_map: FieldsIdsMap,
}

#[derive(Clone, Copy)]
pub struct DynamicSearchRulesView<'a> {
    index: &'a Index,
    rtxn: &'a RoTxn<'a, WithoutTls>,
    db_fields_ids_map: &'a FieldsIdsMap,
}

impl<'a> DynamicSearchRulesView<'a> {
    pub fn new(
        index: &'a Index,
        rtxn: &'a RoTxn<'a, WithoutTls>,
        db_fields_ids_map: &'a FieldsIdsMap,
    ) -> Self {
        Self { index, rtxn, db_fields_ids_map }
    }

    pub fn get(self, rule_uid: &str) -> Result<Option<DocumentFromDb<'a, FieldsIdsMap>>> {
        let Some(docid) = self.index.external_documents_ids().get(self.rtxn, rule_uid)? else {
            return Ok(None);
        };

        self.get_from_internal_id(docid)
    }

    pub fn get_from_internal_id(
        self,
        rule_id: RuleId,
    ) -> Result<Option<DocumentFromDb<'a, FieldsIdsMap>>> {
        let Some(doc) =
            DocumentFromDb::new(rule_id, self.rtxn, self.index, self.db_fields_ids_map)?
        else {
            return Ok(None);
        };

        Ok(Some(doc))
    }

    pub fn resolve_actions(
        &self,
        query_terms: &[LocatedQueryTerm],
        filter: Option<&IndexFilter>,
        universe: &mut RoaringBitmap,
        search_context: &SearchContext,
        fuel: DsrFuel,
    ) -> Result<(Vec<PinDoc>, Vec<ScaleDocs>)> {
        wip::fixme!("write migration for DSRs to accomodate to new actions syntax");
        let active_rules =
            self.active_rules_for_query(query_terms, filter, search_context, fuel)?;

        let mut pins = Vec::new();
        let mut scales = Vec::new();

        for res in self.find_actions(
            self.rule_ids_sorted_by_precedence(active_rules)?,
            search_context,
            fuel,
        ) {
            if pins.len() >= fuel.max_pin_actions() && scales.len() >= fuel.max_scale_actions() {
                break;
            }

            let (pin, scale) = res?;
            for pin in pin {
                if pins.len() >= fuel.max_pin_actions() {
                    break;
                }
                let pin = pin?;
                if universe.remove(pin.id) {
                    pins.push(pin);
                }
            }

            for scale in scale {
                if scales.len() >= fuel.max_scale_actions() {
                    break;
                }
                let scale = scale?;
                scales.push(scale);
            }
        }

        Pin::dedup_and_sort(&mut pins);

        Ok((pins, scales))
    }

    pub fn rules_from_rule_ids<I>(
        self,
        rule_ids: I,
    ) -> impl ExactSizeIterator<Item = Result<DocumentFromDb<'a, FieldsIdsMap>>>
    where
        I: IntoIterator<Item = RuleId>,
        I::IntoIter: ExactSizeIterator + 'a,
    {
        rule_ids.into_iter().map(move |rule_id| {
            self.get_from_internal_id(rule_id)
                .transpose()
                .ok_or(UserError::UnknownInternalDocumentId { document_id: rule_id }.into())
                .flatten()
        })
    }

    /// Find the list of active or inactive rules, depending on `is_active`.
    ///
    /// If no rule contains the "active" field, then all declared rules are considered active.
    pub fn active_rule_ids(&self, is_active: bool) -> Result<RoaringBitmap> {
        let left_bound = if is_active { "true" } else { "false" };
        let active_rules = if let Some(active_fid) = self.db_fields_ids_map.id(fields::ACTIVE) {
            let active_key = FacetGroupKey { field_id: active_fid, level: 0, left_bound };
            let Some(FacetGroupValue { size: _, bitmap: active_rules }) =
                self.index.facet_id_string_docids.get(self.rtxn, &active_key)?
            else {
                return Ok(RoaringBitmap::new());
            };
            active_rules
        } else if is_active {
            self.index.documents_ids(self.rtxn)?
        } else {
            RoaringBitmap::default()
        };
        Ok(active_rules)
    }

    pub fn all_rule_ids(&self) -> Result<RoaringBitmap> {
        Ok(self.index.documents_ids(self.rtxn)?)
    }

    pub fn search_in_description_and_words(
        &self,
        query: Option<String>,
        universe: &RoaringBitmap,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResult> {
        let progress = Default::default();
        let mut search = self.index.search(
            self.rtxn,
            "",
            self.db_fields_ids_map,
            OffsetDateTime::now_utc(),
            &progress,
        );

        if let Some(query) = query {
            search.query(query);
        }

        search.candidates(universe);

        search.exhaustive_number_hits(true);
        search.limit(limit);
        search.offset(offset);
        search.sort_criteria(vec![AscDesc::Desc(crate::Member::Field(
            fields::LAST_UPDATED_AT.into(),
        ))]);
        let searchable_attrs = [fields::DESCRIPTION.into(), fields::CONDITIONS_QUERY_WORDS.into()];
        search.searchable_attributes(&searchable_attrs);

        search.execute()
    }

    fn find_pin_actions(
        precedence: Precedence,
        actions: Vec<PinAction>,
        search_context: &'a SearchContext,
    ) -> impl Iterator<Item = Result<PinDoc>> + 'a {
        actions.into_iter().filter_map(move |action| {
            let doc_id = action.active_document(search_context).transpose()?;

            let doc_id = match doc_id {
                Ok(doc_id) => doc_id,
                Err(err) => return Some(Err(err)),
            };

            Some(Ok(PinDoc { position: action.position, precedence, id: doc_id }))
        })
    }

    fn find_scale_actions(
        actions: Vec<ScaleAction>,
        search_context: &'a SearchContext,
    ) -> impl Iterator<Item = Result<ScaleDocs>> + 'a {
        actions.into_iter().filter_map(|action| {
            let docs = action.active_documents(search_context).transpose()?;

            let docs = match docs {
                Ok(docs) => docs,
                Err(err) => return Some(Err(err)),
            };

            Some(Ok(ScaleDocs { docs, weight: action.weight }))
        })
    }

    fn find_actions(
        self,
        sorted_active_rules: impl IntoIterator<Item = Result<RuleId>> + 'a,
        search_context: &'a SearchContext,
        fuel: DsrFuel,
    ) -> impl Iterator<
        Item = Result<(
            impl Iterator<Item = Result<PinDoc>> + 'a,
            impl Iterator<Item = Result<ScaleDocs>> + 'a,
        )>,
    > + 'a {
        sorted_active_rules
            .into_iter()
            .take(fuel.max_active_rules())
            .map(move |rule_id| {
                let rule_id = rule_id?;
                let Some(rule) =
                    DocumentFromDb::new(rule_id, self.rtxn, self.index, self.db_fields_ids_map)?
                else {
                    tracing::warn!(
                        "rule with internal id `{rule_id}` could not be found in docs db"
                    );
                    return Ok(None);
                };

                let Some(actions) = rule.field(fields::ACTIONS)? else {
                    return Ok(None);
                };

                let precedence: Result<Option<u64>, _> = match rule.field(fields::PRECEDENCE)? {
                    Some(precedence) => serde_json::from_str(precedence.get()),
                    None => Ok(None),
                };

                let precedence = match precedence {
                    Ok(precedence) => precedence,
                    Err(err) => {
                        tracing::warn!(
                        "could not deserialize actions of rule with internal id `{rule_id}`: {err}"
                    );
                        return Ok(None);
                    }
                };

                let actions: Result<RuleActions, serde_json::Error> =
                    serde_json::from_str(actions.get());
                match actions {
                    Ok(actions) => Ok(Some((
                        Self::find_pin_actions(Precedence(precedence), actions.pin, search_context),
                        Self::find_scale_actions(actions.scale, search_context),
                    ))),
                    Err(err) => {
                        tracing::warn!(
                        "could not deserialize actions of rule with internal id `{rule_id}`: {err}"
                    );
                        Ok(None)
                    }
                }
            })
            .filter_map(|x| x.transpose())
    }

    fn active_rules_for_query(
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
                                std::iter::once(&constraint_count_rules)
                                    .chain(word_rules.into_iter()),
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
                        std::iter::once(&constraint_count_rules).chain(combination.into_iter()),
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

    fn rule_ids_sorted_by_precedence(
        self,
        mut active_rules: RoaringBitmap,
    ) -> Result<impl Iterator<Item = Result<RuleId>> + 'a> {
        let db = self.index.facet_id_f64_docids.remap_types();

        if let Some(precedence_field_id) = self.db_fields_ids_map.id(fields::PRECEDENCE) {
            // faceted = active rules with a non-null field
            let mut faceted = self
                .index
                .facet_id_exists_docids
                .get(self.rtxn, &precedence_field_id)?
                .unwrap_or_default();

            faceted &= &active_rules;
            faceted -= self
                .index
                .facet_id_is_null_docids
                .get(self.rtxn, &precedence_field_id)?
                .unwrap_or_default();

            // partition the active rules depending on whether they are faceted
            active_rules -= &faceted;
            Ok(either::Left(
                ascending_facet_sort(self.rtxn, db, precedence_field_id, faceted)?
                    .flat_map(|res| match res {
                        Ok((bucket, _precedence)) => {
                            either::Either::Left(bucket.into_iter().map(Ok))
                        }
                        Err(err) => either::Either::Right(std::iter::once(Err(err.into()))),
                    })
                    .chain(active_rules.into_iter().map(Ok)),
            ))
        } else {
            Ok(either::Right(active_rules.into_iter().map(Ok)))
        }
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

impl DynamicSearchRules {
    pub fn new(index: Index) -> Result<Self> {
        let rtxn = index.static_read_txn()?;

        let db_fields_ids_map = index.fields_ids_map(&rtxn)?;
        Ok(Self { index, rtxn, db_fields_ids_map })
    }

    pub fn as_view(&self) -> DynamicSearchRulesView<'_> {
        DynamicSearchRulesView {
            index: &self.index,
            rtxn: &self.rtxn,
            db_fields_ids_map: &self.db_fields_ids_map,
        }
    }

    pub fn as_raw(&self) -> (&Index, &RoTxn<'static, WithoutTls>, &FieldsIdsMap) {
        (&self.index, &self.rtxn, &self.db_fields_ids_map)
    }

    pub fn get<'t>(&'t self, rule_uid: &str) -> Result<Option<DocumentFromDb<'t, FieldsIdsMap>>> {
        self.as_view().get(rule_uid)
    }

    pub fn get_from_internal_id<'t>(
        &'t self,
        rule_id: RuleId,
    ) -> Result<Option<DocumentFromDb<'t, FieldsIdsMap>>> {
        self.as_view().get_from_internal_id(rule_id)
    }

    pub fn resolve_actions(
        &self,
        query_terms: &[LocatedQueryTerm],
        filter: Option<&IndexFilter>,
        universe: &mut RoaringBitmap,
        search_context: &SearchContext,
        fuel: DsrFuel,
    ) -> Result<(Vec<PinDoc>, Vec<ScaleDocs>)> {
        self.as_view().resolve_actions(query_terms, filter, universe, search_context, fuel)
    }

    pub fn rules_from_rule_ids<'t, I>(
        &'t self,
        rule_ids: I,
    ) -> impl ExactSizeIterator<Item = Result<DocumentFromDb<'t, FieldsIdsMap>>>
    where
        I: IntoIterator<Item = RuleId>,
        I::IntoIter: ExactSizeIterator + 't,
    {
        self.as_view().rules_from_rule_ids(rule_ids)
    }

    /// Find the list of active or inactive rules, depending on `is_active`.
    ///
    /// If no rule contains the "active" field, then all declared rules are considered active.
    pub fn active_rule_ids(&self, is_active: bool) -> Result<RoaringBitmap> {
        self.as_view().active_rule_ids(is_active)
    }

    pub fn all_rule_ids(&self) -> Result<RoaringBitmap> {
        self.as_view().all_rule_ids()
    }

    pub fn search_in_description_and_words(
        &self,
        query: Option<String>,
        universe: &RoaringBitmap,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResult> {
        self.as_view().search_in_description_and_words(query, universe, limit, offset)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DsrFuel {
    max_counted_words: u8,
    max_active_rules: u32,
    max_pin_actions: u32,
    max_scale_actions: u8,
    remaining_word_fuel: Saturating<u32>,
    remaining_filter_fuel: Saturating<u32>,
    remaining_scale_fuel: Saturating<u8>,
    filter_constraint_fuel: FilterConstraintFuel,
}

impl DsrFuel {
    pub fn new(
        max_counted_words: u8,
        max_active_rules: u32,
        max_pin_actions: u32,
        max_scale_actions: u8,
        word_fuel: u32,
        filter_fuel: u32,
        scale_fuel: u8,
        filter_constraint_fuel: FilterConstraintFuel,
    ) -> Self {
        Self {
            max_counted_words,
            max_active_rules,
            max_pin_actions,
            max_scale_actions,
            remaining_word_fuel: Saturating(word_fuel),
            remaining_filter_fuel: Saturating(filter_fuel),
            remaining_scale_fuel: Saturating(scale_fuel),
            filter_constraint_fuel,
        }
    }

    pub fn max_counted_words(&self) -> usize {
        self.max_counted_words.into()
    }

    pub fn consume_word_combination(&mut self) -> ControlFlow<(), ()> {
        self.remaining_word_fuel -= 1;
        if self.remaining_word_fuel.0 == 0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    pub fn consume_filter_combination(&mut self) -> ControlFlow<(), ()> {
        self.remaining_filter_fuel -= 1;
        if self.remaining_filter_fuel.0 == 0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    pub fn consume_scale_combination(&mut self) -> ControlFlow<(), ()> {
        self.remaining_scale_fuel -= 1;
        if self.remaining_scale_fuel.0 == 0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    pub fn max_active_rules(&self) -> usize {
        self.max_active_rules as usize
    }

    pub fn max_pin_actions(&self) -> usize {
        self.max_pin_actions as usize
    }

    fn max_scale_actions(&self) -> usize {
        self.max_scale_actions as usize
    }
}

/// Fields used in DSR documents
pub mod fields {
    pub const UID: &str = "uid";
    pub const ACTIVE: &str = "active";
    pub const PRECEDENCE: &str = "precedence";
    pub const DESCRIPTION: &str = "description";
    pub const ACTIONS: &str = "actions";
    pub const LAST_UPDATED_AT: &str = "lastUpdatedAt";

    pub const CONDITIONS: &str = "conditions";
    pub const FILTER: &str = "filter";
    pub const NB_CONSTRAINTS: &str = "nbConstraints";

    pub const CONDITIONS_TIME_START: &str = "conditions.time.start";
    pub const CONDITIONS_TIME_END: &str = "conditions.time.end";
    pub const CONDITIONS_QUERY_IS_EMPTY: &str = "conditions.query.isEmpty";
    pub const CONDITIONS_QUERY_WORDS: &str = "conditions.query.words";
    pub const CONDITIONS_FILTER_NB_CONSTRAINTS: &str = "conditions.filter.nbConstraints";
    pub const CONDITIONS_FILTER_VALUES: &str = "conditions.filter.values";
}

/// List of actions to apply when this rule is active for the query.
#[routes::request(proxied, db, setting, no_error)]
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RuleActions {
    /// Pins a selected document.
    #[request(default)]
    pub pin: Vec<PinAction>,
    /// Applies a multiplicative factor to the score of selected documents.
    #[request(default)]
    pub scale: Vec<ScaleAction>,
}

/// An action that pins a selected document.
#[routes::request(proxied, db, setting, no_error)]
#[derive(Debug, Clone, PartialEq)]
pub struct PinAction {
    /// Index name.
    ///
    /// For the action to select any document, when this parameter is provided,
    /// the index of the query must match the provided parameter.
    #[request(default)]
    pub index_uid: Option<String>,
    /// Document ID of the document to select.
    ///
    /// Only the document whose [primary key](https://www.meilisearch.com/docs/learn/getting_started/primary_key) value
    /// matches the specified id will be selected by the action.
    ///
    /// If there is no such document in the index of the query, then no documents will be selected and no pinning will occur.
    #[request(required)]
    pub id: String,
    /// Position at which the document should be pinned.
    #[request(required)]
    pub position: u32,
}

impl PinAction {
    pub fn active_document(
        &self,
        search_context: &SearchContext<'_>,
    ) -> Result<Option<DocumentId>> {
        if let Some(target_index_uid) = &self.index_uid {
            if search_context.index_uid != target_index_uid {
                return Ok(None);
            }
        }

        Ok(search_context.index.external_documents_ids().get(search_context.txn, &self.id)?)
    }
}

/// An action that applies a multiplicative factor to the score of selected documents.
#[routes::request(proxied, db, setting, no_error)]
#[derive(Debug, Clone, PartialEq)]
pub struct ScaleAction {
    /// List of index patterns.
    ///
    /// For the action to select any document, when this parameter is provided,
    /// the index of the query must match the provided parameter.
    #[request(default)]
    pub index_uid: Option<String>,
    /// Array of specific document IDs to select.
    ///
    /// Only documents whose [primary key](https://www.meilisearch.com/docs/learn/getting_started/primary_key) value
    /// matches the specified ids will be selected by the action.
    ///
    /// If `filter` is also specified,
    /// the documents must also satisfy the filter to be selected.
    #[request(default)]
    pub ids: Option<Vec<String>>,
    /// Filter expression to select documents. Attributes must be added to the
    /// `filterableAttributes` index setting before they can be used in filters.
    /// Accepts a string or an array of arrays of strings for AND/OR combinations.
    ///
    /// Only documents matching the specified filter will be selected.
    ///
    /// If `ids` is also specified,
    /// the documents matching the filter must also have their primary key part of the `ids`
    /// list to be selected.
    ///
    /// If the filter cannot be evaluated for the current index due to referencing attributes
    /// that are not filterable, then no document will be applied for this action.
    #[request(default)]
    pub filter: Option<serde_json::Value>,
    /// Scale factor for selected documents.
    ///
    /// - Set it >1.0 to boost the selected documents.
    /// - Set it <1.0 to deboost the selected documents.
    /// - Set it =0.0 to hide the selected documents.
    #[request(required)]
    pub weight: f64,
}

impl ScaleAction {
    pub fn active_documents(
        &self,
        search_context: &SearchContext<'_>,
    ) -> Result<Option<RoaringBitmap>> {
        if let Some(target_index_uid) = &self.index_uid {
            if search_context.index_uid != target_index_uid {
                return Ok(None);
            }
        }

        Ok(match (&self.ids, &self.filter) {
            (None, None) => None,
            (None, Some(filter)) => {
                let Ok(filter) = Filter::from_json(filter) else {
                    tracing::warn!("cannot parse filter for DSR");
                    return Ok(None);
                };

                let Some(filter) = filter else { return Ok(None) };
                // filter was parsed and checked for foreign at update time
                let Ok(filter) = IndexFilter::from_filter_without_foreign(filter) else {
                    tracing::warn!("filter for DSR contains foreign");
                    return Ok(None);
                };

                let Ok(candidates) = filter.evaluate(
                    search_context.txn,
                    search_context.index,
                    search_context.fields_ids_map,
                ) else {
                    return Ok(None);
                };

                if candidates.is_empty() {
                    None
                } else {
                    Some(candidates)
                }
            }
            (Some(ids), None) => {
                let candidates = candidates_from_ids(search_context, ids)?;
                candidates.is_empty().not().then_some(candidates)
            }
            (Some(ids), Some(filter)) => {
                let mut candidates = candidates_from_ids(search_context, ids)?;
                if candidates.is_empty() {
                    return Ok(None);
                }
                let Ok(filter) = Filter::from_json(filter) else {
                    tracing::warn!("cannot parse filter for DSR");
                    return Ok(None);
                };

                let Some(filter) = filter else { return Ok(Some(candidates)) };
                // filter was parsed and checked for foreign at update time
                let Ok(filter) = IndexFilter::from_filter_without_foreign(filter) else {
                    tracing::warn!("filter for DSR contains foreign");
                    return Ok(None);
                };

                let Ok(filter_candidates) = filter.evaluate(
                    search_context.txn,
                    search_context.index,
                    search_context.fields_ids_map,
                ) else {
                    return Ok(None);
                };

                candidates &= filter_candidates;

                if candidates.is_empty() {
                    None
                } else {
                    Some(candidates)
                }
            }
        })
    }
}

fn candidates_from_ids(
    search_context: &SearchContext<'_>,
    ids: &[String],
) -> Result<RoaringBitmap> {
    let mut candidates = RoaringBitmap::new();
    for id in ids {
        let Some(id) = search_context.index.external_documents_ids().get(search_context.txn, id)?
        else {
            continue;
        };
        candidates.push(id);
    }
    Ok(candidates)
}

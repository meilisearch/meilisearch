use std::ops::{Bound, ControlFlow};

use heed::{RoTxn, WithoutTls};
use itertools::Itertools as _;
use roaring::RoaringBitmap;
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::search::facet::ascending_facet_sort;
use crate::search::facet::facet_range_search::find_docids_of_facet_within_bounds;
use crate::search::new::LocatedQueryTerm;
use crate::update::new::document::DocumentFromDb;
use crate::{
    DocumentId, FieldsIdsMap, Index, PinDoc, Result, SearchContext, SearchResult, UserError,
    MAX_COUNTED_WORDS,
};

type RuleId = u32;

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

    pub fn resolve_pins(
        &self,
        query_terms: &[LocatedQueryTerm],
        universe: &mut RoaringBitmap,
        search_context: &SearchContext,
        fuel: DsrFuel,
    ) -> Result<Vec<PinDoc>> {
        let active_rules = self.active_rules_for_query(query_terms, search_context, fuel)?;

        self.find_pins(self.rule_ids_sorted_by_precedence(active_rules)?, search_context)
            .take(fuel.max_active_rules())
            .filter(
                |pin| {
                    if let Ok(pin) = pin.as_ref() {
                        universe.remove(pin.doc_id)
                    } else {
                        true
                    }
                },
            )
            .take(fuel.max_pin_actions())
            .collect()
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
        let active_rules = if let Some(active_fid) = self.db_fields_ids_map.id("active") {
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
        universe: RoaringBitmap,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResult> {
        let progress = Default::default();
        let mut search = self.index.search(self.rtxn, "", OffsetDateTime::now_utc(), &progress);

        if let Some(query) = query {
            search.query(query);
        }

        search.candidates(&universe);

        search.exhaustive_number_hits(true);
        search.max_total_hits(Some(
            self.index.pagination_max_total_hits(self.rtxn)?.unwrap_or(1000) as usize,
        ));
        search.limit(limit);
        search.offset(offset);
        let searchable_attrs = ["description".into(), "conditions.query.words".into()];
        search.searchable_attributes(&searchable_attrs);

        search.execute()
    }

    fn find_pins(
        self,
        sorted_active_rules: impl IntoIterator<Item = Result<RuleId>> + 'a,
        search_context: &'a SearchContext,
    ) -> impl Iterator<Item = Result<PinDoc>> + 'a {
        sorted_active_rules
            .into_iter()
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

                let Some(actions) = rule.field("actions")? else {
                    return Ok(None);
                };
                let actions: Result<Vec<RuleAction>, serde_json::Error> =
                    serde_json::from_str(actions.get());
                match actions {
                    Ok(actions) => Ok(Some(actions.into_iter())),
                    Err(err) => {
                        tracing::warn!(
                        "could not deserialize actions of rule with internal id `{rule_id}`: {err}"
                    );
                        Ok(None)
                    }
                }
            })
            .filter_map(|x| x.transpose())
            .flatten_ok()
            .filter_map_ok(|action| {
                let doc_id = action.active_document(search_context).transpose()?;

                let doc_id = match doc_id {
                    Ok(doc_id) => doc_id,
                    Err(err) => return Some(Err(err)),
                };
                match action.action {
                    DynamicSearchRuleAction::Pin { position } => {
                        Some(Ok(PinDoc { pos: position, doc_id }))
                    }
                }
            })
            .map(|x| x.flatten())
    }

    fn active_rules_for_query(
        &self,
        query_terms: &[LocatedQueryTerm],
        search_context: &SearchContext,
        mut fuel: DsrFuel,
    ) -> Result<RoaringBitmap> {
        // 1. include rules that are active
        let mut active_rules = self.active_rule_ids(true)?;
        // 2. exclude rules that have a time condition that is not met
        let target_time = search_context.before_search.format(&Rfc3339).unwrap();
        let db = self.index.facet_id_string_docids;
        if let Some(time_start_fid) = self.db_fields_ids_map.id("conditions.time.start") {
            let mut time_start_after_now = RoaringBitmap::new();

            // looking for all rules whose time.start is AFTER target_time
            // so ]target_time, ..]
            let left = Bound::Excluded(target_time.as_str());
            let right = Bound::Unbounded;
            find_docids_of_facet_within_bounds(
                self.rtxn,
                db,
                time_start_fid,
                &left,
                &right,
                Some(&active_rules),
                &mut time_start_after_now,
            )?;
            active_rules -= time_start_after_now;
        }
        if let Some(time_end_fid) = self.db_fields_ids_map.id("conditions.time.end") {
            let mut time_end_before_now = RoaringBitmap::new();

            // looking for all rules whose time.end is BEFORE target_time
            // so ].., target_time]
            let left = Bound::Unbounded;
            let right = Bound::Excluded(target_time.as_str());
            find_docids_of_facet_within_bounds(
                self.rtxn,
                db,
                time_end_fid,
                &left,
                &right,
                Some(&active_rules),
                &mut time_end_before_now,
            )?;
            active_rules -= time_end_before_now;
        }

        // 3. exclude rules that have the a different query emptiness condition
        let is_query_empty = query_terms.is_empty();
        if let Some(is_query_empty_fid) = self.db_fields_ids_map.id("conditions.query.isEmpty") {
            if is_query_empty {
                let is_query_not_empty_key =
                    FacetGroupKey { field_id: is_query_empty_fid, level: 0, left_bound: "false" };
                if let Some(FacetGroupValue { size: _, bitmap: is_query_not_empty_rules }) =
                    self.index.facet_id_string_docids.get(self.rtxn, &is_query_not_empty_key)?
                {
                    active_rules -= is_query_not_empty_rules;
                }
            } else {
                let is_query_empty_key =
                    FacetGroupKey { field_id: is_query_empty_fid, level: 0, left_bound: "true" };
                if let Some(FacetGroupValue { size: _, bitmap: is_query_empty_rules }) =
                    self.index.facet_id_string_docids.get(self.rtxn, &is_query_empty_key)?
                {
                    active_rules -= is_query_empty_rules;
                }
            }
        };

        let words_count =
            query_terms.len().min(MAX_COUNTED_WORDS).min(fuel.max_counted_words()) as u8;
        if let Some(query_words_fid) = self.db_fields_ids_map.id("conditions.query.words") {
            let word_count_db = &self.index.field_id_word_count_docids;

            // 4. exclude words with more word constraints than present in the query
            if let Some(words_count_plus_one) = words_count.checked_add(1) {
                for res in word_count_db.range(
                    self.rtxn,
                    &((query_words_fid, words_count_plus_one)..=(query_words_fid, u8::MAX)),
                )? {
                    let ((_, _constraint_count), more_constraints_than_query_rules) = res?;
                    active_rules -= more_constraints_than_query_rules;
                }
            }

            let mut words_rules = Vec::new();
            for word in query_terms.iter().take(words_count.into()) {
                let Some(word) = word.value.original_single_word(search_context) else {
                    continue;
                };
                let word = search_context.word_interner.get(word).as_str();
                let Some(mut word_rules) =
                    self.index.word_fid_docids.get(self.rtxn, &(word, query_words_fid))?
                else {
                    continue;
                };

                word_rules &= &active_rules;

                if word_rules.is_empty() {
                    continue;
                }

                words_rules.push(word_rules);
            }

            // 5. check that the correct constraints are present
            for constraint_count in 0..=words_count {
                let Some(constraint_count_rules) =
                    word_count_db.get(self.rtxn, &(query_words_fid, constraint_count))?
                else {
                    continue;
                };

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
                // remove all rules that have that number of words but don't verify the constraints
                match fuel.consume_word_combination() {
                    ControlFlow::Continue(()) => {
                        active_rules -= constraint_count_rules - verifying_constraints_rules
                    }
                    // no more fuel, we have to remove all rules because we couldn't complete `verifying_constraints_rules`
                    ControlFlow::Break(()) => active_rules -= constraint_count_rules,
                }
            }
        }

        Ok(active_rules)
    }

    fn rule_ids_sorted_by_precedence(
        self,
        active_rules: RoaringBitmap,
    ) -> Result<impl Iterator<Item = Result<RuleId>> + 'a> {
        let db = self.index.facet_id_f64_docids.remap_types();

        if let Some(precedence_field_id) = self.db_fields_ids_map.id("precedence") {
            Ok(either::Left(
                ascending_facet_sort(self.rtxn, db, precedence_field_id, active_rules)?.flat_map(
                    |res| match res {
                        Ok((bucket, _precedence)) => {
                            either::Either::Left(bucket.into_iter().map(Ok))
                        }
                        Err(err) => either::Either::Right(std::iter::once(Err(err.into()))),
                    },
                ),
            ))
        } else {
            Ok(either::Right(active_rules.into_iter().map(Ok)))
        }
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

    pub fn resolve_pins(
        &self,
        query_terms: &[LocatedQueryTerm],
        universe: &mut RoaringBitmap,
        search_context: &SearchContext,
        fuel: DsrFuel,
    ) -> Result<Vec<PinDoc>> {
        self.as_view().resolve_pins(query_terms, universe, search_context, fuel)
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
        universe: RoaringBitmap,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResult> {
        self.as_view().search_in_description_and_words(query, universe, limit, offset)
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuleAction {
    /// Target document selector for this action.
    pub selector: Selector,
    // Use Object here because utoipa's tagged-enum schema generation combines
    // allOf with additionalProperties: false in a way that Spectral rejects.
    /// Action payload to apply to the selected document.
    pub action: DynamicSearchRuleAction,
}

impl RuleAction {
    fn active_document(&self, search_context: &SearchContext<'_>) -> Result<Option<DocumentId>> {
        if self.selector.index_uid.as_ref().is_some_and(|selector_index_uid| {
            selector_index_uid.as_str() != search_context.index_uid
        }) {
            return Ok(None);
        }

        Ok(search_context
            .index
            .external_documents_ids()
            .get(search_context.txn, &self.selector.id)?)
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Selector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<String>,
    pub id: String,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
enum DynamicSearchRuleAction {
    Pin { position: u32 },
}

#[derive(Debug, Clone, Copy)]
pub struct DsrFuel {
    max_counted_words: u8,
    max_active_rules: u32,
    max_pin_actions: u32,
    remaining_word_fuel: std::num::Saturating<u32>,
}
impl DsrFuel {
    pub fn new(
        max_counted_words: u8,
        max_active_rules: u32,
        max_pin_actions: u32,
        word_fuel: u32,
    ) -> Self {
        Self {
            max_counted_words,
            max_active_rules,
            max_pin_actions,
            remaining_word_fuel: std::num::Saturating(word_fuel),
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

    pub fn max_active_rules(&self) -> usize {
        self.max_active_rules as usize
    }

    pub fn max_pin_actions(&self) -> usize {
        self.max_pin_actions as usize
    }
}

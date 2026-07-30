#![warn(missing_docs)]

use heed::{RoTxn, WithoutTls};
use roaring::RoaringBitmap;
use time::OffsetDateTime;

use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::search::facet::ascending_facet_sort;
use crate::search::new::LocatedQueryTerm;
use crate::search::{Pin, ScaleDocs};
use crate::update::new::document::DocumentFromDb;
use crate::{
    AscDesc, DocumentId, FieldsIdsMap, Index, IndexFilter, PinDoc, Result, SearchContext,
    SearchResult, UserError,
};

mod action;
mod condition;
mod fuel;
mod upgrade;

/// Fields used in DSR documents
pub mod fields;

pub use action::{PinAction, RuleActions, ScaleAction};
pub use fuel::DsrFuel;
pub use upgrade::{create_metadata, upgrade_dsrs, METADATA_UID};

/// Internal identifier of a rule.
///
/// - Roaring bitmaps reference rules by this id.
/// - Not to be confused with a rule uid, which is a user-provided string uniquely naming the rule.
/// - Internal identifiers are not expected to remain stable across indexing and
///
/// See also [`DocumentId`].
pub type RuleId = u32;

/// Wrapper around the DSR index.
///
/// Allows to:
/// - List rules.
/// - Find applicable actions for a given query.
///
/// If you don't want to relinquish ownership of a transaction and/or an index, use [`DynamicSearchRulesView`].
///
/// # Metadata
///
/// The DSR index includes a special "metadata" document. The implementation ensures that it is not returned as a normal rule.
/// This document is responsible to hold the internal schema version, and is updated on version upgrades.
/// It should also be created when creating the index (see [`create_metadata`]).
pub struct DynamicSearchRules {
    index: Index,
    rtxn: RoTxn<'static, WithoutTls>,
    db_fields_ids_map: FieldsIdsMap,
}

/// Wrapper around the DSR index.
///
/// Allows to:
/// - List rules.
/// - Find applicable actions for a given query.
///
/// If you want to store this view in a struct without a lifetime, consider [`DynamicSearchRules`].
///
/// # Metadata
///
/// The DSR index includes a special "metadata" document. The implementation ensures that it is not returned as a normal rule.
/// This document is responsible to hold the internal schema version, and is updated on version upgrades.
/// It should also be created when creating the index (see [`create_metadata`]).
#[derive(Clone, Copy)]
pub struct DynamicSearchRulesView<'a> {
    index: &'a Index,
    rtxn: &'a RoTxn<'a, WithoutTls>,
    db_fields_ids_map: &'a FieldsIdsMap,
}

impl<'a> DynamicSearchRulesView<'a> {
    /// Creates a view around an index and rtxn.
    pub fn new(
        index: &'a Index,
        rtxn: &'a RoTxn<'a, WithoutTls>,
        db_fields_ids_map: &'a FieldsIdsMap,
    ) -> Self {
        Self { index, rtxn, db_fields_ids_map }
    }

    fn metadata_internal_id(self) -> Result<Option<DocumentId>> {
        Ok(self.index.external_documents_ids().get(self.rtxn, METADATA_UID)?)
    }

    /// Get the raw representation of a rule from its UID.
    pub fn get(self, rule_uid: &str) -> Result<Option<DocumentFromDb<'a, FieldsIdsMap>>> {
        if rule_uid == METADATA_UID {
            return Ok(None);
        }

        let Some(docid) = self.index.external_documents_ids().get(self.rtxn, rule_uid)? else {
            return Ok(None);
        };

        self.get_from_internal_id(docid)
    }

    /// Get the raw representation of a rule or the index metadata, from its internal ID.
    pub fn get_from_internal_id(
        self,
        rule_or_metadata_id: DocumentId,
    ) -> Result<Option<DocumentFromDb<'a, FieldsIdsMap>>> {
        let Some(doc) = DocumentFromDb::new(
            rule_or_metadata_id,
            self.rtxn,
            self.index,
            self.db_fields_ids_map,
        )?
        else {
            return Ok(None);
        };

        Ok(Some(doc))
    }

    /// Resolve the applicable actions for the given query.
    pub fn resolve_actions(
        &self,
        query_terms: &[LocatedQueryTerm],
        filter: Option<&IndexFilter>,
        universe: &mut RoaringBitmap,
        search_context: &SearchContext,
        fuel: DsrFuel,
    ) -> Result<(Vec<PinDoc>, Vec<ScaleDocs>)> {
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

    /// Provide access to the raw rule representation from an iterator of rule internal ids.
    pub fn rules_from_rule_ids<I>(
        self,
        rule_ids: I,
    ) -> Result<impl Iterator<Item = Result<DocumentFromDb<'a, FieldsIdsMap>>>>
    where
        I: IntoIterator<Item = RuleId>,
    {
        let metadata_id = self.metadata_internal_id()?;

        Ok(rule_ids.into_iter().filter(move |rule_id| Some(*rule_id) != metadata_id).map(
            move |rule_id| {
                self.get_from_internal_id(rule_id)
                    .transpose()
                    .ok_or(UserError::UnknownInternalDocumentId { document_id: rule_id }.into())
                    .flatten()
            },
        ))
    }

    /// Find the list of active or inactive rules, depending on `is_active`.
    ///
    /// If no rule contains the "active" field, then all declared rules are considered active.
    pub fn active_rule_ids(&self, is_active: bool) -> Result<RoaringBitmap> {
        let left_bound = if is_active { "true" } else { "false" };
        let mut active_rules = if let Some(active_fid) = self.db_fields_ids_map.id(fields::ACTIVE) {
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
        if let Some(metadata_id) = self.metadata_internal_id()? {
            active_rules.remove(metadata_id);
        }
        Ok(active_rules)
    }

    /// A bitmap of all the rule internal ids.
    ///
    /// The internal ID of the special "metadata" document is excluded from the returned list.
    pub fn all_rule_ids(&self) -> Result<RoaringBitmap> {
        let mut all_rules = self.index.documents_ids(self.rtxn)?;
        if let Some(metadata_id) = self.metadata_internal_id()? {
            all_rules.remove(metadata_id);
        }
        Ok(all_rules)
    }

    /// Performs a search query against the rules specified in `universe` according to the following parameters:
    ///
    /// - `query`: String to look for (with a `Last` word matching strategy) in description and `query.words`
    /// - `universe`: List of internal rule ids. It must not contain the internal id for the special "metadata" document.
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
}

impl DynamicSearchRules {
    /// Creates a new wrapper around the DSR index.
    pub fn new(index: Index) -> Result<Self> {
        let rtxn = index.static_read_txn()?;

        let db_fields_ids_map = index.fields_ids_map(&rtxn)?;
        Ok(Self { index, rtxn, db_fields_ids_map })
    }

    /// Returns the corresponding non-owning view.
    pub fn as_view(&self) -> DynamicSearchRulesView<'_> {
        DynamicSearchRulesView {
            index: &self.index,
            rtxn: &self.rtxn,
            db_fields_ids_map: &self.db_fields_ids_map,
        }
    }

    /// Returns the raw components making up this object.
    pub fn as_raw(&self) -> (&Index, &RoTxn<'static, WithoutTls>, &FieldsIdsMap) {
        (&self.index, &self.rtxn, &self.db_fields_ids_map)
    }

    /// Get the raw representation of a rule from its uid.
    pub fn get<'t>(&'t self, rule_uid: &str) -> Result<Option<DocumentFromDb<'t, FieldsIdsMap>>> {
        self.as_view().get(rule_uid)
    }

    /// Get the raw representation of a rule from its internal id.
    pub fn get_from_internal_id<'t>(
        &'t self,
        rule_id: RuleId,
    ) -> Result<Option<DocumentFromDb<'t, FieldsIdsMap>>> {
        self.as_view().get_from_internal_id(rule_id)
    }

    /// Resolve the applicable actions for the given query.
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

    /// Provide access to the raw rule representation from an iterator of rule internal ids.
    pub fn rules_from_rule_ids<'t, I>(
        &'t self,
        rule_ids: I,
    ) -> Result<impl Iterator<Item = Result<DocumentFromDb<'t, FieldsIdsMap>>>>
    where
        I: IntoIterator<Item = RuleId>,
    {
        self.as_view().rules_from_rule_ids(rule_ids)
    }

    /// Find the list of active or inactive rules, depending on `is_active`.
    ///
    /// If no rule contains the "active" field, then all declared rules are considered active.
    pub fn active_rule_ids(&self, is_active: bool) -> Result<RoaringBitmap> {
        self.as_view().active_rule_ids(is_active)
    }

    /// A bitmap of all the rule internal ids.
    ///
    /// The internal ID of the special "metadata" document is excluded from the returned list.
    pub fn all_rule_ids(&self) -> Result<RoaringBitmap> {
        self.as_view().all_rule_ids()
    }

    /// Performs a search query against the rules specified in `universe` according to the following parameters:
    ///
    /// - `query`: String to look for (with a `Last` word matching strategy) in description and `query.words`
    /// - `universe`: List of internal rule ids. It must not contain the internal id for the special "metadata" document.
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

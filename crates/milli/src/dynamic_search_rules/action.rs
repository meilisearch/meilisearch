use std::ops::Not as _;

use roaring::RoaringBitmap;

use crate::dynamic_search_rules::{fields, DsrFuel, DynamicSearchRulesView, RuleId};
use crate::search::{Precedence, ScaleDocs};
use crate::update::new::document::DocumentFromDb;
use crate::{DocumentId, Filter, IndexFilter, PinDoc, Result, SearchContext};

impl<'a> DynamicSearchRulesView<'a> {
    fn find_pin_actions(
        precedence: Precedence,
        actions: Vec<PinAction>,
        search_context: &'a SearchContext,
        rule_uid: String,
    ) -> impl Iterator<Item = Result<PinDoc>> + 'a {
        actions.into_iter().filter_map(move |action| {
            let doc_id = action.active_document(search_context).transpose()?;

            let doc_id = match doc_id {
                Ok(doc_id) => doc_id,
                Err(err) => return Some(Err(err)),
            };

            Some(Ok(PinDoc {
                position: action.position,
                precedence,
                id: doc_id,
                rule_uid: rule_uid.clone(),
            }))
        })
    }

    fn find_scale_actions(
        actions: Vec<ScaleAction>,
        search_context: &'a SearchContext,
        rule_uid: String,
    ) -> impl Iterator<Item = Result<ScaleDocs>> + 'a {
        actions.into_iter().filter_map(move |action| {
            let docs = action.active_documents(search_context).transpose()?;

            let docs = match docs {
                Ok(docs) => docs,
                Err(err) => return Some(Err(err)),
            };

            Some(Ok(ScaleDocs { docs, weight: action.weight, rule_uid: rule_uid.clone() }))
        })
    }

    pub(super) fn find_actions(
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

                let Some(raw_rule_uid) = rule.field(fields::UID)? else {
                    tracing::warn!(
                        "Could not find field `uid` for rule with internal id `{rule_id}`"
                    );
                    return Ok(None);
                };

                let rule_uid : Result<String, serde_json::Error> = serde_json::from_str(raw_rule_uid.get());
                let rule_uid = match rule_uid {
                    Ok(rule_uid) => rule_uid,
                    Err(err) => {
                        tracing::warn!("Could not deserialize field `uid` (raw value: `{}`) for rule with internal id `{rule_id}`: {err}", raw_rule_uid.get());
                        return Ok(None);
                    }
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
                        "could not deserialize precedence of rule with internal id `{rule_id}`: {err}"
                    );
                        return Ok(None);
                    }
                };

                let actions: Result<RuleActions, serde_json::Error> =
                    serde_json::from_str(actions.get());
                match actions {
                    Ok(actions) => Ok(Some((
                        Self::find_pin_actions(Precedence(precedence), actions.pin, search_context, rule_uid.clone()),
                        Self::find_scale_actions(actions.scale, search_context, rule_uid),
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
}

/// List of actions to apply when this rule is active for the query.
#[routes::request(proxied, db, setting, no_error)]
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RuleActions {
    /// Pins a selected document.
    #[request(default, skip_serializing_if = "Vec::is_empty")]
    pub pin: Vec<PinAction>,
    /// Applies a multiplicative factor to the score of selected documents.
    #[request(default, skip_serializing_if = "Vec::is_empty")]
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
    #[request(default, skip_serializing_if = "Option::is_none")]
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
    fn active_document(&self, search_context: &SearchContext<'_>) -> Result<Option<DocumentId>> {
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
    #[request(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<String>,
    /// Array of specific document IDs to select.
    ///
    /// Only documents whose [primary key](https://www.meilisearch.com/docs/learn/getting_started/primary_key) value
    /// matches the specified ids will be selected by the action.
    ///
    /// If `filter` is also specified,
    /// the documents must also satisfy the filter to be selected.
    #[request(default, skip_serializing_if = "Option::is_none")]
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
    #[request(default, skip_serializing_if = "Option::is_none")]
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
    fn active_documents(
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
        candidates.insert(id);
    }
    Ok(candidates)
}

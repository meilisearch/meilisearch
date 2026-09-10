use serde::Deserializer;

use super::metadata::Metadata;
use crate::documents::DocumentIdExtractionError;
use crate::dynamic_search_rules::{fields, DynamicSearchRulesView, PinAction, RuleActions};
use crate::update::new::document::{Document, Versions};
use crate::update::new::indexer::de::DocumentIdVisitor;
use crate::update::new::indexer::DocumentUpdater;
use crate::update::new::{DocumentIdentifiers, Update};
use crate::{DocumentId, Result};

pub(super) struct DsrActionsUpdater {
    metadata_docid: DocumentId,
}
impl DsrActionsUpdater {
    pub fn new(metadata_docid: DocumentId) -> Self {
        Self { metadata_docid }
    }
}

impl DocumentUpdater for DsrActionsUpdater {
    fn update<'doc, T, D>(
        &self,
        context: &'doc crate::update::new::document::DocumentContext<T>,
        docid: u32,
        current: D,
    ) -> Result<Option<crate::update::new::DocumentChange<'doc>>>
    where
        T: crate::update::new::thread_local::MostlySend + 'doc,
        D: Document<'doc>,
    {
        if self.metadata_docid == docid {
            return Ok(None);
        }

        let Some(actions) = current.top_level_field(fields::ACTIONS)? else {
            return Ok(None);
        };

        let Some(external_docid) = current.top_level_field(fields::UID)? else {
            tracing::warn!("No external id for rule with internal id `{docid}`");
            return Ok(None);
        };

        let external_docid =
            match external_docid.deserialize_any(DocumentIdVisitor(&context.doc_alloc)) {
                Ok(Ok(external_docid)) => external_docid,
                Ok(Err(DocumentIdExtractionError::InvalidDocumentId(err))) => {
                    tracing::warn!(
                    "Could not deserialize external id for rule with internal id `{docid}`: {err}"
                );

                    return Ok(None);
                }
                Err(err) => {
                    tracing::warn!(
                    "Could not deserialize external id for rule with internal id `{docid}`: {err}"
                );

                    return Ok(None);
                }
                Ok(Err(_)) => {
                    tracing::warn!(
                        "Could not deserialize external id for rule with internal id `{docid}`"
                    );

                    return Ok(None);
                }
            };

        let external_docid = external_docid.to_de();

        let actions: Result<Vec<super::v1_53::RuleAction>, _> = serde_json::from_str(actions.get());
        let actions = match actions {
            Ok(actions) => actions,
            Err(err) => {
                tracing::warn!("Could not recover action for rule `{external_docid}`: {err}");
                return Ok(None);
            }
        };

        let pin: Vec<_> = actions
            .into_iter()
            .map(|action| {
                let super::v1_53::DynamicSearchRuleAction::Pin { position } = action.action;
                let index_uid = action.selector.index_uid;
                let id = action.selector.id;

                PinAction { index_uid, id, position }
            })
            .collect();

        let actions = RuleActions { pin, scale: Default::default() };

        let mut update = Versions::empty(&context.doc_alloc);
        if let Err(err) = update.insert_top_level_field_value(fields::ACTIONS, &actions) {
            tracing::error!(
                "Could not update actions field for rule `{external_docid}`: {err}. Deleting rule"
            );
            return Ok(Some(crate::update::new::DocumentChange::Deletion(
                DocumentIdentifiers::create(docid, external_docid),
            )));
        }

        Ok(Some(crate::update::new::DocumentChange::Update(Update::create(
            docid,
            external_docid,
            update,
            false,
        ))))
    }
}

impl<'a> DynamicSearchRulesView<'a> {
    pub(super) fn metadata(self, metadata_docid: DocumentId) -> Result<Option<Metadata>> {
        let Some(doc) = self.get_from_internal_id(metadata_docid)? else { return Ok(None) };
        Ok(Some(Metadata::from_doc(doc)?))
    }
}

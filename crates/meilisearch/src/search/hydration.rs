use std::collections::BTreeSet;

use index_scheduler::IndexScheduler;
use meilisearch_types::{
    error::ResponseError,
    heed::RoTxn,
    milli::{
        self, progress::Progress, ExternalDocumentsIds, FederatingResultsStep, FieldId,
        FieldsIdsMap, ForeignKey,
    },
    Index,
};
use permissive_json_pointer::map_leaf_values;
use serde_json::{Map, Value};

use crate::search::{federated::SearchHitByIndex, make_document, ExternalDocumentId, SearchHit};

/// Hydrate the documents based on the foreign keys
///
/// This function will walk the document and hydrate the foreign key values with the full document from the foreign index using the displayed fields.
/// If a foreign key value is not a valid document id, we warn and skip the document
pub fn hydrate_federated_documents(
    documents: &mut Vec<SearchHitByIndex>,
    foreign_keys: &[ForeignKey],
    index_scheduler: &IndexScheduler,
    progress: &Progress,
) -> Result<(), ResponseError> {
    progress.update_progress_scoped(FederatingResultsStep::HydrateDocuments);
    hydrate_documents_(documents, foreign_keys, index_scheduler, |document| {
        &mut document.hit.document
    })
}

/// Hydrate the documents based on the foreign keys
///
/// This function will walk the document and hydrate the foreign key values with the full document from the foreign index using the displayed fields.
/// If a foreign key value is not a valid document id, we warn and skip the document
pub fn hydrate_documents(
    documents: &mut Vec<SearchHit>,
    foreign_keys: &[ForeignKey],
    index_scheduler: &IndexScheduler,
    progress: &Progress,
) -> Result<(), ResponseError> {
    progress.update_progress_scoped(FederatingResultsStep::HydrateDocuments);
    hydrate_documents_(documents, foreign_keys, index_scheduler, |document| &mut document.document)
}

fn hydrate_documents_<D, F>(
    documents: &mut Vec<D>,
    foreign_keys: &[ForeignKey],
    index_scheduler: &IndexScheduler,
    get_document: F,
) -> Result<(), ResponseError>
where
    F: Fn(&mut D) -> &mut Map<String, Value>,
{
    // Open each foreign index once
    for foreign_key in foreign_keys {
        let index_uid = foreign_key.foreign_index_uid.as_str();
        let field_name = foreign_key.field_name.as_str();
        let index = index_scheduler.index(index_uid)?;
        let rtxn = index.read_txn()?;
        let formatter = HydrationFormatter::new(&index, &rtxn, index_uid, field_name)?;

        for document in documents.iter_mut() {
            formatter.hydrate_document(get_document(document))?;
        }
    }

    Ok(())
}

struct HydrationFormatter<'a> {
    index_uid: &'a str,
    index: &'a Index,
    rtxn: &'a RoTxn<'a>,
    external_documents_ids: ExternalDocumentsIds,
    displayed_ids: BTreeSet<FieldId>,
    fields_ids_map: FieldsIdsMap,
    field_name: &'a str,
}

impl<'a> HydrationFormatter<'a> {
    fn new(
        index: &'a Index,
        rtxn: &'a RoTxn<'a>,
        index_uid: &'a str,
        field_name: &'a str,
    ) -> milli::Result<Self> {
        let external_documents_ids = index.external_documents_ids();
        let fields_ids_map = index.fields_ids_map(rtxn)?;

        // If displayed_fields_ids is None, we use all the fields ids present in the fields_ids_map
        let displayed_ids = index.displayed_fields_ids(rtxn)?.map_or_else(
            || fields_ids_map.iter().map(|(id, _)| id).collect(),
            |fields| fields.into_iter().collect::<BTreeSet<_>>(),
        );

        Ok(Self {
            index_uid,
            index,
            rtxn,
            external_documents_ids,
            displayed_ids,
            fields_ids_map,
            field_name,
        })
    }

    /// Make the document from the foreign index using the displayed fields.
    fn make_document(&self, docid: &Value) -> Result<Map<String, Value>, ResponseError> {
        let Ok(external_document_id) = ExternalDocumentId::try_from(docid.clone()) else {
            tracing::warn!("Foreign key value `{docid:?}` is not a valid document id when hydrating field `{}`", self.field_name);
            return Ok(Map::new());
        };
        let Some(id) = self.external_documents_ids.get(self.rtxn, external_document_id)? else {
            tracing::warn!("Foreign key value `{docid:?}` does not match any document id in index `{}` when hydrating field `{}`", self.index_uid, self.field_name);
            return Ok(Map::new());
        };
        let (_, obkv) =
            self.index.iter_documents(self.rtxn, std::iter::once(id))?.next().unwrap()?;

        make_document(&self.displayed_ids, &self.fields_ids_map, obkv).map_err(ResponseError::from)
    }

    /// Replace the foreign key value with the full document from the foreign index using the displayed fields.
    fn hydrate_document_value(&self, value: &mut Value) -> Result<(), ResponseError> {
        match value {
            value @ Value::String(_) => {
                let document = self.make_document(value)?;
                *value = Value::Object(document);
            }
            value @ Value::Number(_) => {
                let document = self.make_document(value)?;
                *value = Value::Object(document);
            }
            Value::Array(values) => {
                for value in values {
                    self.hydrate_document_value(value)?;
                }
            }
            _ => tracing::warn!("Foreign key value is not a string or number: {value:?}"),
        }
        Ok(())
    }

    fn hydrate_document(&self, document: &mut Map<String, Value>) -> Result<(), ResponseError> {
        let mut res = Ok(());
        map_leaf_values(document, [self.field_name], |_key, _array_indices, value| {
            if res.is_ok() {
                res = self.hydrate_document_value(value);
            }
        });

        res
    }
}

use std::collections::{BTreeMap, BTreeSet, HashMap};

use index_scheduler::IndexScheduler;
use meilisearch_types::{
    error::ResponseError,
    heed::RoTxn,
    milli::{self, ExternalDocumentsIds, FieldId, FieldsIdsMap, ForeignKey},
    Index,
};
use permissive_json_pointer::{map_leaf_values, select_values};
use serde_json::{Map, Value};

use crate::search::{make_document, ExternalDocumentId, SearchHit};

/// Hydrate the documents based on the foreign keys
///
/// This function will walk the document and hydrate the foreign key values with the full document from the foreign index using the displayed fields.
/// If a foreign key value is not a valid document id, we warn and skip the document
pub fn hydrate_documents(
    documents: &mut [SearchHit],
    foreign_keys: &[ForeignKey],
    index_scheduler: &IndexScheduler,
) -> Result<(), ResponseError> {
    // Open each foreign index once
    for ForeignKey { foreign_index_uid, field_name } in foreign_keys {
        let index = index_scheduler.index(foreign_index_uid)?;
        let rtxn = index.read_txn()?;
        let formatter = HydrationFormatter::new(&index, &rtxn, field_name)?;

        for document in documents.iter_mut() {
            formatter.hydrate_document(&mut document.document)?;
            formatter.hydrate_document(&mut document.formatted)?;
        }
    }

    Ok(())
}

struct HydrationFormatter<'a> {
    document_maker: IndexDocumentMaker<'a>,
    field_name: &'a str,
}

impl<'a> HydrationFormatter<'a> {
    fn new(index: &'a Index, rtxn: &'a RoTxn<'a>, field_name: &'a str) -> milli::Result<Self> {
        let document_maker = IndexDocumentMaker::new(index, rtxn)?;

        Ok(Self { document_maker, field_name })
    }

    /// Replace the foreign key value with the full document from the foreign index using the displayed fields.
    fn hydrate_document_value(&self, value: &mut Value) -> Result<(), ResponseError> {
        let Ok(external_document_id) = ExternalDocumentId::try_from(value.clone()) else {
            tracing::warn!("Foreign key value `{value:?}` is not a valid document id when hydrating field `{}`", self.field_name);
            return Ok(());
        };
        let document = self.document_maker.make_document(&external_document_id)?;
        *value = Value::Object(document);

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

struct IndexDocumentMaker<'a> {
    index: &'a Index,
    rtxn: &'a RoTxn<'a>,
    external_documents_ids: ExternalDocumentsIds,
    displayed_ids: BTreeSet<FieldId>,
    fields_ids_map: FieldsIdsMap,
}

impl<'a> IndexDocumentMaker<'a> {
    fn new(index: &'a Index, rtxn: &'a RoTxn<'a>) -> milli::Result<Self> {
        let external_documents_ids = index.external_documents_ids();
        let fields_ids_map = index.fields_ids_map(rtxn)?;

        // If displayed_fields_ids is None, we use all the fields ids present in the fields_ids_map
        let displayed_ids = index.displayed_fields_ids(rtxn)?.map_or_else(
            || fields_ids_map.iter().map(|(id, _)| id).collect(),
            |fields| fields.into_iter().collect::<BTreeSet<_>>(),
        );

        Ok(Self { index, rtxn, external_documents_ids, displayed_ids, fields_ids_map })
    }

    /// Make the document from the foreign index using the displayed fields.
    fn make_document(
        &self,
        external_document_id: &ForeignExternalDocumentId,
    ) -> Result<Map<String, Value>, ResponseError> {
        let Some(id) = self.external_documents_ids.get(self.rtxn, external_document_id)? else {
            tracing::warn!(
                "Foreign key value `{external_document_id:?}` does not match any document id"
            );
            return Ok(Map::new());
        };
        let (_, obkv) =
            self.index.iter_documents(self.rtxn, std::iter::once(id))?.next().unwrap()?;

        make_document(&self.displayed_ids, &self.fields_ids_map, obkv).map_err(ResponseError::from)
    }
}

pub type ForeignIndexUid = String;
pub type SourceIndexUid = String;
pub type ForeignExternalDocumentId = ExternalDocumentId;
pub struct HydrationCache {
    // list of indexes in the order of the queries
    index_by_query_index: Vec<SourceIndexUid>,
    // map from index uid to foreign keys
    hydration_settings: BTreeMap<SourceIndexUid, Vec<ForeignKey>>,
    // map from foreign index uid to foreign document ids
    // TODO Document join: add remote name to the key when implementing network support
    hydration_docids: BTreeMap<ForeignIndexUid, Vec<ForeignExternalDocumentId>>,
}

impl HydrationCache {
    pub fn new(index_by_query_index: impl IntoIterator<Item = SourceIndexUid>) -> Self {
        let index_by_query_index = index_by_query_index.into_iter().collect();
        Self {
            index_by_query_index,
            hydration_settings: BTreeMap::new(),
            hydration_docids: BTreeMap::new(),
        }
    }

    pub fn register_foreign_settings(
        &mut self,
        index_uid: &ForeignIndexUid,
        foreign_keys: &[ForeignKey],
    ) {
        self.hydration_settings.insert(index_uid.to_string(), foreign_keys.to_vec());
    }

    pub fn register_foreign_docids(&mut self, hit: &SearchHit, query_index: usize) {
        let index_uid = &self.index_by_query_index[query_index];
        let Some(foreign_keys) = self.hydration_settings.get(index_uid) else {
            // TODO Document join: when implementing network support, fallback on seeking the foreign keys in the index settings
            return;
        };

        for ForeignKey { foreign_index_uid, field_name } in foreign_keys {
            match select_values(&hit.document, [field_name.as_str()]).get(field_name.as_str()) {
                Some(Value::Array(values)) => {
                    for value in values {
                        let Ok(external_document_id) = ExternalDocumentId::try_from(value.clone())
                        else {
                            tracing::warn!(
                                "Foreign key value `{value:?}` is not a valid document id in `{field_name}`"
                            );
                            return;
                        };
                        self.hydration_docids
                            .entry(foreign_index_uid.clone())
                            .or_default()
                            .push(external_document_id);
                    }
                }
                Some(value) => {
                    let Ok(external_document_id) = ExternalDocumentId::try_from(value.clone())
                    else {
                        tracing::warn!(
                            "Foreign key value `{value:?}` is not a valid document id in `{field_name}`"
                        );
                        return;
                    };
                    self.hydration_docids
                        .entry(foreign_index_uid.clone())
                        .or_default()
                        .push(external_document_id);
                }
                None => {}
            }
        }
    }
}

pub struct FederatedHydrationFormatter {
    // list of indexes in the order of the queries
    index_by_query_index: Vec<SourceIndexUid>,
    // map from index uid to foreign keys
    hydration_settings: BTreeMap<SourceIndexUid, Vec<ForeignKey>>,
    // map from foreign index uid and foreign document id to document
    hydration_documents: HashMap<(ForeignIndexUid, ForeignExternalDocumentId), Map<String, Value>>,
}

impl FederatedHydrationFormatter {
    pub fn new(
        hydration_cache: HydrationCache,
        index_scheduler: &IndexScheduler,
    ) -> Result<Self, ResponseError> {
        let HydrationCache { index_by_query_index, hydration_settings, hydration_docids } =
            hydration_cache;

        // Fetch the documents from the foreign indexes
        let mut hydration_documents = HashMap::new();
        for (index_uid, docids) in hydration_docids {
            let index = index_scheduler.index(&index_uid)?;
            let rtxn = index.read_txn()?;
            let document_maker = IndexDocumentMaker::new(&index, &rtxn)?;
            for docid in docids {
                let document = document_maker.make_document(&docid)?;
                hydration_documents.insert((index_uid.clone(), docid), document);
            }
        }

        Ok(Self { index_by_query_index, hydration_settings, hydration_documents })
    }

    pub fn hydrate_documents(
        &self,
        documents: &mut [(usize, SearchHit)],
    ) -> Result<(), ResponseError> {
        for (query_index, document) in documents.iter_mut() {
            let index_uid = &self.index_by_query_index[*query_index];
            let Some(foreign_keys) = self.hydration_settings.get(index_uid) else {
                // No foreign keys for this index, skip
                continue;
            };

            // Hydrate the document
            for ForeignKey { foreign_index_uid, field_name } in foreign_keys {
                map_leaf_values(
                    &mut document.document,
                    [field_name.as_str()],
                    |key, _array_indices, value| {
                        self.hydrate_document_value(key, value, foreign_index_uid);
                    },
                );
            }

            // Hydrate the formatted document
            for ForeignKey { foreign_index_uid, field_name } in foreign_keys {
                map_leaf_values(
                    &mut document.formatted,
                    [field_name.as_str()],
                    |key, _array_indices, value| {
                        self.hydrate_document_value(key, value, foreign_index_uid);
                    },
                );
            }
        }
        Ok(())
    }

    /// Replace the foreign key value with the full document from the cache.
    fn hydrate_document_value(&self, key: &str, value: &mut Value, index_uid: &ForeignIndexUid) {
        let Ok(external_document_id) = ExternalDocumentId::try_from(value.clone()) else {
            tracing::warn!("Foreign key value `{value:?}` is not a valid document id in `{key}`");
            return;
        };
        let Some(document) =
            self.hydration_documents.get(&(index_uid.clone(), external_document_id))
        else {
            tracing::warn!(
                "Foreign key value `{value:?}` in `{key}` does not match any document in index `{index_uid}`"
            );
            return;
        };
        *value = Value::Object(document.clone());
    }
}

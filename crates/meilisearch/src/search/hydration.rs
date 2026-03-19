use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::heed::RoTxn;
use meilisearch_types::milli::{self, ExternalDocumentsIds, FieldId, FieldsIdsMap, ForeignKey};
use meilisearch_types::Index;
use permissive_json_pointer::{map_leaf_values, map_leaf_values_in_object, visit_leaf_values};
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
    // Group the foreign keys by index uid
    let mut foreign_keys_by_index_uid: HashMap<_, Vec<_>> = HashMap::new();
    for ForeignKey { foreign_index_uid, field_name } in foreign_keys {
        foreign_keys_by_index_uid.entry(foreign_index_uid).or_default().push(field_name.as_str());
    }

    // Open each foreign index once
    for (foreign_index_uid, field_names) in foreign_keys_by_index_uid {
        let index = index_scheduler.index(foreign_index_uid)?;
        let rtxn = index.read_txn()?;
        let formatter = HydrationFormatter::new(&index, &rtxn, field_names.as_slice())?;

        for document in documents.iter_mut() {
            formatter.hydrate_document(&mut document.document)?;
            formatter.hydrate_document(&mut document.formatted)?;
        }
    }

    Ok(())
}

struct HydrationFormatter<'a> {
    document_maker: IndexDocumentMaker<'a>,
    field_names: &'a [&'a str],
}

impl<'a> HydrationFormatter<'a> {
    fn new(
        index: &'a Index,
        rtxn: &'a RoTxn<'a>,
        field_names: &'a [&'a str],
    ) -> milli::Result<Self> {
        let document_maker = IndexDocumentMaker::new(index, rtxn)?;

        Ok(Self { document_maker, field_names })
    }

    /// Replace the foreign key value with the full document from the foreign index using the displayed fields.
    fn hydrate_document_value(&self, value: &mut Value) -> Result<(), ResponseError> {
        let Ok(external_document_id) = ExternalDocumentId::try_from(value.clone()) else {
            tracing::warn!("Foreign key value `{value:?}` is not a valid document id when hydrating fields `{:?}`", self.field_names);
            return Ok(());
        };
        let document = self.document_maker.make_document(&external_document_id)?;
        *value = Value::Object(document);

        Ok(())
    }

    fn hydrate_document(&self, document: &mut Map<String, Value>) -> Result<(), ResponseError> {
        let mut res = Ok(());
        map_leaf_values_in_object(
            document,
            self.field_names,
            "",
            &[],
            &mut |_key, _array_indices, value| {
                if res.is_ok() {
                    res = self.hydrate_document_value(value);
                }
            },
        );

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

        let obkv = self.index.document(self.rtxn, id)?;
        let selectors: Vec<_> = self
            .displayed_ids
            .iter()
            .map(|&fid| self.fields_ids_map.name(fid).expect("Missing field name"))
            .collect();

        make_document(obkv, &self.fields_ids_map, &selectors).map_err(ResponseError::from)
    }
}

pub type ForeignIndexUid = Rc<str>;
pub type SourceIndexUid = String;
pub type ForeignExternalDocumentId = ExternalDocumentId;
pub struct HydrationContext {
    // list of indexes in the order of the queries
    index_by_query_index: Vec<SourceIndexUid>,
    // map from index uid to foreign keys
    hydration_settings: HashMap<SourceIndexUid, Vec<(ForeignIndexUid, Rc<str>)>>,
    // map from foreign index uid to foreign document ids
    // TODO Document join: add remote name to the key when implementing network support
    hydration_docids: HashMap<ForeignIndexUid, Vec<ForeignExternalDocumentId>>,
}

impl HydrationContext {
    pub fn new(index_by_query_index: impl IntoIterator<Item = SourceIndexUid>) -> Self {
        let index_by_query_index = index_by_query_index.into_iter().collect();
        Self {
            index_by_query_index,
            hydration_settings: HashMap::new(),
            hydration_docids: HashMap::new(),
        }
    }

    pub fn register_foreign_settings(
        &mut self,
        index_uid: SourceIndexUid,
        foreign_keys: Vec<ForeignKey>,
    ) {
        let foreign_keys = foreign_keys
            .into_iter()
            .map(|ForeignKey { foreign_index_uid, field_name }| {
                (Rc::from(foreign_index_uid.as_str()), Rc::from(field_name.as_str()))
            })
            .collect();
        self.hydration_settings.insert(index_uid, foreign_keys);
    }

    pub fn register_foreign_docids(&mut self, hit: &SearchHit, query_index: usize) {
        let index_uid = &self.index_by_query_index[query_index];
        let Some(foreign_keys) = self.hydration_settings.get(index_uid) else {
            // TODO Document join: when implementing network support, fallback on seeking the foreign keys in the index settings
            return;
        };

        for (foreign_index_uid, field_name) in foreign_keys {
            visit_leaf_values(&hit.document, field_name.as_ref(), &mut |value| match value {
                Value::Array(values) => {
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
                value => {
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
            });
        }
    }
}

pub struct FederatedHydrationFormatter {
    // list of indexes in the order of the queries
    index_by_query_index: Vec<SourceIndexUid>,
    // map from index uid to foreign keys
    hydration_settings: HashMap<SourceIndexUid, Vec<(ForeignIndexUid, Rc<str>)>>,
    // map from foreign index uid and foreign document id to document
    hydration_documents: HashMap<(ForeignIndexUid, ForeignExternalDocumentId), Map<String, Value>>,
}

impl FederatedHydrationFormatter {
    pub fn new(
        hydration_cache: HydrationContext,
        index_scheduler: &IndexScheduler,
    ) -> Result<Self, ResponseError> {
        let HydrationContext { index_by_query_index, hydration_settings, hydration_docids } =
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
            for (foreign_index_uid, field_name) in foreign_keys {
                map_leaf_values(
                    &mut document.document,
                    [field_name.as_ref()],
                    |key, _array_indices, value| {
                        self.hydrate_document_value(key, value, foreign_index_uid);
                    },
                );
            }

            // Hydrate the formatted document
            for (foreign_index_uid, field_name) in foreign_keys {
                map_leaf_values(
                    &mut document.formatted,
                    [field_name.as_ref()],
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

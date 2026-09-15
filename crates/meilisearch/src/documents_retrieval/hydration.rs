use std::collections::{BTreeSet, HashMap};

use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthFilter;
use meilisearch_types::error::ResponseError;
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::{ForeignIndexUid, IndexUid, SourceIndexUid};
use meilisearch_types::milli::{
    self, make_document, ExternalDocumentsIds, FieldId, FieldsIdsMap, ForeignKey,
};
use meilisearch_types::{ForeignKeysPerIndex, Index};
use permissive_json_pointer::{map_leaf_values, map_leaf_values_in_object, visit_leaf_values};
use serde_json::{Map, Value};

use crate::documents_retrieval::preprocessing::{
    fuse_remote_documents, take_document_id_from_federation_hit, take_federation_hit,
};
use crate::documents_retrieval::{RemoteErrors, RemoteRetrieveDocuments};
use crate::routes::indexes::documents::{BrowseQuery, BrowseQueryWithIndex};
use crate::search::federated::types::PreprocessedQuery;
use crate::search::federated::NetworkPartitioner;
use crate::search::proxy::ProxySearchParams;
use crate::search::{ExternalDocumentId, SearchHit};

/// Hydrate the documents based on the foreign keys
///
/// This function will walk the document and hydrate the foreign key values with the full document from the foreign index using the displayed fields.
/// If a foreign key value is not a valid document id, we warn and skip the document
pub fn hydrate_documents(
    documents: &mut [SearchHit],
    foreign_keys: &[ForeignKey],
    index_scheduler: &IndexScheduler,
    auth_filter: &AuthFilter,
) -> Result<(), ResponseError> {
    // Group the foreign keys by index uid
    let mut foreign_keys_by_index_uid: HashMap<_, Vec<_>> = HashMap::new();
    for ForeignKey { foreign_index_uid, field_name } in foreign_keys {
        foreign_keys_by_index_uid.entry(foreign_index_uid).or_default().push(field_name.as_str());
    }

    // Open each foreign index once
    for (foreign_index_uid, field_names) in foreign_keys_by_index_uid {
        let index = index_scheduler
            .user_index(foreign_index_uid, auth_filter)
            .map_err(ResponseError::from)
            .map_err(|mut e| {
                e.message = format!("When trying to open an hydration index: {}", e.message);
                e
            })?;
        let rtxn = index.read_txn()?;
        let fields_ids_map = index.fields_ids_map(&rtxn)?;
        let formatter =
            HydrationFormatter::new(&index, &rtxn, &fields_ids_map, field_names.as_slice())?;

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
        fields_ids_map: &'a FieldsIdsMap,
        field_names: &'a [&'a str],
    ) -> milli::Result<Self> {
        let document_maker = IndexDocumentMaker::new(index, rtxn, fields_ids_map)?;

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
    fields_ids_map: &'a FieldsIdsMap,
}

impl<'a> IndexDocumentMaker<'a> {
    fn new(
        index: &'a Index,
        rtxn: &'a RoTxn<'a>,
        fields_ids_map: &'a FieldsIdsMap,
    ) -> milli::Result<Self> {
        let external_documents_ids = index.external_documents_ids();

        // If displayed_fields_ids is None, we use all the fields ids present in the fields_ids_map
        let displayed_ids = index.displayed_fields_ids(rtxn, fields_ids_map)?.map_or_else(
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

        make_document(obkv, self.fields_ids_map, &selectors).map_err(ResponseError::from)
    }
}

pub type ForeignExternalDocumentId = ExternalDocumentId;
#[derive(Clone)]
pub struct HydrationContext {
    // list of indexes in the order of the queries
    index_by_query_index: Vec<SourceIndexUid>,
    // map from index uid to foreign keys
    hydration_settings: ForeignKeysPerIndex,
    // map from foreign index uid to foreign document ids
    hydration_docids: HashMap<ForeignIndexUid, Vec<ForeignExternalDocumentId>>,
}

impl HydrationContext {
    pub fn new(
        index_by_query_index: Vec<SourceIndexUid>,
        hydration_settings: ForeignKeysPerIndex,
    ) -> Self {
        Self { index_by_query_index, hydration_settings, hydration_docids: HashMap::new() }
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
                                "Foreign key value `{value:?}` is not a valid document id in `{}`",
                                field_name.as_ref()
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
                            "Foreign key value `{value:?}` is not a valid document id in `{}`",
                            field_name.as_ref()
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
    hydration_settings: ForeignKeysPerIndex,
    // map from foreign index uid and foreign document id to document
    hydration_documents: HashMap<(ForeignIndexUid, ForeignExternalDocumentId), Map<String, Value>>,
}

fn local_fetch_hydration_documents(
    index_scheduler: &IndexScheduler,
    index_uid: &ForeignIndexUid,
    docids: &[ForeignExternalDocumentId],
    hydration_documents: &mut HashMap<
        (ForeignIndexUid, ForeignExternalDocumentId),
        Map<String, Value>,
    >,
    auth_filter: &AuthFilter,
) -> Result<(), ResponseError> {
    let index = index_scheduler
        .user_index(index_uid.as_ref(), auth_filter)
        .map_err(ResponseError::from)
        .map_err(|mut e| {
            e.message = format!("When trying to open an hydration index: {}", e.message);
            e
        })?;
    let rtxn = index.read_txn()?;
    let fields_ids_map = index.fields_ids_map(&rtxn)?;
    let document_maker = IndexDocumentMaker::new(&index, &rtxn, &fields_ids_map)?;
    for docid in docids {
        let document = document_maker.make_document(docid)?;
        hydration_documents.insert((index_uid.clone(), docid.clone()), document);
    }

    Ok(())
}

async fn federated_fetch_hydration_documents(
    index_scheduler: &IndexScheduler,
    network_partitioner: &NetworkPartitioner,
    hydration_docids: HashMap<ForeignIndexUid, Vec<ForeignExternalDocumentId>>,
    auth_filter: &AuthFilter,
) -> Result<
    (HashMap<(ForeignIndexUid, ForeignExternalDocumentId), Map<String, Value>>, RemoteErrors),
    ResponseError,
> {
    let params =
        ProxySearchParams::new_with_deadline_from_env(index_scheduler.web_client().clone());

    let mut hydration_documents = HashMap::new();
    let mut remote_queries = Vec::new();
    for (index_uid, docids) in hydration_docids.iter() {
        let index = index_scheduler
            .user_index(index_uid.as_ref(), auth_filter)
            .map_err(ResponseError::from)
            .map_err(|mut e| {
                e.message = format!("When trying to open an hydration index: {}", e.message);
                e
            })?;
        let rtxn = index.read_txn()?;

        let displayed_fields = index.displayed_fields(&rtxn)?;
        let fields = displayed_fields
            .map(|fields| fields.into_iter().map(|field| field.to_string()).collect());
        let ids = docids.iter().map(|docid| Value::String(docid.as_ref().to_string())).collect();
        let query = PreprocessedQuery {
            query: BrowseQueryWithIndex {
                index_uid: IndexUid::new_unchecked(index_uid),
                remote: None,
                query: BrowseQuery {
                    offset: 0,
                    limit: docids.len(),
                    filter: None,
                    fields,
                    retrieve_vectors: false,
                    ids: Some(ids),
                    sort: None,
                    use_network: Some(false),
                },
            },
            filter: None,
        };
        let queries = network_partitioner
            .to_partition(&query)?
            .filter(|query| query.query.remote.as_deref() != network_partitioner.local())
            .map(|query| (index_uid, query));

        remote_queries.extend(queries);
    }

    //remote
    let remote_retrieve_documents =
        RemoteRetrieveDocuments::start(network_partitioner, params, remote_queries).await?;

    // Perform local search
    for (index_uid, docids) in hydration_docids.iter() {
        local_fetch_hydration_documents(
            index_scheduler,
            index_uid,
            docids,
            &mut hydration_documents,
            auth_filter,
        )?;
    }

    // wait
    let (remote_results, errors) = remote_retrieve_documents.finish(index_scheduler).await?;

    // Merge results
    for (index_uid, documents) in fuse_remote_documents(remote_results) {
        for mut document in documents {
            let mut federation_hit = take_federation_hit(&mut document);
            let external_docid = take_document_id_from_federation_hit(&mut federation_hit);
            hydration_documents.insert((index_uid.clone(), external_docid), document);
        }
    }

    let remote_errors = errors
        .into_iter()
        .map(|(index_uid, mut error)| {
            // Add a context to the error message
            error.message = format!("During Hydration: {}", error.message);
            (index_uid, error)
        })
        .collect();

    Ok((hydration_documents, remote_errors))
}

impl FederatedHydrationFormatter {
    pub async fn new(
        hydration_cache: HydrationContext,
        index_scheduler: &IndexScheduler,
        network_partitioner: &NetworkPartitioner,
        auth_filter: &AuthFilter,
    ) -> Result<(Self, RemoteErrors), ResponseError> {
        let HydrationContext { index_by_query_index, hydration_settings, hydration_docids } =
            hydration_cache;

        // Fetch the documents from the foreign indexes
        let (hydration_documents, remote_errors) = if network_partitioner.sharding() {
            federated_fetch_hydration_documents(
                index_scheduler,
                network_partitioner,
                hydration_docids.clone(),
                auth_filter,
            )
            .await?
        } else {
            let mut hydration_documents = HashMap::new();
            for (index_uid, docids) in hydration_docids {
                local_fetch_hydration_documents(
                    index_scheduler,
                    &index_uid,
                    &docids,
                    &mut hydration_documents,
                    auth_filter,
                )?;
            }

            (hydration_documents, Default::default())
        };

        Ok((Self { index_by_query_index, hydration_settings, hydration_documents }, remote_errors))
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
                "Foreign key value `{value:?}` in `{key}` does not match any document in index `{}`",
                index_uid.as_ref()
            );
            return;
        };
        *value = Value::Object(document.clone());
    }
}

use std::collections::HashMap;

use actix_web::web::Data;
use index_scheduler::filter::{
    condition_to_index_condition, parse_filter, parse_local_index_filter,
};
use index_scheduler::{IndexScheduler, RoFeatures};
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::{
    self, filtered_universe, FederatingResultsStep, Filter, IndexFilter, IndexFilterCondition,
    LightToken, Token, TokenLike,
};
use meilisearch_types::Document;
use serde_json::{Map, Value};

use crate::documents_retrieval::{HydrationContext, RemoteErrors};
use crate::documents_retrieval::{RemoteRetrieveDocuments, WithIndex};
use crate::error::MeilisearchHttpError;
use crate::routes::indexes::documents::{BrowseQuery, BrowseQueryWithIndex, DocumentsResult};
use crate::search::federated::types::{
    PreprocessableQuery, PreprocessedQuery, FEDERATION_EXTERNAL_DOCUMENT_ID, FEDERATION_HIT,
};
use crate::search::federated::NetworkPartitioner;
use crate::search::proxy::ProxySearchParams;
use crate::search::ExternalDocumentId;

/// The maximum number of documents a foreign filter can retrieve per index
///
/// This is to avoid potential performance issues with large foreign filters.
/// If the foreign filter is retrieving too many documents, it will return an error.
const MAX_FOREIGN_FILTER_DOCIDS: u64 = 1000;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ForeignIndexUid(pub IndexUid);

impl std::borrow::Borrow<str> for ForeignIndexUid {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ForeignIndexUid {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceFieldName(pub String);

impl AsRef<str> for SourceFieldName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceIndexUid(pub IndexUid);

impl std::borrow::Borrow<str> for SourceIndexUid {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SourceIndexUid {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

pub type ForeignKeysPerIndex = HashMap<SourceIndexUid, Vec<(ForeignIndexUid, SourceFieldName)>>;

pub async fn preprocess_filters<Q: PreprocessableQuery>(
    index_scheduler: Data<IndexScheduler>,
    network_partitioner: &NetworkPartitioner,
    queries: Vec<Q>,
    features: RoFeatures,
    is_proxy: bool,
    progress: &Progress,
    code: Code,
) -> Result<
    (Option<HydrationContext>, Vec<PreprocessedQuery<Q>>, RemoteErrors),
    (ResponseError, Option<usize>),
> {
    progress.update_progress(FederatingResultsStep::PreprocessFilters);

    // Document join: list of indexes in the order of the queries
    // only create the hydration cache if the foreign keys feature is enabled
    if features.runtime_features().foreign_keys && !is_proxy {
        let foreign_keys_settings = retrieve_foreign_keys_settings(
            &index_scheduler,
            queries.iter().map(|q| SourceIndexUid(q.index_uid().clone())),
        )
        .without_index()?;

        let source_index_uids =
            queries.iter().map(|q| SourceIndexUid(q.index_uid().clone())).collect::<Vec<_>>();

        let (queries, remote_errors) = preprocess_filters_allowing_foreign_keys(
            index_scheduler,
            network_partitioner,
            &foreign_keys_settings,
            queries,
            features,
            progress,
            code,
        )
        .await?;

        let hydration_cache = HydrationContext::new(source_index_uids, foreign_keys_settings);
        Ok((Some(hydration_cache), queries, remote_errors))
    } else {
        preprocess_filters_forbidding_foreign_keys(queries, features, code)
            .map(|queries| (None, queries, Default::default()))
    }
}

fn preprocess_filters_forbidding_foreign_keys<Q: PreprocessableQuery>(
    queries: Vec<Q>,
    features: RoFeatures,
    code: Code,
) -> Result<Vec<PreprocessedQuery<Q>>, (ResponseError, Option<usize>)> {
    let queries: Result<Vec<_>, _> = queries
        .into_iter()
        .enumerate()
        .map(|(query_index, mut query)| {
            let filter = query.filter_field().take();
            filter
                .and_then(|filter| {
                    parse_local_index_filter(&filter, Some(query.index_uid()), features, code)
                        .transpose()
                })
                .transpose()
                .map(|filter| PreprocessedQuery { query, filter })
                .with_index(query_index)
        })
        .collect();
    queries
}

async fn preprocess_filters_allowing_foreign_keys<Q: PreprocessableQuery>(
    index_scheduler: Data<IndexScheduler>,
    network_partitioner: &NetworkPartitioner,
    foreign_keys_settings: &ForeignKeysPerIndex,
    mut queries: Vec<Q>,
    features: RoFeatures,
    progress: &Progress,
    code: Code,
) -> Result<(Vec<PreprocessedQuery<Q>>, RemoteErrors), (ResponseError, Option<usize>)> {
    // parse each query filter and bind them to their respective index
    let filters = queries
        .iter_mut()
        .enumerate()
        .map(|(query_index, query)| match query.filter_field().take() {
            Some(filter) => {
                let filter = parse_filter(&filter, code, features).with_index(query_index)?;

                Ok((SourceIndexUid(query.index_uid().clone()), filter))
            }
            None => Ok((SourceIndexUid(query.index_uid().clone()), None)),
        })
        .collect::<Result<_, (ResponseError, Option<usize>)>>()?;

    // convert the filters to index filters by evaluating the foreign filters
    let (filters, remote_errors) = filters_into_index_filters(
        &index_scheduler,
        network_partitioner,
        filters,
        foreign_keys_settings,
        progress,
    )
    .await
    .without_index()?;

    Ok((
        queries
            .into_iter()
            .zip(filters.into_iter())
            .map(|(query, filter)| PreprocessedQuery { query, filter })
            .collect(),
        remote_errors,
    ))
}

fn extract_foreign_filters(
    filters: &[(SourceIndexUid, Option<Filter>)],
    foreign_keys_per_index: &ForeignKeysPerIndex,
) -> Result<Vec<(SourceIndexUid, ForeignIndexUid, Token, Option<IndexFilter>)>, ResponseError> {
    // list all the foreign filters and check their validity
    let mut foreign_filters = Vec::new();
    for (index_uid, filter) in filters.iter() {
        let Some(filter) = filter else { continue };
        for (fid, op) in filter.condition.list_foreign_filters() {
            // get the foreign keys settings for the index
            let foreign_keys = foreign_keys_per_index.get(index_uid).ok_or(
                milli::Error::UserError(milli::UserError::InvalidFilter(format!(
                    "Index `{}`: Index does not have foreign keys",
                    index_uid.as_ref()
                ))),
            )?;

            // get the foreign index uid for the foreign key
            let (foreign_index_uid, _) = foreign_keys
                .iter()
                .find(|(_f_index, s_fname)| s_fname.as_ref() == fid.fragment())
                .ok_or_else(|| {
                    let error = milli::Error::UserError(milli::UserError::InvalidFilter(format!(
                        "Index `{}`: Field `{}` is not a foreign key",
                        index_uid.as_ref(),
                        fid.fragment()
                    )));
                    milli::Error::from(fid.to_external_error(error))
                })?;

            // convert inner foreign filter into an index filter, throw an error if there is a nested foreign filter
            let index_filter =
                IndexFilter::from(condition_to_index_condition(op.clone(), &mut |_| {
                    let error = milli::Error::UserError(milli::UserError::InvalidFilter(
                        "Nested foreign filters are not supported".to_string(),
                    ));
                    Err(fid.to_external_error(error).into())
                })?);

            foreign_filters.push((
                index_uid.clone(),
                foreign_index_uid.clone(),
                fid.clone(),
                Some(index_filter),
            ));
        }
    }

    Ok(foreign_filters)
}

fn group_foreign_filters_by_foreign_index(
    foreign_filters: &[(SourceIndexUid, ForeignIndexUid, Token, Option<IndexFilter>)],
) -> HashMap<ForeignIndexUid, Vec<usize>> {
    let mut filters_per_foreign_index: HashMap<ForeignIndexUid, Vec<usize>> = HashMap::new();
    for (i, (_, foreign_index_uid, _, _)) in foreign_filters.iter().enumerate() {
        filters_per_foreign_index.entry(foreign_index_uid.clone()).or_default().push(i);
    }
    filters_per_foreign_index
}

async fn local_process_foreign_filters(
    index_scheduler: &Data<IndexScheduler>,
    foreign_filters: &[(SourceIndexUid, ForeignIndexUid, Token, Option<IndexFilter>)],
    progress: &Progress,
) -> Result<Vec<Vec<LightToken>>, ResponseError> {
    let index_scheduler = index_scheduler.clone();
    let foreign_filters = foreign_filters.to_vec();
    let progress = progress.clone();

    tokio::task::spawn_blocking(move || {
    let filters_per_foreign_index = group_foreign_filters_by_foreign_index(&foreign_filters);

    let mut foreign_filters_external_docids = vec![vec![]; foreign_filters.len()];
    // open each foreign index once and process the filters
    for (foreign_index_uid, filter_indices) in filters_per_foreign_index.iter() {
        let foreign_index = index_scheduler.user_index(foreign_index_uid.as_ref())?;
        let foreign_rtxn = foreign_index.read_txn()?;
        let foreign_external_docids = foreign_index.external_documents_ids();
        let fields_ids_map = foreign_index.fields_ids_map(&foreign_rtxn)?;

        // Gather the internal docids for each filter
        let mut filters_internal_docids = Vec::new();
        for filter_index in filter_indices.iter() {
            // Safety (Data oriented): `filter_index` is an index into the `foreign_filters` vector, so it's safe to dereference it
            let (_, _, _, index_filter) = &foreign_filters[*filter_index];

            // filter the foreign index
            let docids =
                filtered_universe(&foreign_index, &foreign_rtxn, &fields_ids_map, index_filter, None, &progress)
                .map_err(|err| MeilisearchHttpError::from_milli(err, Some(foreign_index_uid.as_ref().to_string())))?;

            filters_internal_docids.push(docids);
        }

        // Build the In filter for each filter converting the internal docids to external docids
        //
        // Fetch all the external docids once
        let docids_to_fetch = filters_internal_docids
            .iter()
            .fold(roaring::RoaringBitmap::new(), |bitmap, docids| bitmap | docids);
        if docids_to_fetch.len() > MAX_FOREIGN_FILTER_DOCIDS {
            return Err(milli::Error::UserError(milli::UserError::InvalidFilter(
                format!("Foreign filter is retrieving too many documents, foreign filters can't retrieve more than {MAX_FOREIGN_FILTER_DOCIDS} documents per index"),
            )))?;
        }
        let mut internal_to_external_docids = HashMap::new();
        // TODO: optimize DB scan (linear: EXP-1117)
        for result in foreign_external_docids.iter(&foreign_rtxn)? {
            let (external, internal) = result?;
            if docids_to_fetch.contains(internal) {
                internal_to_external_docids.insert(internal, external.to_string());
            }
        }

        // Build the In filter for each filter
        for (filter_index, docids) in filter_indices.iter().zip(filters_internal_docids.into_iter())
        {
            let inner: Result<Vec<_>, _> = foreign_index.external_id_of(&foreign_rtxn, &fields_ids_map, docids)?.into_iter().map(|id| id.map(|id| id.into())).collect();

            // Safety (Data oriented): `filter_index` is an index into the `foreign_filters_external_docids` vector, so it's safe to dereference it
            foreign_filters_external_docids[*filter_index] = inner?;
        }
    }

    Ok(foreign_filters_external_docids) }).await.map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal)).flatten()
}

async fn federated_process_foreign_filters(
    index_scheduler: &Data<IndexScheduler>,
    partitioner: &NetworkPartitioner,
    foreign_filters: &[(SourceIndexUid, ForeignIndexUid, Token, Option<IndexFilter>)],
    progress: &Progress,
) -> Result<(Vec<Vec<LightToken>>, RemoteErrors), ResponseError> {
    let params =
        ProxySearchParams::new_with_deadline_from_env(index_scheduler.web_client().clone());

    let mut remote_queries = Vec::new();
    for (query_index, (_index_uid, foreign_index_uid, _, index_filter)) in
        foreign_filters.iter().enumerate()
    {
        let query = PreprocessedQuery {
            query: BrowseQueryWithIndex {
                index_uid: IndexUid::new_unchecked(foreign_index_uid),
                remote: None,
                query: BrowseQuery {
                    offset: 0,
                    limit: MAX_FOREIGN_FILTER_DOCIDS as usize,
                    filter: None,
                    fields: Some(vec![]),
                    retrieve_vectors: false,
                    ids: None,
                    sort: None,
                    use_network: Some(false),
                },
            },
            filter: index_filter.clone(),
        };
        let queries = partitioner
            .to_partition(&query)?
            .filter(|query| query.query.remote.as_deref() != partitioner.local())
            .map(|query| (query_index, query));

        remote_queries.extend(queries);
    }

    //remote
    let remote_retrieve_documents =
        RemoteRetrieveDocuments::start(partitioner, params, remote_queries).await?;

    // Perform local search
    let mut foreign_filters_external_docids =
        local_process_foreign_filters(index_scheduler, foreign_filters, progress).await?;

    // wait
    let (remote_results, errors) = remote_retrieve_documents.finish(index_scheduler).await?;

    // Merge results
    for (query_id, documents) in fuse_remote_documents(remote_results) {
        for mut document in documents {
            let mut federation_hit = take_federation_hit(&mut document);
            let external_docid = take_document_id_from_federation_hit(&mut federation_hit);
            foreign_filters_external_docids[query_id]
                .push(LightToken::from(external_docid.into_inner()));
        }
    }

    Ok((
        foreign_filters_external_docids,
        errors
            .into_iter()
            .map(|(index_uid, mut error)| {
                // Add a context to the error message
                error.message = format!("During Foreign Filters Processing: {}", error.message);
                (index_uid, error)
            })
            .collect(),
    ))
}

pub fn take_federation_hit(document: &mut Document) -> Map<String, Value> {
    let Value::Object(federation_hit) =
        document.remove(FEDERATION_HIT).expect("Federation hit must be present")
    else {
        unreachable!()
    };

    federation_hit
}

pub fn take_document_id_from_federation_hit(
    federation_hit: &mut Map<String, Value>,
) -> ExternalDocumentId {
    let external_docid = federation_hit
        .remove(FEDERATION_EXTERNAL_DOCUMENT_ID)
        .expect("External document id must be present");
    ExternalDocumentId::try_from(external_docid).expect("External document id must be a valid")
}

pub fn fuse_remote_documents<T: Ord + Eq>(
    mut results: Vec<(T, DocumentsResult)>,
) -> impl Iterator<Item = (T, Vec<Document>)> {
    results.sort_by(|a, b| a.0.cmp(&b.0));

    let mut merged_results = Vec::new();
    for (fuse_by, results) in results {
        if merged_results.last().is_none_or(|(last_fuse_by, _)| *last_fuse_by != fuse_by) {
            merged_results.push((fuse_by, results.results));
        } else {
            merged_results.last_mut().unwrap().1.extend(results.results);
        }
    }

    merged_results.into_iter()
}

/// Convert a vector of filters into a vector of index filters by evaluating the foreign filters
///
/// This function will open each foreign index once and process the filters.
async fn filters_into_index_filters(
    index_scheduler: &Data<IndexScheduler>,
    network_partitioner: &NetworkPartitioner,
    filters: Vec<(SourceIndexUid, Option<Filter>)>,
    foreign_keys_per_index: &ForeignKeysPerIndex,
    progress: &Progress,
) -> Result<(Vec<Option<IndexFilter>>, RemoteErrors), ResponseError> {
    let foreign_filters = extract_foreign_filters(&filters, foreign_keys_per_index)?;

    // retrieve the external docids executing each foreign filter
    let (foreign_filters_external_docids, remote_errors) = if network_partitioner.sharding() {
        // network + local
        federated_process_foreign_filters(
            index_scheduler,
            network_partitioner,
            &foreign_filters,
            progress,
        )
        .await?
    } else {
        // local
        (
            local_process_foreign_filters(index_scheduler, &foreign_filters, progress).await?,
            Default::default(),
        )
    };

    // build the index filters replacing the foreign filters with a IN filter containing the retrieved external docids
    let mut in_iter = foreign_filters.into_iter().zip(foreign_filters_external_docids.into_iter());
    filters
        .into_iter()
        .map(|(_index_uid, filter)| {
            let Some(filter) = filter else { return Ok(None) };
            condition_to_index_condition(filter.condition, &mut |_| {
                let Some(((_, _, fid, _), els)) = in_iter.next() else { unreachable!() };
                Ok(IndexFilterCondition::In { fid, els })
            })
            .map(|condition| Some(IndexFilter { condition }))
        })
        .collect::<milli::Result<_>>()
        .map_err(|e| e.into())
        .map(|index_filters| (index_filters, remote_errors))
}

/// Retrieve the foreign keys settings for a list of indexes
///
/// This function will open each index once and retrieve the foreign keys settings.
pub fn retrieve_foreign_keys_settings(
    index_scheduler: &IndexScheduler,
    index_uids: impl IntoIterator<Item = SourceIndexUid>,
) -> Result<ForeignKeysPerIndex, ResponseError> {
    let mut foreign_keys_settings = HashMap::new();
    for index_uid in index_uids.into_iter() {
        if foreign_keys_settings.contains_key(index_uid.as_ref()) {
            continue;
        }

        let index = index_scheduler.user_index(index_uid.as_ref())?;
        let rtxn = index.read_txn()?;
        let foreign_keys = index
            .foreign_keys(&rtxn)?
            .into_iter()
            .map(|fk| {
                (
                    ForeignIndexUid(IndexUid::new_unchecked(fk.foreign_index_uid)),
                    SourceFieldName(fk.field_name),
                )
            })
            .collect();
        foreign_keys_settings.insert(index_uid.clone(), foreign_keys);
    }
    Ok(foreign_keys_settings)
}

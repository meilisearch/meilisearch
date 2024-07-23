use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::fs;
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::http::header::{CONTENT_TYPE, USER_AGENT};
use actix_web::HttpRequest;
use byte_unit::Byte;
use index_scheduler::IndexScheduler;
use meilisearch_auth::{AuthController, AuthFilter};
use meilisearch_types::locales::Locale;
use meilisearch_types::InstanceUid;
use once_cell::sync::Lazy;
use regex::Regex;
use segment::message::{Identify, Track, User};
use segment::{AutoBatcher, Batcher, HttpClient};
use serde::Serialize;
use serde_json::{json, Value};
use sysinfo::{Disks, System};
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use super::{
    config_user_id_path, DocumentDeletionKind, DocumentFetchKind, MEILISEARCH_CONFIG_PATH,
};
use crate::analytics::Analytics;
use crate::option::{
    default_http_addr, IndexerOpts, LogMode, MaxMemory, MaxThreads, ScheduleSnapshot,
};
use crate::routes::indexes::documents::{DocumentEditionByFunction, UpdateDocumentsQuery};
use crate::routes::indexes::facet_search::FacetSearchQuery;
use crate::routes::{create_all_stats, Stats};
use crate::search::{
    FacetSearchResult, FederatedSearch, MatchingStrategy, SearchQuery, SearchQueryWithIndex,
    SearchResult, SimilarQuery, SimilarResult, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER,
    DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT,
    DEFAULT_SEMANTIC_RATIO,
};
use crate::Opt;

const ANALYTICS_HEADER: &str = "X-Meilisearch-Client";

/// Write the instance-uid in the `data.ms` and in `~/.config/MeiliSearch/path-to-db-instance-uid`. Ignore the errors.
fn write_user_id(db_path: &Path, user_id: &InstanceUid) {
    let _ = fs::write(db_path.join("instance-uid"), user_id.to_string());
    if let Some((meilisearch_config_path, user_id_path)) =
        MEILISEARCH_CONFIG_PATH.as_ref().zip(config_user_id_path(db_path))
    {
        let _ = fs::create_dir_all(meilisearch_config_path);
        let _ = fs::write(user_id_path, user_id.to_string());
    }
}

const SEGMENT_API_KEY: &str = "P3FWhhEsJiEDCuEHpmcN9DHcK4hVfBvb";

pub fn extract_user_agents(request: &HttpRequest) -> Vec<String> {
    request
        .headers()
        .get(ANALYTICS_HEADER)
        .or_else(|| request.headers().get(USER_AGENT))
        .and_then(|header| header.to_str().ok())
        .unwrap_or("unknown")
        .split(';')
        .map(str::trim)
        .map(ToString::to_string)
        .collect()
}

pub enum AnalyticsMsg {
    BatchMessage(Track),
    AggregateGetSearch(SearchAggregator),
    AggregatePostSearch(SearchAggregator),
    AggregateGetSimilar(SimilarAggregator),
    AggregatePostSimilar(SimilarAggregator),
    AggregatePostMultiSearch(MultiSearchAggregator),
    AggregatePostFacetSearch(FacetSearchAggregator),
    AggregateAddDocuments(DocumentsAggregator),
    AggregateDeleteDocuments(DocumentsDeletionAggregator),
    AggregateUpdateDocuments(DocumentsAggregator),
    AggregateEditDocumentsByFunction(EditDocumentsByFunctionAggregator),
    AggregateGetFetchDocuments(DocumentsFetchAggregator),
    AggregatePostFetchDocuments(DocumentsFetchAggregator),
}

pub struct SegmentAnalytics {
    instance_uid: InstanceUid,
    sender: Sender<AnalyticsMsg>,
    user: User,
}

impl SegmentAnalytics {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        opt: &Opt,
        index_scheduler: Arc<IndexScheduler>,
        auth_controller: Arc<AuthController>,
    ) -> Arc<dyn Analytics> {
        let instance_uid = super::find_user_id(&opt.db_path);
        let first_time_run = instance_uid.is_none();
        let instance_uid = instance_uid.unwrap_or_else(Uuid::new_v4);
        write_user_id(&opt.db_path, &instance_uid);

        let client = reqwest::Client::builder().connect_timeout(Duration::from_secs(10)).build();

        // if reqwest throws an error we won't be able to send analytics
        if client.is_err() {
            return super::MockAnalytics::new(opt);
        }

        let client =
            HttpClient::new(client.unwrap(), "https://telemetry.meilisearch.com".to_string());
        let user = User::UserId { user_id: instance_uid.to_string() };
        let mut batcher = AutoBatcher::new(client, Batcher::new(None), SEGMENT_API_KEY.to_string());

        // If Meilisearch is Launched for the first time:
        // 1. Send an event Launched associated to the user `total_launch`.
        // 2. Batch an event Launched with the real instance-id and send it in one hour.
        if first_time_run {
            let _ = batcher
                .push(Track {
                    user: User::UserId { user_id: "total_launch".to_string() },
                    event: "Launched".to_string(),
                    ..Default::default()
                })
                .await;
            let _ = batcher.flush().await;
            let _ = batcher
                .push(Track {
                    user: user.clone(),
                    event: "Launched".to_string(),
                    ..Default::default()
                })
                .await;
        }

        let (sender, inbox) = mpsc::channel(100); // How many analytics can we bufferize

        let segment = Box::new(Segment {
            inbox,
            user: user.clone(),
            opt: opt.clone(),
            batcher,
            post_search_aggregator: SearchAggregator::default(),
            post_multi_search_aggregator: MultiSearchAggregator::default(),
            post_facet_search_aggregator: FacetSearchAggregator::default(),
            get_search_aggregator: SearchAggregator::default(),
            add_documents_aggregator: DocumentsAggregator::default(),
            delete_documents_aggregator: DocumentsDeletionAggregator::default(),
            update_documents_aggregator: DocumentsAggregator::default(),
            edit_documents_by_function_aggregator: EditDocumentsByFunctionAggregator::default(),
            get_fetch_documents_aggregator: DocumentsFetchAggregator::default(),
            post_fetch_documents_aggregator: DocumentsFetchAggregator::default(),
            get_similar_aggregator: SimilarAggregator::default(),
            post_similar_aggregator: SimilarAggregator::default(),
        });
        tokio::spawn(segment.run(index_scheduler.clone(), auth_controller.clone()));

        let this = Self { instance_uid, sender, user: user.clone() };

        Arc::new(this)
    }
}

impl super::Analytics for SegmentAnalytics {
    fn instance_uid(&self) -> Option<&InstanceUid> {
        Some(&self.instance_uid)
    }

    fn publish(&self, event_name: String, mut send: Value, request: Option<&HttpRequest>) {
        let user_agent = request.map(extract_user_agents);

        send["user-agent"] = json!(user_agent);
        let event = Track {
            user: self.user.clone(),
            event: event_name.clone(),
            properties: send,
            ..Default::default()
        };
        let _ = self.sender.try_send(AnalyticsMsg::BatchMessage(event));
    }

    fn get_search(&self, aggregate: SearchAggregator) {
        let _ = self.sender.try_send(AnalyticsMsg::AggregateGetSearch(aggregate));
    }

    fn post_search(&self, aggregate: SearchAggregator) {
        let _ = self.sender.try_send(AnalyticsMsg::AggregatePostSearch(aggregate));
    }

    fn get_similar(&self, aggregate: SimilarAggregator) {
        let _ = self.sender.try_send(AnalyticsMsg::AggregateGetSimilar(aggregate));
    }

    fn post_similar(&self, aggregate: SimilarAggregator) {
        let _ = self.sender.try_send(AnalyticsMsg::AggregatePostSimilar(aggregate));
    }

    fn post_facet_search(&self, aggregate: FacetSearchAggregator) {
        let _ = self.sender.try_send(AnalyticsMsg::AggregatePostFacetSearch(aggregate));
    }

    fn post_multi_search(&self, aggregate: MultiSearchAggregator) {
        let _ = self.sender.try_send(AnalyticsMsg::AggregatePostMultiSearch(aggregate));
    }

    fn add_documents(
        &self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    ) {
        let aggregate = DocumentsAggregator::from_query(documents_query, index_creation, request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregateAddDocuments(aggregate));
    }

    fn delete_documents(&self, kind: DocumentDeletionKind, request: &HttpRequest) {
        let aggregate = DocumentsDeletionAggregator::from_query(kind, request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregateDeleteDocuments(aggregate));
    }

    fn update_documents(
        &self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    ) {
        let aggregate = DocumentsAggregator::from_query(documents_query, index_creation, request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregateUpdateDocuments(aggregate));
    }

    fn update_documents_by_function(
        &self,
        documents_query: &DocumentEditionByFunction,
        index_creation: bool,
        request: &HttpRequest,
    ) {
        let aggregate =
            EditDocumentsByFunctionAggregator::from_query(documents_query, index_creation, request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregateEditDocumentsByFunction(aggregate));
    }

    fn get_fetch_documents(&self, documents_query: &DocumentFetchKind, request: &HttpRequest) {
        let aggregate = DocumentsFetchAggregator::from_query(documents_query, request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregateGetFetchDocuments(aggregate));
    }

    fn post_fetch_documents(&self, documents_query: &DocumentFetchKind, request: &HttpRequest) {
        let aggregate = DocumentsFetchAggregator::from_query(documents_query, request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregatePostFetchDocuments(aggregate));
    }
}

/// This structure represent the `infos` field we send in the analytics.
/// It's quite close to the `Opt` structure except all sensitive informations
/// have been simplified to a boolean.
/// It's send as-is in amplitude thus you should never update a name of the
/// struct without the approval of the PM.
#[derive(Debug, Clone, Serialize)]
struct Infos {
    env: String,
    experimental_contains_filter: bool,
    experimental_enable_metrics: bool,
    experimental_search_queue_size: usize,
    experimental_logs_mode: LogMode,
    experimental_replication_parameters: bool,
    experimental_enable_logs_route: bool,
    experimental_reduce_indexing_memory_usage: bool,
    experimental_max_number_of_batched_tasks: usize,
    gpu_enabled: bool,
    db_path: bool,
    import_dump: bool,
    dump_dir: bool,
    ignore_missing_dump: bool,
    ignore_dump_if_db_exists: bool,
    import_snapshot: bool,
    schedule_snapshot: Option<u64>,
    snapshot_dir: bool,
    ignore_missing_snapshot: bool,
    ignore_snapshot_if_db_exists: bool,
    http_addr: bool,
    http_payload_size_limit: Byte,
    task_queue_webhook: bool,
    task_webhook_authorization_header: bool,
    log_level: String,
    max_indexing_memory: MaxMemory,
    max_indexing_threads: MaxThreads,
    with_configuration_file: bool,
    ssl_auth_path: bool,
    ssl_cert_path: bool,
    ssl_key_path: bool,
    ssl_ocsp_path: bool,
    ssl_require_auth: bool,
    ssl_resumption: bool,
    ssl_tickets: bool,
}

impl From<Opt> for Infos {
    fn from(options: Opt) -> Self {
        // We wants to decompose this whole struct by hand to be sure we don't forget
        // to add analytics when we add a field in the Opt.
        // Thus we must not insert `..` at the end.
        let Opt {
            db_path,
            experimental_contains_filter,
            experimental_enable_metrics,
            experimental_search_queue_size,
            experimental_logs_mode,
            experimental_replication_parameters,
            experimental_enable_logs_route,
            experimental_reduce_indexing_memory_usage,
            experimental_max_number_of_batched_tasks,
            http_addr,
            master_key: _,
            env,
            task_webhook_url,
            task_webhook_authorization_header,
            max_index_size: _,
            max_task_db_size: _,
            http_payload_size_limit,
            ssl_cert_path,
            ssl_key_path,
            ssl_auth_path,
            ssl_ocsp_path,
            ssl_require_auth,
            ssl_resumption,
            ssl_tickets,
            import_snapshot,
            ignore_missing_snapshot,
            ignore_snapshot_if_db_exists,
            snapshot_dir,
            schedule_snapshot,
            import_dump,
            ignore_missing_dump,
            ignore_dump_if_db_exists,
            dump_dir,
            log_level,
            indexer_options,
            config_file_path,
            #[cfg(feature = "analytics")]
                no_analytics: _,
        } = options;

        let schedule_snapshot = match schedule_snapshot {
            ScheduleSnapshot::Disabled => None,
            ScheduleSnapshot::Enabled(interval) => Some(interval),
        };

        let IndexerOpts { max_indexing_memory, max_indexing_threads, skip_index_budget: _ } =
            indexer_options;

        // We're going to override every sensible information.
        // We consider information sensible if it contains a path, an address, or a key.
        Self {
            env,
            experimental_contains_filter,
            experimental_enable_metrics,
            experimental_search_queue_size,
            experimental_logs_mode,
            experimental_replication_parameters,
            experimental_enable_logs_route,
            experimental_reduce_indexing_memory_usage,
            gpu_enabled: meilisearch_types::milli::vector::is_cuda_enabled(),
            db_path: db_path != PathBuf::from("./data.ms"),
            import_dump: import_dump.is_some(),
            dump_dir: dump_dir != PathBuf::from("dumps/"),
            ignore_missing_dump,
            ignore_dump_if_db_exists,
            import_snapshot: import_snapshot.is_some(),
            schedule_snapshot,
            snapshot_dir: snapshot_dir != PathBuf::from("snapshots/"),
            ignore_missing_snapshot,
            ignore_snapshot_if_db_exists,
            http_addr: http_addr != default_http_addr(),
            http_payload_size_limit,
            experimental_max_number_of_batched_tasks,
            task_queue_webhook: task_webhook_url.is_some(),
            task_webhook_authorization_header: task_webhook_authorization_header.is_some(),
            log_level: log_level.to_string(),
            max_indexing_memory,
            max_indexing_threads,
            with_configuration_file: config_file_path.is_some(),
            ssl_auth_path: ssl_auth_path.is_some(),
            ssl_cert_path: ssl_cert_path.is_some(),
            ssl_key_path: ssl_key_path.is_some(),
            ssl_ocsp_path: ssl_ocsp_path.is_some(),
            ssl_require_auth,
            ssl_resumption,
            ssl_tickets,
        }
    }
}

pub struct Segment {
    inbox: Receiver<AnalyticsMsg>,
    user: User,
    opt: Opt,
    batcher: AutoBatcher,
    get_search_aggregator: SearchAggregator,
    post_search_aggregator: SearchAggregator,
    post_multi_search_aggregator: MultiSearchAggregator,
    post_facet_search_aggregator: FacetSearchAggregator,
    add_documents_aggregator: DocumentsAggregator,
    delete_documents_aggregator: DocumentsDeletionAggregator,
    update_documents_aggregator: DocumentsAggregator,
    edit_documents_by_function_aggregator: EditDocumentsByFunctionAggregator,
    get_fetch_documents_aggregator: DocumentsFetchAggregator,
    post_fetch_documents_aggregator: DocumentsFetchAggregator,
    get_similar_aggregator: SimilarAggregator,
    post_similar_aggregator: SimilarAggregator,
}

impl Segment {
    fn compute_traits(opt: &Opt, stats: Stats) -> Value {
        static FIRST_START_TIMESTAMP: Lazy<Instant> = Lazy::new(Instant::now);
        static SYSTEM: Lazy<Value> = Lazy::new(|| {
            let disks = Disks::new_with_refreshed_list();
            let mut sys = System::new_all();
            sys.refresh_all();
            let kernel_version = System::kernel_version()
                .and_then(|k| k.split_once('-').map(|(k, _)| k.to_string()));
            json!({
                    "distribution": System::name(),
                    "kernel_version": kernel_version,
                    "cores": sys.cpus().len(),
                    "ram_size": sys.total_memory(),
                    "disk_size": disks.iter().map(|disk| disk.total_space()).max(),
                    "server_provider": std::env::var("MEILI_SERVER_PROVIDER").ok(),
            })
        });
        let number_of_documents =
            stats.indexes.values().map(|index| index.number_of_documents).collect::<Vec<u64>>();

        json!({
            "start_since_days": FIRST_START_TIMESTAMP.elapsed().as_secs() / (60 * 60 * 24), // one day
            "system": *SYSTEM,
            "stats": {
                "database_size": stats.database_size,
                "indexes_number": stats.indexes.len(),
                "documents_number": number_of_documents,
            },
            "infos": Infos::from(opt.clone()),
        })
    }

    async fn run(
        mut self,
        index_scheduler: Arc<IndexScheduler>,
        auth_controller: Arc<AuthController>,
    ) {
        const INTERVAL: Duration = Duration::from_secs(60 * 60); // one hour
                                                                 // The first batch must be sent after one hour.
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + INTERVAL, INTERVAL);

        loop {
            select! {
                _ = interval.tick() => {
                    self.tick(index_scheduler.clone(), auth_controller.clone()).await;
                },
                msg = self.inbox.recv() => {
                    match msg {
                        Some(AnalyticsMsg::BatchMessage(msg)) => drop(self.batcher.push(msg).await),
                        Some(AnalyticsMsg::AggregateGetSearch(agreg)) => self.get_search_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregatePostSearch(agreg)) => self.post_search_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregatePostMultiSearch(agreg)) => self.post_multi_search_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregatePostFacetSearch(agreg)) => self.post_facet_search_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateAddDocuments(agreg)) => self.add_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateDeleteDocuments(agreg)) => self.delete_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateUpdateDocuments(agreg)) => self.update_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateEditDocumentsByFunction(agreg)) => self.edit_documents_by_function_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateGetFetchDocuments(agreg)) => self.get_fetch_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregatePostFetchDocuments(agreg)) => self.post_fetch_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateGetSimilar(agreg)) => self.get_similar_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregatePostSimilar(agreg)) => self.post_similar_aggregator.aggregate(agreg),
                        None => (),
                    }
                }
            }
        }
    }

    async fn tick(
        &mut self,
        index_scheduler: Arc<IndexScheduler>,
        auth_controller: Arc<AuthController>,
    ) {
        if let Ok(stats) =
            create_all_stats(index_scheduler.into(), auth_controller.into(), &AuthFilter::default())
        {
            // Replace the version number with the prototype name if any.
            let version = if let Some(prototype) = build_info::DescribeResult::from_build()
                .and_then(|describe| describe.as_prototype())
            {
                prototype
            } else {
                env!("CARGO_PKG_VERSION")
            };

            let _ = self
                .batcher
                .push(Identify {
                    context: Some(json!({
                        "app": {
                            "version": version.to_string(),
                        },
                    })),
                    user: self.user.clone(),
                    traits: Self::compute_traits(&self.opt, stats),
                    ..Default::default()
                })
                .await;
        }

        let Segment {
            inbox: _,
            opt: _,
            batcher: _,
            user,
            get_search_aggregator,
            post_search_aggregator,
            post_multi_search_aggregator,
            post_facet_search_aggregator,
            add_documents_aggregator,
            delete_documents_aggregator,
            update_documents_aggregator,
            edit_documents_by_function_aggregator,
            get_fetch_documents_aggregator,
            post_fetch_documents_aggregator,
            get_similar_aggregator,
            post_similar_aggregator,
        } = self;

        if let Some(get_search) =
            take(get_search_aggregator).into_event(user, "Documents Searched GET")
        {
            let _ = self.batcher.push(get_search).await;
        }
        if let Some(post_search) =
            take(post_search_aggregator).into_event(user, "Documents Searched POST")
        {
            let _ = self.batcher.push(post_search).await;
        }
        if let Some(post_multi_search) = take(post_multi_search_aggregator)
            .into_event(user, "Documents Searched by Multi-Search POST")
        {
            let _ = self.batcher.push(post_multi_search).await;
        }
        if let Some(post_facet_search) =
            take(post_facet_search_aggregator).into_event(user, "Facet Searched POST")
        {
            let _ = self.batcher.push(post_facet_search).await;
        }
        if let Some(add_documents) =
            take(add_documents_aggregator).into_event(user, "Documents Added")
        {
            let _ = self.batcher.push(add_documents).await;
        }
        if let Some(delete_documents) =
            take(delete_documents_aggregator).into_event(user, "Documents Deleted")
        {
            let _ = self.batcher.push(delete_documents).await;
        }
        if let Some(update_documents) =
            take(update_documents_aggregator).into_event(user, "Documents Updated")
        {
            let _ = self.batcher.push(update_documents).await;
        }
        if let Some(edit_documents_by_function) = take(edit_documents_by_function_aggregator)
            .into_event(user, "Documents Edited By Function")
        {
            let _ = self.batcher.push(edit_documents_by_function).await;
        }
        if let Some(get_fetch_documents) =
            take(get_fetch_documents_aggregator).into_event(user, "Documents Fetched GET")
        {
            let _ = self.batcher.push(get_fetch_documents).await;
        }
        if let Some(post_fetch_documents) =
            take(post_fetch_documents_aggregator).into_event(user, "Documents Fetched POST")
        {
            let _ = self.batcher.push(post_fetch_documents).await;
        }

        if let Some(get_similar_documents) =
            take(get_similar_aggregator).into_event(user, "Similar GET")
        {
            let _ = self.batcher.push(get_similar_documents).await;
        }

        if let Some(post_similar_documents) =
            take(post_similar_aggregator).into_event(user, "Similar POST")
        {
            let _ = self.batcher.push(post_similar_documents).await;
        }
        let _ = self.batcher.flush().await;
    }
}

#[derive(Default)]
pub struct SearchAggregator {
    timestamp: Option<OffsetDateTime>,

    // context
    user_agents: HashSet<String>,

    // requests
    total_received: usize,
    total_succeeded: usize,
    total_degraded: usize,
    total_used_negative_operator: usize,
    time_spent: BinaryHeap<usize>,

    // sort
    sort_with_geo_point: bool,
    // every time a request has a filter, this field must be incremented by the number of terms it contains
    sort_sum_of_criteria_terms: usize,
    // every time a request has a filter, this field must be incremented by one
    sort_total_number_of_criteria: usize,

    // distinct
    distinct: bool,

    // filter
    filter_with_geo_radius: bool,
    filter_with_geo_bounding_box: bool,
    // every time a request has a filter, this field must be incremented by the number of terms it contains
    filter_sum_of_criteria_terms: usize,
    // every time a request has a filter, this field must be incremented by one
    filter_total_number_of_criteria: usize,
    used_syntax: HashMap<String, usize>,

    // attributes_to_search_on
    // every time a search is done using attributes_to_search_on
    attributes_to_search_on_total_number_of_uses: usize,

    // q
    // The maximum number of terms in a q request
    max_terms_number: usize,

    // vector
    // The maximum number of floats in a vector request
    max_vector_size: usize,
    // Whether the semantic ratio passed to a hybrid search equals the default ratio.
    semantic_ratio: bool,
    // Whether a non-default embedder was specified
    embedder: bool,
    hybrid: bool,
    retrieve_vectors: bool,

    // every time a search is done, we increment the counter linked to the used settings
    matching_strategy: HashMap<String, usize>,

    // List of the unique Locales passed as parameter
    locales: BTreeSet<Locale>,

    // pagination
    max_limit: usize,
    max_offset: usize,
    finite_pagination: usize,

    // formatting
    max_attributes_to_retrieve: usize,
    max_attributes_to_highlight: usize,
    highlight_pre_tag: bool,
    highlight_post_tag: bool,
    max_attributes_to_crop: usize,
    crop_marker: bool,
    show_matches_position: bool,
    crop_length: bool,

    // facets
    facets_sum_of_terms: usize,
    facets_total_number_of_facets: usize,

    // scoring
    show_ranking_score: bool,
    show_ranking_score_details: bool,
    ranking_score_threshold: bool,
}

impl SearchAggregator {
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_query(query: &SearchQuery, request: &HttpRequest) -> Self {
        let SearchQuery {
            q,
            vector,
            offset,
            limit,
            page,
            hits_per_page,
            attributes_to_retrieve: _,
            retrieve_vectors,
            attributes_to_crop: _,
            crop_length,
            attributes_to_highlight: _,
            show_matches_position,
            show_ranking_score,
            show_ranking_score_details,
            filter,
            sort,
            distinct,
            facets: _,
            highlight_pre_tag,
            highlight_post_tag,
            crop_marker,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
        } = query;

        let mut ret = Self::default();
        ret.timestamp = Some(OffsetDateTime::now_utc());

        ret.total_received = 1;
        ret.user_agents = extract_user_agents(request).into_iter().collect();

        if let Some(ref sort) = sort {
            ret.sort_total_number_of_criteria = 1;
            ret.sort_with_geo_point = sort.iter().any(|s| s.contains("_geoPoint("));
            ret.sort_sum_of_criteria_terms = sort.len();
        }

        ret.distinct = distinct.is_some();

        if let Some(ref filter) = filter {
            static RE: Lazy<Regex> = Lazy::new(|| Regex::new("AND | OR").unwrap());
            ret.filter_total_number_of_criteria = 1;

            let syntax = match filter {
                Value::String(_) => "string".to_string(),
                Value::Array(values) => {
                    if values.iter().map(|v| v.to_string()).any(|s| RE.is_match(&s)) {
                        "mixed".to_string()
                    } else {
                        "array".to_string()
                    }
                }
                _ => "none".to_string(),
            };
            // convert the string to a HashMap
            ret.used_syntax.insert(syntax, 1);

            let stringified_filters = filter.to_string();
            ret.filter_with_geo_radius = stringified_filters.contains("_geoRadius(");
            ret.filter_with_geo_bounding_box = stringified_filters.contains("_geoBoundingBox(");
            ret.filter_sum_of_criteria_terms = RE.split(&stringified_filters).count();
        }

        // attributes_to_search_on
        if attributes_to_search_on.is_some() {
            ret.attributes_to_search_on_total_number_of_uses = 1;
        }

        if let Some(ref q) = q {
            ret.max_terms_number = q.split_whitespace().count();
        }

        if let Some(ref vector) = vector {
            ret.max_vector_size = vector.len();
        }
        ret.retrieve_vectors |= retrieve_vectors;

        if query.is_finite_pagination() {
            let limit = hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
            ret.max_limit = limit;
            ret.max_offset = page.unwrap_or(1).saturating_sub(1) * limit;
            ret.finite_pagination = 1;
        } else {
            ret.max_limit = *limit;
            ret.max_offset = *offset;
            ret.finite_pagination = 0;
        }

        ret.matching_strategy.insert(format!("{:?}", matching_strategy), 1);

        if let Some(locales) = locales {
            ret.locales = locales.into_iter().copied().collect();
        }

        ret.highlight_pre_tag = *highlight_pre_tag != DEFAULT_HIGHLIGHT_PRE_TAG();
        ret.highlight_post_tag = *highlight_post_tag != DEFAULT_HIGHLIGHT_POST_TAG();
        ret.crop_marker = *crop_marker != DEFAULT_CROP_MARKER();
        ret.crop_length = *crop_length != DEFAULT_CROP_LENGTH();
        ret.show_matches_position = *show_matches_position;

        ret.show_ranking_score = *show_ranking_score;
        ret.show_ranking_score_details = *show_ranking_score_details;
        ret.ranking_score_threshold = ranking_score_threshold.is_some();

        if let Some(hybrid) = hybrid {
            ret.semantic_ratio = hybrid.semantic_ratio != DEFAULT_SEMANTIC_RATIO();
            ret.embedder = hybrid.embedder.is_some();
            ret.hybrid = true;
        }

        ret
    }

    pub fn succeed(&mut self, result: &SearchResult) {
        let SearchResult {
            hits: _,
            query: _,
            processing_time_ms,
            hits_info: _,
            semantic_hit_count: _,
            facet_distribution: _,
            facet_stats: _,
            degraded,
            used_negative_operator,
        } = result;

        self.total_succeeded = self.total_succeeded.saturating_add(1);
        if *degraded {
            self.total_degraded = self.total_degraded.saturating_add(1);
        }
        if *used_negative_operator {
            self.total_used_negative_operator = self.total_used_negative_operator.saturating_add(1);
        }
        self.time_spent.push(*processing_time_ms as usize);
    }

    /// Aggregate one [SearchAggregator] into another.
    pub fn aggregate(&mut self, mut other: Self) {
        let Self {
            timestamp,
            user_agents,
            total_received,
            total_succeeded,
            ref mut time_spent,
            sort_with_geo_point,
            sort_sum_of_criteria_terms,
            sort_total_number_of_criteria,
            distinct,
            filter_with_geo_radius,
            filter_with_geo_bounding_box,
            filter_sum_of_criteria_terms,
            filter_total_number_of_criteria,
            used_syntax,
            attributes_to_search_on_total_number_of_uses,
            max_terms_number,
            max_vector_size,
            retrieve_vectors,
            matching_strategy,
            max_limit,
            max_offset,
            finite_pagination,
            max_attributes_to_retrieve,
            max_attributes_to_highlight,
            highlight_pre_tag,
            highlight_post_tag,
            max_attributes_to_crop,
            crop_marker,
            show_matches_position,
            crop_length,
            facets_sum_of_terms,
            facets_total_number_of_facets,
            show_ranking_score,
            show_ranking_score_details,
            semantic_ratio,
            embedder,
            hybrid,
            total_degraded,
            total_used_negative_operator,
            ranking_score_threshold,
            ref mut locales,
        } = other;

        if self.timestamp.is_none() {
            self.timestamp = timestamp;
        }

        // context
        for user_agent in user_agents.into_iter() {
            self.user_agents.insert(user_agent);
        }

        // request
        self.total_received = self.total_received.saturating_add(total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(total_succeeded);
        self.total_degraded = self.total_degraded.saturating_add(total_degraded);
        self.total_used_negative_operator =
            self.total_used_negative_operator.saturating_add(total_used_negative_operator);
        self.time_spent.append(time_spent);

        // sort
        self.sort_with_geo_point |= sort_with_geo_point;
        self.sort_sum_of_criteria_terms =
            self.sort_sum_of_criteria_terms.saturating_add(sort_sum_of_criteria_terms);
        self.sort_total_number_of_criteria =
            self.sort_total_number_of_criteria.saturating_add(sort_total_number_of_criteria);

        // distinct
        self.distinct |= distinct;

        // filter
        self.filter_with_geo_radius |= filter_with_geo_radius;
        self.filter_with_geo_bounding_box |= filter_with_geo_bounding_box;
        self.filter_sum_of_criteria_terms =
            self.filter_sum_of_criteria_terms.saturating_add(filter_sum_of_criteria_terms);
        self.filter_total_number_of_criteria =
            self.filter_total_number_of_criteria.saturating_add(filter_total_number_of_criteria);
        for (key, value) in used_syntax.into_iter() {
            let used_syntax = self.used_syntax.entry(key).or_insert(0);
            *used_syntax = used_syntax.saturating_add(value);
        }

        // attributes_to_search_on
        self.attributes_to_search_on_total_number_of_uses = self
            .attributes_to_search_on_total_number_of_uses
            .saturating_add(attributes_to_search_on_total_number_of_uses);

        // q
        self.max_terms_number = self.max_terms_number.max(max_terms_number);

        // vector
        self.max_vector_size = self.max_vector_size.max(max_vector_size);
        self.retrieve_vectors |= retrieve_vectors;
        self.semantic_ratio |= semantic_ratio;
        self.hybrid |= hybrid;
        self.embedder |= embedder;

        // pagination
        self.max_limit = self.max_limit.max(max_limit);
        self.max_offset = self.max_offset.max(max_offset);
        self.finite_pagination += finite_pagination;

        // formatting
        self.max_attributes_to_retrieve =
            self.max_attributes_to_retrieve.max(max_attributes_to_retrieve);
        self.max_attributes_to_highlight =
            self.max_attributes_to_highlight.max(max_attributes_to_highlight);
        self.highlight_pre_tag |= highlight_pre_tag;
        self.highlight_post_tag |= highlight_post_tag;
        self.max_attributes_to_crop = self.max_attributes_to_crop.max(max_attributes_to_crop);
        self.crop_marker |= crop_marker;
        self.show_matches_position |= show_matches_position;
        self.crop_length |= crop_length;

        // facets
        self.facets_sum_of_terms = self.facets_sum_of_terms.saturating_add(facets_sum_of_terms);
        self.facets_total_number_of_facets =
            self.facets_total_number_of_facets.saturating_add(facets_total_number_of_facets);

        // matching strategy
        for (key, value) in matching_strategy.into_iter() {
            let matching_strategy = self.matching_strategy.entry(key).or_insert(0);
            *matching_strategy = matching_strategy.saturating_add(value);
        }

        // scoring
        self.show_ranking_score |= show_ranking_score;
        self.show_ranking_score_details |= show_ranking_score_details;
        self.ranking_score_threshold |= ranking_score_threshold;

        // locales
        self.locales.append(locales);
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        let Self {
            timestamp,
            user_agents,
            total_received,
            total_succeeded,
            time_spent,
            sort_with_geo_point,
            sort_sum_of_criteria_terms,
            sort_total_number_of_criteria,
            distinct,
            filter_with_geo_radius,
            filter_with_geo_bounding_box,
            filter_sum_of_criteria_terms,
            filter_total_number_of_criteria,
            used_syntax,
            attributes_to_search_on_total_number_of_uses,
            max_terms_number,
            max_vector_size,
            retrieve_vectors,
            matching_strategy,
            max_limit,
            max_offset,
            finite_pagination,
            max_attributes_to_retrieve,
            max_attributes_to_highlight,
            highlight_pre_tag,
            highlight_post_tag,
            max_attributes_to_crop,
            crop_marker,
            show_matches_position,
            crop_length,
            facets_sum_of_terms,
            facets_total_number_of_facets,
            show_ranking_score,
            show_ranking_score_details,
            semantic_ratio,
            embedder,
            hybrid,
            total_degraded,
            total_used_negative_operator,
            ranking_score_threshold,
            locales,
        } = self;

        if total_received == 0 {
            None
        } else {
            // we get all the values in a sorted manner
            let time_spent = time_spent.into_sorted_vec();
            // the index of the 99th percentage of value
            let percentile_99th = time_spent.len() * 99 / 100;
            // We are only interested by the slowest value of the 99th fastest results
            let time_spent = time_spent.get(percentile_99th);

            let properties = json!({
                "user-agent": user_agents,
                "requests": {
                    "99th_response_time": time_spent.map(|t| format!("{:.2}", t)),
                    "total_succeeded": total_succeeded,
                    "total_failed": total_received.saturating_sub(total_succeeded), // just to be sure we never panics
                    "total_received": total_received,
                    "total_degraded": total_degraded,
                    "total_used_negative_operator": total_used_negative_operator,
                },
                "sort": {
                    "with_geoPoint": sort_with_geo_point,
                    "avg_criteria_number": format!("{:.2}", sort_sum_of_criteria_terms as f64 / sort_total_number_of_criteria as f64),
                },
                "distinct": distinct,
                "filter": {
                   "with_geoRadius": filter_with_geo_radius,
                   "with_geoBoundingBox": filter_with_geo_bounding_box,
                   "avg_criteria_number": format!("{:.2}", filter_sum_of_criteria_terms as f64 / filter_total_number_of_criteria as f64),
                   "most_used_syntax": used_syntax.iter().max_by_key(|(_, v)| *v).map(|(k, _)| json!(k)).unwrap_or_else(|| json!(null)),
                },
                "attributes_to_search_on": {
                   "total_number_of_uses": attributes_to_search_on_total_number_of_uses,
                },
                "q": {
                   "max_terms_number": max_terms_number,
                },
                "vector": {
                    "max_vector_size": max_vector_size,
                    "retrieve_vectors": retrieve_vectors,
                },
                "hybrid": {
                    "enabled": hybrid,
                    "semantic_ratio": semantic_ratio,
                    "embedder": embedder,
                },
                "pagination": {
                   "max_limit": max_limit,
                   "max_offset": max_offset,
                   "most_used_navigation": if finite_pagination > (total_received / 2) { "exhaustive" } else { "estimated" },
                },
                "formatting": {
                    "max_attributes_to_retrieve": max_attributes_to_retrieve,
                    "max_attributes_to_highlight": max_attributes_to_highlight,
                    "highlight_pre_tag": highlight_pre_tag,
                    "highlight_post_tag": highlight_post_tag,
                    "max_attributes_to_crop": max_attributes_to_crop,
                    "crop_marker": crop_marker,
                    "show_matches_position": show_matches_position,
                    "crop_length": crop_length,
                },
                "facets": {
                    "avg_facets_number": format!("{:.2}", facets_sum_of_terms as f64 / facets_total_number_of_facets as f64),
                },
                "matching_strategy": {
                    "most_used_strategy": matching_strategy.iter().max_by_key(|(_, v)| *v).map(|(k, _)| json!(k)).unwrap_or_else(|| json!(null)),
                },
                "locales": locales,
                "scoring": {
                    "show_ranking_score": show_ranking_score,
                    "show_ranking_score_details": show_ranking_score_details,
                    "ranking_score_threshold": ranking_score_threshold,
                },
            });

            Some(Track {
                timestamp,
                user: user.clone(),
                event: event_name.to_string(),
                properties,
                ..Default::default()
            })
        }
    }
}

#[derive(Default)]
pub struct MultiSearchAggregator {
    timestamp: Option<OffsetDateTime>,

    // requests
    total_received: usize,
    total_succeeded: usize,

    // sum of the number of distinct indexes in each single request, use with total_received to compute an avg
    total_distinct_index_count: usize,
    // number of queries with a single index, use with total_received to compute a proportion
    total_single_index: usize,

    // sum of the number of search queries in the requests, use with total_received to compute an average
    total_search_count: usize,

    // scoring
    show_ranking_score: bool,
    show_ranking_score_details: bool,

    // federation
    use_federation: bool,

    // context
    user_agents: HashSet<String>,
}

impl MultiSearchAggregator {
    pub fn from_federated_search(
        federated_search: &FederatedSearch,
        request: &HttpRequest,
    ) -> Self {
        let timestamp = Some(OffsetDateTime::now_utc());

        let user_agents = extract_user_agents(request).into_iter().collect();

        let use_federation = federated_search.federation.is_some();

        let distinct_indexes: HashSet<_> = federated_search
            .queries
            .iter()
            .map(|query| {
                let query = &query;
                // make sure we get a compilation error if a field gets added to / removed from SearchQueryWithIndex
                let SearchQueryWithIndex {
                    index_uid,
                    federation_options: _,
                    q: _,
                    vector: _,
                    offset: _,
                    limit: _,
                    page: _,
                    hits_per_page: _,
                    attributes_to_retrieve: _,
                    retrieve_vectors: _,
                    attributes_to_crop: _,
                    crop_length: _,
                    attributes_to_highlight: _,
                    show_ranking_score: _,
                    show_ranking_score_details: _,
                    show_matches_position: _,
                    filter: _,
                    sort: _,
                    distinct: _,
                    facets: _,
                    highlight_pre_tag: _,
                    highlight_post_tag: _,
                    crop_marker: _,
                    matching_strategy: _,
                    attributes_to_search_on: _,
                    hybrid: _,
                    ranking_score_threshold: _,
                    locales: _,
                } = query;

                index_uid.as_str()
            })
            .collect();

        let show_ranking_score =
            federated_search.queries.iter().any(|query| query.show_ranking_score);
        let show_ranking_score_details =
            federated_search.queries.iter().any(|query| query.show_ranking_score_details);

        Self {
            timestamp,
            total_received: 1,
            total_succeeded: 0,
            total_distinct_index_count: distinct_indexes.len(),
            total_single_index: if distinct_indexes.len() == 1 { 1 } else { 0 },
            total_search_count: federated_search.queries.len(),
            show_ranking_score,
            show_ranking_score_details,
            user_agents,
            use_federation,
        }
    }

    pub fn succeed(&mut self) {
        self.total_succeeded = self.total_succeeded.saturating_add(1);
    }

    /// Aggregate one [MultiSearchAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        // write the aggregate in a way that will cause a compilation error if a field is added.

        // get ownership of self, replacing it by a default value.
        let this = std::mem::take(self);

        let timestamp = this.timestamp.or(other.timestamp);
        let total_received = this.total_received.saturating_add(other.total_received);
        let total_succeeded = this.total_succeeded.saturating_add(other.total_succeeded);
        let total_distinct_index_count =
            this.total_distinct_index_count.saturating_add(other.total_distinct_index_count);
        let total_single_index = this.total_single_index.saturating_add(other.total_single_index);
        let total_search_count = this.total_search_count.saturating_add(other.total_search_count);
        let show_ranking_score = this.show_ranking_score || other.show_ranking_score;
        let show_ranking_score_details =
            this.show_ranking_score_details || other.show_ranking_score_details;
        let mut user_agents = this.user_agents;
        let use_federation = this.use_federation || other.use_federation;

        for user_agent in other.user_agents.into_iter() {
            user_agents.insert(user_agent);
        }

        // need all fields or compile error
        let mut aggregated = Self {
            timestamp,
            total_received,
            total_succeeded,
            total_distinct_index_count,
            total_single_index,
            total_search_count,
            user_agents,
            show_ranking_score,
            show_ranking_score_details,
            use_federation,
            // do not add _ or ..Default::default() here
        };

        // replace the default self with the aggregated value
        std::mem::swap(self, &mut aggregated);
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        let Self {
            timestamp,
            total_received,
            total_succeeded,
            total_distinct_index_count,
            total_single_index,
            total_search_count,
            user_agents,
            show_ranking_score,
            show_ranking_score_details,
            use_federation,
        } = self;

        if total_received == 0 {
            None
        } else {
            let properties = json!({
                "user-agent": user_agents,
                "requests": {
                    "total_succeeded": total_succeeded,
                    "total_failed": total_received.saturating_sub(total_succeeded), // just to be sure we never panics
                    "total_received": total_received,
                },
                "indexes": {
                    "total_single_index": total_single_index,
                    "total_distinct_index_count": total_distinct_index_count,
                    "avg_distinct_index_count": (total_distinct_index_count as f64) / (total_received as f64), // not 0 else returned early
                },
                "searches": {
                    "total_search_count": total_search_count,
                    "avg_search_count": (total_search_count as f64) / (total_received as f64),
                },
                "scoring": {
                    "show_ranking_score": show_ranking_score,
                    "show_ranking_score_details": show_ranking_score_details,
                },
                "federation": {
                    "use_federation": use_federation,
                }
            });

            Some(Track {
                timestamp,
                user: user.clone(),
                event: event_name.to_string(),
                properties,
                ..Default::default()
            })
        }
    }
}

#[derive(Default)]
pub struct FacetSearchAggregator {
    timestamp: Option<OffsetDateTime>,

    // context
    user_agents: HashSet<String>,

    // requests
    total_received: usize,
    total_succeeded: usize,
    time_spent: BinaryHeap<usize>,

    // The set of all facetNames that were used
    facet_names: HashSet<String>,

    // As there been any other parameter than the facetName or facetQuery ones?
    additional_search_parameters_provided: bool,
}

impl FacetSearchAggregator {
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_query(query: &FacetSearchQuery, request: &HttpRequest) -> Self {
        let FacetSearchQuery {
            facet_query: _,
            facet_name,
            vector,
            q,
            filter,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
        } = query;

        let mut ret = Self::default();
        ret.timestamp = Some(OffsetDateTime::now_utc());

        ret.total_received = 1;
        ret.user_agents = extract_user_agents(request).into_iter().collect();
        ret.facet_names = Some(facet_name.clone()).into_iter().collect();

        ret.additional_search_parameters_provided = q.is_some()
            || vector.is_some()
            || filter.is_some()
            || *matching_strategy != MatchingStrategy::default()
            || attributes_to_search_on.is_some()
            || hybrid.is_some()
            || ranking_score_threshold.is_some()
            || locales.is_some();

        ret
    }

    pub fn succeed(&mut self, result: &FacetSearchResult) {
        let FacetSearchResult { facet_hits: _, facet_query: _, processing_time_ms } = result;
        self.total_succeeded = self.total_succeeded.saturating_add(1);
        self.time_spent.push(*processing_time_ms as usize);
    }

    /// Aggregate one [FacetSearchAggregator] into another.
    pub fn aggregate(&mut self, mut other: Self) {
        let Self {
            timestamp,
            user_agents,
            total_received,
            total_succeeded,
            ref mut time_spent,
            facet_names,
            additional_search_parameters_provided,
        } = other;

        if self.timestamp.is_none() {
            self.timestamp = timestamp;
        }

        // context
        for user_agent in user_agents.into_iter() {
            self.user_agents.insert(user_agent);
        }

        // request
        self.total_received = self.total_received.saturating_add(total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(total_succeeded);
        self.time_spent.append(time_spent);

        // facet_names
        for facet_name in facet_names.into_iter() {
            self.facet_names.insert(facet_name);
        }

        // additional_search_parameters_provided
        self.additional_search_parameters_provided |= additional_search_parameters_provided;
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        let Self {
            timestamp,
            user_agents,
            total_received,
            total_succeeded,
            time_spent,
            facet_names,
            additional_search_parameters_provided,
        } = self;

        if total_received == 0 {
            None
        } else {
            // the index of the 99th percentage of value
            let percentile_99th = 0.99 * (total_succeeded as f64 - 1.) + 1.;
            // we get all the values in a sorted manner
            let time_spent = time_spent.into_sorted_vec();
            // We are only interested by the slowest value of the 99th fastest results
            let time_spent = time_spent.get(percentile_99th as usize);

            let properties = json!({
                "user-agent": user_agents,
                "requests": {
                    "99th_response_time":  time_spent.map(|t| format!("{:.2}", t)),
                    "total_succeeded": total_succeeded,
                    "total_failed": total_received.saturating_sub(total_succeeded), // just to be sure we never panics
                    "total_received": total_received,
                },
                "facets": {
                    "total_distinct_facet_count": facet_names.len(),
                    "additional_search_parameters_provided": additional_search_parameters_provided,
                },
            });

            Some(Track {
                timestamp,
                user: user.clone(),
                event: event_name.to_string(),
                properties,
                ..Default::default()
            })
        }
    }
}

#[derive(Default)]
pub struct DocumentsAggregator {
    timestamp: Option<OffsetDateTime>,

    // set to true when at least one request was received
    updated: bool,

    // context
    user_agents: HashSet<String>,

    content_types: HashSet<String>,
    primary_keys: HashSet<String>,
    index_creation: bool,
}

impl DocumentsAggregator {
    pub fn from_query(
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    ) -> Self {
        let UpdateDocumentsQuery { primary_key, csv_delimiter: _ } = documents_query;

        let mut primary_keys = HashSet::new();
        if let Some(primary_key) = primary_key.clone() {
            primary_keys.insert(primary_key);
        }

        let mut content_types = HashSet::new();
        let content_type = request
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|s| s.to_str().ok())
            .unwrap_or("unknown")
            .to_string();
        content_types.insert(content_type);

        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            updated: true,
            user_agents: extract_user_agents(request).into_iter().collect(),
            content_types,
            primary_keys,
            index_creation,
        }
    }

    /// Aggregate one [DocumentsAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        let Self { timestamp, user_agents, primary_keys, content_types, index_creation, updated } =
            other;

        if self.timestamp.is_none() {
            self.timestamp = timestamp;
        }

        self.updated |= updated;
        // we can't create a union because there is no `into_union` method
        for user_agent in user_agents {
            self.user_agents.insert(user_agent);
        }
        for primary_key in primary_keys {
            self.primary_keys.insert(primary_key);
        }
        for content_type in content_types {
            self.content_types.insert(content_type);
        }
        self.index_creation |= index_creation;
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        let Self { timestamp, user_agents, primary_keys, content_types, index_creation, updated } =
            self;

        if !updated {
            None
        } else {
            let properties = json!({
                "user-agent": user_agents,
                "payload_type": content_types,
                "primary_key": primary_keys,
                "index_creation": index_creation,
            });

            Some(Track {
                timestamp,
                user: user.clone(),
                event: event_name.to_string(),
                properties,
                ..Default::default()
            })
        }
    }
}

#[derive(Default)]
pub struct EditDocumentsByFunctionAggregator {
    timestamp: Option<OffsetDateTime>,

    // Set to true if at least one request was filtered
    filtered: bool,
    // Set to true if at least one request contained a context
    with_context: bool,

    // context
    user_agents: HashSet<String>,

    index_creation: bool,
}

impl EditDocumentsByFunctionAggregator {
    pub fn from_query(
        documents_query: &DocumentEditionByFunction,
        index_creation: bool,
        request: &HttpRequest,
    ) -> Self {
        let DocumentEditionByFunction { filter, context, function: _ } = documents_query;

        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            user_agents: extract_user_agents(request).into_iter().collect(),
            filtered: filter.is_some(),
            with_context: context.is_some(),
            index_creation,
        }
    }

    /// Aggregate one [DocumentsAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        let Self { timestamp, user_agents, index_creation, filtered, with_context } = other;

        if self.timestamp.is_none() {
            self.timestamp = timestamp;
        }

        // we can't create a union because there is no `into_union` method
        for user_agent in user_agents {
            self.user_agents.insert(user_agent);
        }
        self.index_creation |= index_creation;
        self.filtered |= filtered;
        self.with_context |= with_context;
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        let Self { timestamp, user_agents, index_creation, filtered, with_context } = self;

        let properties = json!({
            "user-agent": user_agents,
            "filtered": filtered,
            "with_context": with_context,
            "index_creation": index_creation,
        });

        Some(Track {
            timestamp,
            user: user.clone(),
            event: event_name.to_string(),
            properties,
            ..Default::default()
        })
    }
}

#[derive(Default, Serialize)]
pub struct DocumentsDeletionAggregator {
    #[serde(skip)]
    timestamp: Option<OffsetDateTime>,

    // context
    #[serde(rename = "user-agent")]
    user_agents: HashSet<String>,

    #[serde(rename = "requests.total_received")]
    total_received: usize,
    per_document_id: bool,
    clear_all: bool,
    per_batch: bool,
    per_filter: bool,
}

impl DocumentsDeletionAggregator {
    pub fn from_query(kind: DocumentDeletionKind, request: &HttpRequest) -> Self {
        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            user_agents: extract_user_agents(request).into_iter().collect(),
            total_received: 1,
            per_document_id: matches!(kind, DocumentDeletionKind::PerDocumentId),
            clear_all: matches!(kind, DocumentDeletionKind::ClearAll),
            per_batch: matches!(kind, DocumentDeletionKind::PerBatch),
            per_filter: matches!(kind, DocumentDeletionKind::PerFilter),
        }
    }

    /// Aggregate one [DocumentsAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        let Self {
            timestamp,
            user_agents,
            total_received,
            per_document_id,
            clear_all,
            per_batch,
            per_filter,
        } = other;

        if self.timestamp.is_none() {
            self.timestamp = timestamp;
        }

        // we can't create a union because there is no `into_union` method
        for user_agent in user_agents {
            self.user_agents.insert(user_agent);
        }
        self.total_received = self.total_received.saturating_add(total_received);
        self.per_document_id |= per_document_id;
        self.clear_all |= clear_all;
        self.per_batch |= per_batch;
        self.per_filter |= per_filter;
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        // if we had no timestamp it means we never encountered any events and
        // thus we don't need to send this event.
        let timestamp = self.timestamp?;

        Some(Track {
            timestamp: Some(timestamp),
            user: user.clone(),
            event: event_name.to_string(),
            properties: serde_json::to_value(self).ok()?,
            ..Default::default()
        })
    }
}

#[derive(Default, Serialize)]
pub struct DocumentsFetchAggregator {
    #[serde(skip)]
    timestamp: Option<OffsetDateTime>,

    // context
    #[serde(rename = "user-agent")]
    user_agents: HashSet<String>,

    #[serde(rename = "requests.total_received")]
    total_received: usize,

    // a call on ../documents/:doc_id
    per_document_id: bool,
    // if a filter was used
    per_filter: bool,

    #[serde(rename = "vector.retrieve_vectors")]
    retrieve_vectors: bool,

    // pagination
    #[serde(rename = "pagination.max_limit")]
    max_limit: usize,
    #[serde(rename = "pagination.max_offset")]
    max_offset: usize,
}

impl DocumentsFetchAggregator {
    pub fn from_query(query: &DocumentFetchKind, request: &HttpRequest) -> Self {
        let (limit, offset, retrieve_vectors) = match query {
            DocumentFetchKind::PerDocumentId { retrieve_vectors } => (1, 0, *retrieve_vectors),
            DocumentFetchKind::Normal { limit, offset, retrieve_vectors, .. } => {
                (*limit, *offset, *retrieve_vectors)
            }
        };
        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            user_agents: extract_user_agents(request).into_iter().collect(),
            total_received: 1,
            per_document_id: matches!(query, DocumentFetchKind::PerDocumentId { .. }),
            per_filter: matches!(query, DocumentFetchKind::Normal { with_filter, .. } if *with_filter),
            max_limit: limit,
            max_offset: offset,
            retrieve_vectors,
        }
    }

    /// Aggregate one [DocumentsFetchAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        let Self {
            timestamp,
            user_agents,
            total_received,
            per_document_id,
            per_filter,
            max_limit,
            max_offset,
            retrieve_vectors,
        } = other;

        if self.timestamp.is_none() {
            self.timestamp = timestamp;
        }
        for user_agent in user_agents {
            self.user_agents.insert(user_agent);
        }

        self.total_received = self.total_received.saturating_add(total_received);
        self.per_document_id |= per_document_id;
        self.per_filter |= per_filter;

        self.max_limit = self.max_limit.max(max_limit);
        self.max_offset = self.max_offset.max(max_offset);

        self.retrieve_vectors |= retrieve_vectors;
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        // if we had no timestamp it means we never encountered any events and
        // thus we don't need to send this event.
        let timestamp = self.timestamp?;

        Some(Track {
            timestamp: Some(timestamp),
            user: user.clone(),
            event: event_name.to_string(),
            properties: serde_json::to_value(self).ok()?,
            ..Default::default()
        })
    }
}

#[derive(Default)]
pub struct SimilarAggregator {
    timestamp: Option<OffsetDateTime>,

    // context
    user_agents: HashSet<String>,

    // requests
    total_received: usize,
    total_succeeded: usize,
    time_spent: BinaryHeap<usize>,

    // filter
    filter_with_geo_radius: bool,
    filter_with_geo_bounding_box: bool,
    // every time a request has a filter, this field must be incremented by the number of terms it contains
    filter_sum_of_criteria_terms: usize,
    // every time a request has a filter, this field must be incremented by one
    filter_total_number_of_criteria: usize,
    used_syntax: HashMap<String, usize>,

    // Whether a non-default embedder was specified
    embedder: bool,
    retrieve_vectors: bool,

    // pagination
    max_limit: usize,
    max_offset: usize,

    // formatting
    max_attributes_to_retrieve: usize,

    // scoring
    show_ranking_score: bool,
    show_ranking_score_details: bool,
    ranking_score_threshold: bool,
}

impl SimilarAggregator {
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_query(query: &SimilarQuery, request: &HttpRequest) -> Self {
        let SimilarQuery {
            id: _,
            embedder,
            offset,
            limit,
            attributes_to_retrieve: _,
            retrieve_vectors,
            show_ranking_score,
            show_ranking_score_details,
            filter,
            ranking_score_threshold,
        } = query;

        let mut ret = Self::default();
        ret.timestamp = Some(OffsetDateTime::now_utc());

        ret.total_received = 1;
        ret.user_agents = extract_user_agents(request).into_iter().collect();

        if let Some(ref filter) = filter {
            static RE: Lazy<Regex> = Lazy::new(|| Regex::new("AND | OR").unwrap());
            ret.filter_total_number_of_criteria = 1;

            let syntax = match filter {
                Value::String(_) => "string".to_string(),
                Value::Array(values) => {
                    if values.iter().map(|v| v.to_string()).any(|s| RE.is_match(&s)) {
                        "mixed".to_string()
                    } else {
                        "array".to_string()
                    }
                }
                _ => "none".to_string(),
            };
            // convert the string to a HashMap
            ret.used_syntax.insert(syntax, 1);

            let stringified_filters = filter.to_string();
            ret.filter_with_geo_radius = stringified_filters.contains("_geoRadius(");
            ret.filter_with_geo_bounding_box = stringified_filters.contains("_geoBoundingBox(");
            ret.filter_sum_of_criteria_terms = RE.split(&stringified_filters).count();
        }

        ret.max_limit = *limit;
        ret.max_offset = *offset;

        ret.show_ranking_score = *show_ranking_score;
        ret.show_ranking_score_details = *show_ranking_score_details;
        ret.ranking_score_threshold = ranking_score_threshold.is_some();

        ret.embedder = embedder.is_some();
        ret.retrieve_vectors = *retrieve_vectors;

        ret
    }

    pub fn succeed(&mut self, result: &SimilarResult) {
        let SimilarResult { id: _, hits: _, processing_time_ms, hits_info: _ } = result;

        self.total_succeeded = self.total_succeeded.saturating_add(1);

        self.time_spent.push(*processing_time_ms as usize);
    }

    /// Aggregate one [SimilarAggregator] into another.
    pub fn aggregate(&mut self, mut other: Self) {
        let Self {
            timestamp,
            user_agents,
            total_received,
            total_succeeded,
            ref mut time_spent,
            filter_with_geo_radius,
            filter_with_geo_bounding_box,
            filter_sum_of_criteria_terms,
            filter_total_number_of_criteria,
            used_syntax,
            max_limit,
            max_offset,
            max_attributes_to_retrieve,
            show_ranking_score,
            show_ranking_score_details,
            embedder,
            ranking_score_threshold,
            retrieve_vectors,
        } = other;

        if self.timestamp.is_none() {
            self.timestamp = timestamp;
        }

        // context
        for user_agent in user_agents.into_iter() {
            self.user_agents.insert(user_agent);
        }

        // request
        self.total_received = self.total_received.saturating_add(total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(total_succeeded);
        self.time_spent.append(time_spent);

        // filter
        self.filter_with_geo_radius |= filter_with_geo_radius;
        self.filter_with_geo_bounding_box |= filter_with_geo_bounding_box;
        self.filter_sum_of_criteria_terms =
            self.filter_sum_of_criteria_terms.saturating_add(filter_sum_of_criteria_terms);
        self.filter_total_number_of_criteria =
            self.filter_total_number_of_criteria.saturating_add(filter_total_number_of_criteria);
        for (key, value) in used_syntax.into_iter() {
            let used_syntax = self.used_syntax.entry(key).or_insert(0);
            *used_syntax = used_syntax.saturating_add(value);
        }

        self.embedder |= embedder;
        self.retrieve_vectors |= retrieve_vectors;

        // pagination
        self.max_limit = self.max_limit.max(max_limit);
        self.max_offset = self.max_offset.max(max_offset);

        // formatting
        self.max_attributes_to_retrieve =
            self.max_attributes_to_retrieve.max(max_attributes_to_retrieve);

        // scoring
        self.show_ranking_score |= show_ranking_score;
        self.show_ranking_score_details |= show_ranking_score_details;
        self.ranking_score_threshold |= ranking_score_threshold;
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        let Self {
            timestamp,
            user_agents,
            total_received,
            total_succeeded,
            time_spent,
            filter_with_geo_radius,
            filter_with_geo_bounding_box,
            filter_sum_of_criteria_terms,
            filter_total_number_of_criteria,
            used_syntax,
            max_limit,
            max_offset,
            max_attributes_to_retrieve,
            show_ranking_score,
            show_ranking_score_details,
            embedder,
            ranking_score_threshold,
            retrieve_vectors,
        } = self;

        if total_received == 0 {
            None
        } else {
            // we get all the values in a sorted manner
            let time_spent = time_spent.into_sorted_vec();
            // the index of the 99th percentage of value
            let percentile_99th = time_spent.len() * 99 / 100;
            // We are only interested by the slowest value of the 99th fastest results
            let time_spent = time_spent.get(percentile_99th);

            let properties = json!({
                "user-agent": user_agents,
                "requests": {
                    "99th_response_time": time_spent.map(|t| format!("{:.2}", t)),
                    "total_succeeded": total_succeeded,
                    "total_failed": total_received.saturating_sub(total_succeeded), // just to be sure we never panics
                    "total_received": total_received,
                },
                "filter": {
                   "with_geoRadius": filter_with_geo_radius,
                   "with_geoBoundingBox": filter_with_geo_bounding_box,
                   "avg_criteria_number": format!("{:.2}", filter_sum_of_criteria_terms as f64 / filter_total_number_of_criteria as f64),
                   "most_used_syntax": used_syntax.iter().max_by_key(|(_, v)| *v).map(|(k, _)| json!(k)).unwrap_or_else(|| json!(null)),
                },
                "vector": {
                    "retrieve_vectors": retrieve_vectors,
                },
                "hybrid": {
                    "embedder": embedder,
                },
                "pagination": {
                   "max_limit": max_limit,
                   "max_offset": max_offset,
                },
                "formatting": {
                    "max_attributes_to_retrieve": max_attributes_to_retrieve,
                },
                "scoring": {
                    "show_ranking_score": show_ranking_score,
                    "show_ranking_score_details": show_ranking_score_details,
                    "ranking_score_threshold": ranking_score_threshold,
                },
            });

            Some(Track {
                timestamp,
                user: user.clone(),
                event: event_name.to_string(),
                properties,
                ..Default::default()
            })
        }
    }
}

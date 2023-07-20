use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs;
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::http::header::USER_AGENT;
use actix_web::HttpRequest;
use byte_unit::Byte;
use http::header::CONTENT_TYPE;
use index_scheduler::IndexScheduler;
use meilisearch_auth::{AuthController, AuthFilter};
use meilisearch_types::InstanceUid;
use once_cell::sync::Lazy;
use regex::Regex;
use segment::message::{Identify, Track, User};
use segment::{AutoBatcher, Batcher, HttpClient};
use serde::Serialize;
use serde_json::{json, Value};
use sysinfo::{DiskExt, System, SystemExt};
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use super::{
    config_user_id_path, DocumentDeletionKind, DocumentFetchKind, MEILISEARCH_CONFIG_PATH,
};
use crate::analytics::Analytics;
use crate::option::{default_http_addr, IndexerOpts, MaxMemory, MaxThreads, ScheduleSnapshot};
use crate::routes::indexes::documents::UpdateDocumentsQuery;
use crate::routes::indexes::facet_search::FacetSearchQuery;
use crate::routes::tasks::TasksFilterQuery;
use crate::routes::{create_all_stats, Stats};
use crate::search::{
    FacetSearchResult, MatchingStrategy, SearchQuery, SearchQueryWithIndex, SearchResult,
    DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER, DEFAULT_HIGHLIGHT_POST_TAG,
    DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT,
};
use crate::Opt;

const ANALYTICS_HEADER: &str = "X-Meilisearch-Client";

/// Write the instance-uid in the `data.ms` and in `~/.config/MeiliSearch/path-to-db-instance-uid`. Ignore the errors.
fn write_user_id(db_path: &Path, user_id: &InstanceUid) {
    let _ = fs::write(db_path.join("instance-uid"), user_id.to_string());
    if let Some((meilisearch_config_path, user_id_path)) =
        MEILISEARCH_CONFIG_PATH.as_ref().zip(config_user_id_path(db_path))
    {
        let _ = fs::create_dir_all(&meilisearch_config_path);
        let _ = fs::write(user_id_path, user_id.to_string());
    }
}

const SEGMENT_API_KEY: &str = "P3FWhhEsJiEDCuEHpmcN9DHcK4hVfBvb";

pub fn extract_user_agents(request: &HttpRequest) -> Vec<String> {
    request
        .headers()
        .get(ANALYTICS_HEADER)
        .or_else(|| request.headers().get(USER_AGENT))
        .map(|header| header.to_str().ok())
        .flatten()
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
    AggregatePostMultiSearch(MultiSearchAggregator),
    AggregatePostFacetSearch(FacetSearchAggregator),
    AggregateAddDocuments(DocumentsAggregator),
    AggregateDeleteDocuments(DocumentsDeletionAggregator),
    AggregateUpdateDocuments(DocumentsAggregator),
    AggregateGetFetchDocuments(DocumentsFetchAggregator),
    AggregatePostFetchDocuments(DocumentsFetchAggregator),
    AggregateTasks(TasksAggregator),
    AggregateHealth(HealthAggregator),
}

pub struct SegmentAnalytics {
    instance_uid: InstanceUid,
    sender: Sender<AnalyticsMsg>,
    user: User,
}

impl SegmentAnalytics {
    pub async fn new(
        opt: &Opt,
        index_scheduler: Arc<IndexScheduler>,
        auth_controller: Arc<AuthController>,
    ) -> Arc<dyn Analytics> {
        let instance_uid = super::find_user_id(&opt.db_path);
        let first_time_run = instance_uid.is_none();
        let instance_uid = instance_uid.unwrap_or_else(|| Uuid::new_v4());
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
            get_fetch_documents_aggregator: DocumentsFetchAggregator::default(),
            post_fetch_documents_aggregator: DocumentsFetchAggregator::default(),
            get_tasks_aggregator: TasksAggregator::default(),
            health_aggregator: HealthAggregator::default(),
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
        let user_agent = request.map(|req| extract_user_agents(req));

        send["user-agent"] = json!(user_agent);
        let event = Track {
            user: self.user.clone(),
            event: event_name.clone(),
            properties: send,
            ..Default::default()
        };
        let _ = self.sender.try_send(AnalyticsMsg::BatchMessage(event.into()));
    }

    fn get_search(&self, aggregate: SearchAggregator) {
        let _ = self.sender.try_send(AnalyticsMsg::AggregateGetSearch(aggregate));
    }

    fn post_search(&self, aggregate: SearchAggregator) {
        let _ = self.sender.try_send(AnalyticsMsg::AggregatePostSearch(aggregate));
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

    fn get_fetch_documents(&self, documents_query: &DocumentFetchKind, request: &HttpRequest) {
        let aggregate = DocumentsFetchAggregator::from_query(documents_query, request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregateGetFetchDocuments(aggregate));
    }

    fn post_fetch_documents(&self, documents_query: &DocumentFetchKind, request: &HttpRequest) {
        let aggregate = DocumentsFetchAggregator::from_query(documents_query, request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregatePostFetchDocuments(aggregate));
    }

    fn get_tasks(&self, query: &TasksFilterQuery, request: &HttpRequest) {
        let aggregate = TasksAggregator::from_query(query, request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregateTasks(aggregate));
    }

    fn health_seen(&self, request: &HttpRequest) {
        let aggregate = HealthAggregator::from_query(request);
        let _ = self.sender.try_send(AnalyticsMsg::AggregateHealth(aggregate));
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
    experimental_enable_metrics: bool,
    experimental_reduce_indexing_memory_usage: bool,
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
            experimental_enable_metrics,
            experimental_reduce_indexing_memory_usage,
            http_addr,
            master_key: _,
            env,
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
            #[cfg(all(not(debug_assertions), feature = "analytics"))]
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
            experimental_enable_metrics,
            experimental_reduce_indexing_memory_usage,
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
    get_fetch_documents_aggregator: DocumentsFetchAggregator,
    post_fetch_documents_aggregator: DocumentsFetchAggregator,
    get_tasks_aggregator: TasksAggregator,
    health_aggregator: HealthAggregator,
}

impl Segment {
    fn compute_traits(opt: &Opt, stats: Stats) -> Value {
        static FIRST_START_TIMESTAMP: Lazy<Instant> = Lazy::new(Instant::now);
        static SYSTEM: Lazy<Value> = Lazy::new(|| {
            let mut sys = System::new_all();
            sys.refresh_all();
            let kernel_version = sys
                .kernel_version()
                .map(|k| k.split_once("-").map(|(k, _)| k.to_string()))
                .flatten();
            json!({
                    "distribution": sys.name(),
                    "kernel_version": kernel_version,
                    "cores": sys.cpus().len(),
                    "ram_size": sys.total_memory(),
                    "disk_size": sys.disks().iter().map(|disk| disk.total_space()).max(),
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
                        Some(AnalyticsMsg::AggregateGetFetchDocuments(agreg)) => self.get_fetch_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregatePostFetchDocuments(agreg)) => self.post_fetch_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateTasks(agreg)) => self.get_tasks_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateHealth(agreg)) => self.health_aggregator.aggregate(agreg),
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
            let version = if let Some(prototype) = crate::prototype_name() {
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
            get_fetch_documents_aggregator,
            post_fetch_documents_aggregator,
            get_tasks_aggregator,
            health_aggregator,
        } = self;

        if let Some(get_search) =
            take(get_search_aggregator).into_event(&user, "Documents Searched GET")
        {
            let _ = self.batcher.push(get_search).await;
        }
        if let Some(post_search) =
            take(post_search_aggregator).into_event(&user, "Documents Searched POST")
        {
            let _ = self.batcher.push(post_search).await;
        }
        if let Some(post_multi_search) = take(post_multi_search_aggregator)
            .into_event(&user, "Documents Searched by Multi-Search POST")
        {
            let _ = self.batcher.push(post_multi_search).await;
        }
        if let Some(post_facet_search) =
            take(post_facet_search_aggregator).into_event(&user, "Facet Searched POST")
        {
            let _ = self.batcher.push(post_facet_search).await;
        }
        if let Some(add_documents) =
            take(add_documents_aggregator).into_event(&user, "Documents Added")
        {
            let _ = self.batcher.push(add_documents).await;
        }
        if let Some(delete_documents) =
            take(delete_documents_aggregator).into_event(&user, "Documents Deleted")
        {
            let _ = self.batcher.push(delete_documents).await;
        }
        if let Some(update_documents) =
            take(update_documents_aggregator).into_event(&user, "Documents Updated")
        {
            let _ = self.batcher.push(update_documents).await;
        }
        if let Some(get_fetch_documents) =
            take(get_fetch_documents_aggregator).into_event(&user, "Documents Fetched GET")
        {
            let _ = self.batcher.push(get_fetch_documents).await;
        }
        if let Some(post_fetch_documents) =
            take(post_fetch_documents_aggregator).into_event(&user, "Documents Fetched POST")
        {
            let _ = self.batcher.push(post_fetch_documents).await;
        }
        if let Some(get_tasks) = take(get_tasks_aggregator).into_event(&user, "Tasks Seen") {
            let _ = self.batcher.push(get_tasks).await;
        }
        if let Some(health) = take(health_aggregator).into_event(&user, "Health Seen") {
            let _ = self.batcher.push(health).await;
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
    time_spent: BinaryHeap<usize>,

    // sort
    sort_with_geo_point: bool,
    // every time a request has a filter, this field must be incremented by the number of terms it contains
    sort_sum_of_criteria_terms: usize,
    // every time a request has a filter, this field must be incremented by one
    sort_total_number_of_criteria: usize,

    // filter
    filter_with_geo_radius: bool,
    filter_with_geo_bounding_box: bool,
    // every time a request has a filter, this field must be incremented by the number of terms it contains
    filter_sum_of_criteria_terms: usize,
    // every time a request has a filter, this field must be incremented by one
    filter_total_number_of_criteria: usize,
    used_syntax: HashMap<String, usize>,

    // q
    // The maximum number of terms in a q request
    max_terms_number: usize,

    // vector
    // The maximum number of floats in a vector request
    max_vector_size: usize,

    // every time a search is done, we increment the counter linked to the used settings
    matching_strategy: HashMap<String, usize>,

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
}

impl SearchAggregator {
    pub fn from_query(query: &SearchQuery, request: &HttpRequest) -> Self {
        let timestamp = Some(OffsetDateTime::now_utc());
        let user_agents = extract_user_agents(request).into_iter().collect::<HashSet<String>>();
        let mut ret = Self {
            timestamp,
            user_agents,
            total_received: 1,
            query_parameter_count: query.count_parameters(),
            query_advanced_syntax_used: query.advanced_syntax_used,
            query_filter_chains: !query.filter.is_empty(),
            query_facets: query.facets.map_or(false, |facets| !facets.is_empty()),
            query_query: query.query.as_ref().map(|search_query| search_query.to_string()),
            query_searchable_attributes: query.searchable_attributes.clone(),
            query_retrieve_attributes: query.retrieve_attributes.clone(),
            query_sort_criteria: query.sort_criteria.clone(),
            query_pagination: query.pagination.clone(),
            query_filter: query.filter.clone(),
        };

        ret
    }

    pub fn succeed(&mut self, _result: &SearchResult) {
        self.total_succeeded = self.total_succeeded.saturating_add(1);
    }

    /// Aggregate one [SearchAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        if self.timestamp.is_none() {
            self.timestamp = other.timestamp;
        }
        self.total_received = self.total_received.saturating_add(other.total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(other.total_succeeded);
        self.user_agents.extend(other.user_agents);

        self.query_parameter_count += other.query_parameter_count;
        self.query_advanced_syntax_used |= other.query_advanced_syntax_used;
        self.query_filter_chains |= other.query_filter_chains;
        self.query_facets |= other.query_facets;

        if self.query_query.is_none() {
            self.query_query = other.query_query;
        }
        if self.query_searchable_attributes.is_none() {
            self.query_searchable_attributes = other.query_searchable_attributes;
        }
        if self.query_retrieve_attributes.is_none() {
            self.query_retrieve_attributes = other.query_retrieve_attributes;
        }
        if self.query_sort_criteria.is_none() {
            self.query_sort_criteria = other.query_sort_criteria;
        }
        if self.query_pagination.is_none() {
            self.query_pagination = other.query_pagination;
        }
        if self.query_filter.is_none() {
            self.query_filter = other.query_filter;
        }
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        if self.total_received == 0 {
            None
        } else {
            let properties = json!({
                "user-agent": self.user_agents,
                "requests": {
                    "total_succeeded": self.total_succeeded,
                    "total_failed": self.total_received.saturating_sub(self.total_succeeded),
                    "total_received": self.total_received,
                },
                "query_parameters": {
                    "parameter_count": self.query_parameter_count,
                    "advanced_syntax_used": self.query_advanced_syntax_used,
                    "filter_chains": self.query_filter_chains,
                    "has_facets": self.query_facets,
                    "query": self.query_query,
                    "searchable_attributes": self.query_searchable_attributes,
                    "retrieve_attributes": self.query_retrieve_attributes,
                    "sort_criteria": self.query_sort_criteria,
                    "pagination": self.query_pagination,
                    "filter": self.query_filter,
                },
            });

            Some(Track {
                timestamp: self.timestamp,
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

    // context
    user_agents: HashSet<String>,
}

impl MultiSearchAggregator {
    pub fn from_queries(
        queries: Vec<SearchQuery>,
        request: &HttpRequest,
        index_uids: Option<&SearchQuery>,
    ) -> Self {
        let timestamp = Some(OffsetDateTime::now_utc());
        let user_agents = extract_user_agents(request).into_iter().collect::<HashSet<String>>();
        let mut ret = Self {
            timestamp,
            user_agents,
            total_received: queries.len(),
            index_uids: index_uids.map(|q| q.uids.clone()),
            facet_names: None,
            has_facets: false,
            advanced_syntax_used: false,
            filter_chains: false,
        };

        for query in queries {
            ret.advanced_syntax_used |= query.advanced_syntax_used;
            ret.filter_chains |= !query.filter.is_empty();
            ret.has_facets |= query.facets.map_or(false, |facets| !facets.is_empty());
        }

        ret
    }

    pub fn succeed(&mut self, result: &SearchResult) {
        self.total_succeeded = self.total_succeeded.saturating_add(1);
        self.time_spent.push(result.processing_time_ms as usize);
    }

    /// Aggregate one [MultiSearchAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        if self.timestamp.is_none() {
            self.timestamp = other.timestamp;
        }
        self.user_agents.extend(other.user_agents);

        self.total_received = self.total_received.saturating_add(other.total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(other.total_succeeded);
        self.time_spent.append(&mut other.time_spent);

        self.advanced_syntax_used |= other.advanced_syntax_used;
        self.filter_chains |= other.filter_chains;

        if !self.has_facets {
            self.has_facets = other.has_facets;
            if let Some(facet_names) = other.facet_names {
                self.facet_names = Some(facet_names);
            }
        }
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        if self.total_received == 0 {
            None
        } else {
            // the index of the 99th percentage of value
            let percentile_99th = 0.99 * (self.total_succeeded as f64 - 1.) + 1.;
            // we get all the values in a sorted manner
            let time_spent = self.time_spent.into_sorted_vec();
            // We are only interested by the slowest value of the 99th fastest results
            let time_spent = time_spent.get(percentile_99th as usize);

            let properties = json!({
                "user-agent": self.user_agents,
                "requests": {
                    "99th_response_time":  time_spent.map(|t| format!("{:.2}", t)),
                    "total_succeeded": self.total_succeeded,
                    "total_failed": self.total_received.saturating_sub(self.total_succeeded),
                    "total_received": self.total_received,
                },
                "query_parameters": {
                    "index_uids": self.index_uids,
                    "has_facets": self.has_facets,
                    "advanced_syntax_used": self.advanced_syntax_used,
                    "filter_chains": self.filter_chains,
                },
            });

            Some(Track {
                timestamp: self.timestamp,
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
    pub fn from_query(query: &FacetSearchQuery, request: &HttpRequest) -> Self {
        let FacetSearchQuery {
            facet_name,
            vector,
            q,
            filter,
            matching_strategy,
            attributes_to_search_on,
            ..
        } = query;

        let user_agents = extract_user_agents(request).into_iter().collect::<HashSet<String>>();

        let facet_names = Some(facet_name.clone()).into_iter().collect::<HashSet<String>>();

        let additional_search_parameters_provided = q.is_some()
            || vector.is_some()
            || filter.is_some()
            || *matching_strategy != MatchingStrategy::default()
            || attributes_to_search_on.is_some();

        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            total_received: 1,
            user_agents,
            facet_names,
            additional_search_parameters_provided,
            ..Default::default()
        }
    }

    pub fn succeed(&mut self, result: &FacetSearchResult) {
        self.total_succeeded = self.total_succeeded.saturating_add(1);
        self.time_spent.push(result.processing_time_ms as usize);
    }

    /// Aggregate one [SearchAggregator] into another.
    pub fn aggregate(&mut self, mut other: Self) {
        if self.timestamp.is_none() {
            self.timestamp = other.timestamp;
        }

        // context
        for user_agent in other.user_agents.into_iter() {
            self.user_agents.insert(user_agent);
        }

        // request
        self.total_received = self.total_received.saturating_add(other.total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(other.total_succeeded);
        self.time_spent.append(&mut other.time_spent);

        // facet_names
        for facet_name in other.facet_names.into_iter() {
            self.facet_names.insert(facet_name);
        }

        // additional_search_parameters_provided
        self.additional_search_parameters_provided = self.additional_search_parameters_provided
            | other.additional_search_parameters_provided;
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        if self.total_received == 0 {
            None
        } else {
            // the index of the 99th percentage of value
            let percentile_99th = 0.99 * (self.total_succeeded as f64 - 1.) + 1.;
            // we get all the values in a sorted manner
            let time_spent = self.time_spent.into_sorted_vec();
            // We are only interested by the slowest value of the 99th fastest results
            let time_spent = time_spent.get(percentile_99th as usize);

            let properties = json!({
                "user-agent": self.user_agents,
                "requests": {
                    "99th_response_time":  time_spent.map(|t| format!("{:.2}", t)),
                    "total_succeeded": self.total_succeeded,
                    "total_failed": self.total_received.saturating_sub(self.total_succeeded), // just to be sure we never panics
                    "total_received": self.total_received,
                },
                "facets": {
                    "total_distinct_facet_count": self.facet_names.len(),
                    "additional_search_parameters_provided": self.additional_search_parameters_provided,
                },
            });

            Some(Track {
                timestamp: self.timestamp,
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
        let user_agents = extract_user_agents(request).into_iter().collect::<HashSet<String>>();

        let primary_keys = if let Some(primary_key) = &documents_query.primary_key {
            vec![primary_key.clone()].into_iter().collect()
        } else {
            HashSet::new()
        };

        let content_types = vec![request
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|s| s.to_str().ok())
            .unwrap_or("unknown")
            .to_string()]
        .into_iter()
        .collect::<HashSet<String>>();

        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            updated: true,
            user_agents,
            primary_keys,
            content_types,
            index_creation,
        }
    }

    /// Aggregate one [DocumentsAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        if self.timestamp.is_none() {
            self.timestamp = other.timestamp;
        }

        self.updated |= other.updated;
        // we can't create a union because there is no `into_union` method
        for user_agent in other.user_agents {
            self.user_agents.insert(user_agent);
        }
        for primary_key in other.primary_keys {
            self.primary_keys.insert(primary_key);
        }
        for content_type in other.content_types {
            self.content_types.insert(content_type);
        }
        self.index_creation |= other.index_creation;
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        if !self.updated {
            None
        } else {
            let properties = json!({
                "user-agent": self.user_agents,
                "payload_type": self.content_types,
                "primary_key": self.primary_keys,
                "index_creation": self.index_creation,
            });

            Some(Track {
                timestamp: self.timestamp,
                user: user.clone(),
                event: event_name.to_string(),
                properties,
                ..Default::default()
            })
        }
    }
}

#[derive(Default, Serialize)]
pub struct DocumentsDeletionAggregator {
    #[serde(skip)]
    timestamp: Option<OffsetDateTime>,

    // context
    #[serde(rename = "user-agent")]
    user_agents: HashSet<String>,

    total_received: usize,
    per_document_id: bool,
    clear_all: bool,
    per_batch: bool,
    per_filter: bool,
}

impl DocumentsDeletionAggregator {
    pub fn from_query(kind: DocumentDeletionKind, request: &HttpRequest) -> Self {
        let (per_document_id, clear_all, per_batch, per_filter) = match kind {
            DocumentDeletionKind::PerDocumentId => (true, false, false, false),
            DocumentDeletionKind::ClearAll => (false, true, false, false),
            DocumentDeletionKind::PerBatch => (false, false, true, false),
            DocumentDeletionKind::PerFilter => (false, false, false, true),
        };

        let user_agents = extract_user_agents(request).into_iter().collect::<HashSet<String>>();
        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            user_agents,
            total_received: 1,
            per_document_id,
            clear_all,
            per_batch,
            per_filter,
        }
    }

    /// Aggregate one [DocumentsAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        if self.timestamp.is_none() {
            self.timestamp = other.timestamp;
        }

        // we can't create a union because there is no `into_union` method
        for user_agent in other.user_agents {
            self.user_agents.insert(user_agent);
        }
        self.total_received = self.total_received.saturating_add(other.total_received);
        self.per_document_id |= other.per_document_id;
        self.clear_all |= other.clear_all;
        self.per_batch |= other.per_batch;
        self.per_filter |= other.per_filter;
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
pub struct TasksAggregator {
    #[serde(skip)]
    timestamp: Option<OffsetDateTime>,

    // context
    #[serde(rename = "user-agent")]
    user_agents: HashSet<String>,

    filtered_by_uid: bool,
    filtered_by_index_uid: bool,
    filtered_by_type: bool,
    filtered_by_status: bool,
    filtered_by_canceled_by: bool,
    filtered_by_before_enqueued_at: bool,
    filtered_by_after_enqueued_at: bool,
    filtered_by_before_started_at: bool,
    filtered_by_after_started_at: bool,
    filtered_by_before_finished_at: bool,
    filtered_by_after_finished_at: bool,
    total_received: usize,
}

impl TasksAggregator {
    pub fn from_query(query: &TasksFilterQuery, request: &HttpRequest) -> Self {
        let user_agents = extract_user_agents(request).into_iter().collect::<HashSet<String>>();

        let filtered_by_uid = query.uids.is_some();
        let filtered_by_index_uid = query.index_uids.is_some();
        let filtered_by_type = query.types.is_some();
        let filtered_by_status = query.statuses.is_some();
        let filtered_by_canceled_by = query.canceled_by.is_some();
        let filtered_by_before_enqueued_at = query.before_enqueued_at.is_some();
        let filtered_by_after_enqueued_at = query.after_enqueued_at.is_some();
        let filtered_by_before_started_at = query.before_started_at.is_some();
        let filtered_by_after_started_at = query.after_started_at.is_some();
        let filtered_by_before_finished_at = query.before_finished_at.is_some();
        let filtered_by_after_finished_at = query.after_finished_at.is_some();

        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            user_agents,
            filtered_by_uid,
            filtered_by_index_uid,
            filtered_by_type,
            filtered_by_status,
            filtered_by_canceled_by,
            filtered_by_before_enqueued_at,
            filtered_by_after_enqueued_at,
            filtered_by_before_started_at,
            filtered_by_after_started_at,
            filtered_by_before_finished_at,
            filtered_by_after_finished_at,
            total_received: 1,
        }
    }

    /// Aggregate one [DocumentsAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        if self.timestamp.is_none() {
            self.timestamp = other.timestamp;
        }

        // we can't create a union because there is no `into_union` method
        for user_agent in other.user_agents {
            self.user_agents.insert(user_agent);
        }

        self.filtered_by_uid |= other.filtered_by_uid;
        self.filtered_by_index_uid |= other.filtered_by_index_uid;
        self.filtered_by_type |= other.filtered_by_type;
        self.filtered_by_status |= other.filtered_by_status;
        self.filtered_by_canceled_by |= other.filtered_by_canceled_by;
        self.filtered_by_before_enqueued_at |= other.filtered_by_before_enqueued_at;
        self.filtered_by_after_enqueued_at |= other.filtered_by_after_enqueued_at;
        self.filtered_by_before_started_at |= other.filtered_by_before_started_at;
        self.filtered_by_after_started_at |= other.filtered_by_after_started_at;
        self.filtered_by_before_finished_at |= other.filtered_by_before_finished_at;
        self.filtered_by_after_finished_at |= other.filtered_by_after_finished_at;
        self.filtered_by_after_finished_at |= other.filtered_by_after_finished_at;

        self.total_received = self.total_received.saturating_add(other.total_received);
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
pub struct HealthAggregator {
    #[serde(skip)]
    timestamp: Option<OffsetDateTime>,

    // context
    #[serde(rename = "user-agent")]
    user_agents: HashSet<String>,

    total_received: usize,
}

impl HealthAggregator {
    pub fn from_query(request: &HttpRequest) -> Self {
        let user_agents = extract_user_agents(request).into_iter().collect::<HashSet<String>>();

        Self { timestamp: Some(OffsetDateTime::now_utc()), user_agents, total_received: 1 }
    }

    /// Aggregate one [DocumentsAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        if self.timestamp.is_none() {
            self.timestamp = other.timestamp;
        }

        // we can't create a union because there is no `into_union` method
        for user_agent in other.user_agents {
            self.user_agents.insert(user_agent);
        }
        self.total_received = self.total_received.saturating_add(other.total_received);
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

    #[serde(rename = "requests.max_limit")]
    total_received: usize,

    // a call on ../documents/:doc_id
    per_document_id: bool,
    // if a filter was used
    per_filter: bool,

    // pagination
    #[serde(rename = "pagination.max_limit")]
    max_limit: usize,
    #[serde(rename = "pagination.max_offset")]
    max_offset: usize,
}

impl DocumentsFetchAggregator {
    pub fn from_query(query: &DocumentFetchKind, request: &HttpRequest) -> Self {
        let (limit, offset) = match query {
            DocumentFetchKind::PerDocumentId => (1, 0),
            DocumentFetchKind::Normal { limit, offset, .. } => (*limit, *offset),
        };
        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            user_agents: extract_user_agents(request).into_iter().collect(),
            total_received: 1,
            per_document_id: matches!(query, DocumentFetchKind::PerDocumentId),
            per_filter: matches!(query, DocumentFetchKind::Normal { with_filter, .. } if *with_filter),
            max_limit: limit,
            max_offset: offset,
        }
    }

    /// Aggregate one [DocumentsFetchAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        if self.timestamp.is_none() {
            self.timestamp = other.timestamp;
        }
        for user_agent in other.user_agents {
            self.user_agents.insert(user_agent);
        }

        self.total_received = self.total_received.saturating_add(other.total_received);
        self.per_document_id |= other.per_document_id;
        self.per_filter |= other.per_filter;

        self.max_limit = self.max_limit.max(other.max_limit);
        self.max_offset = self.max_offset.max(other.max_offset);
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

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::http::header::USER_AGENT;
use actix_web::HttpRequest;
use byte_unit::Byte;
use http::header::CONTENT_TYPE;
use index_scheduler::IndexScheduler;
use meilisearch_auth::SearchRules;
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

use super::{config_user_id_path, DocumentDeletionKind, MEILISEARCH_CONFIG_PATH};
use crate::analytics::Analytics;
use crate::option::{default_http_addr, IndexerOpts, MaxMemory, MaxThreads, ScheduleSnapshot};
use crate::routes::indexes::documents::UpdateDocumentsQuery;
use crate::routes::tasks::TasksFilterQuery;
use crate::routes::{create_all_stats, Stats};
use crate::search::{
    SearchQuery, SearchResult, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER,
    DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT,
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
    AggregateAddDocuments(DocumentsAggregator),
    AggregateDeleteDocuments(DocumentsDeletionAggregator),
    AggregateUpdateDocuments(DocumentsAggregator),
    AggregateTasks(TasksAggregator),
    AggregateHealth(HealthAggregator),
}

pub struct SegmentAnalytics {
    instance_uid: InstanceUid,
    sender: Sender<AnalyticsMsg>,
    user: User,
}

impl SegmentAnalytics {
    pub async fn new(opt: &Opt, index_scheduler: Arc<IndexScheduler>) -> Arc<dyn Analytics> {
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
            get_search_aggregator: SearchAggregator::default(),
            add_documents_aggregator: DocumentsAggregator::default(),
            delete_documents_aggregator: DocumentsDeletionAggregator::default(),
            update_documents_aggregator: DocumentsAggregator::default(),
            get_tasks_aggregator: TasksAggregator::default(),
            health_aggregator: HealthAggregator::default(),
        });
        tokio::spawn(segment.run(index_scheduler.clone()));

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

        let IndexerOpts { max_indexing_memory, max_indexing_threads } = indexer_options;

        // We're going to override every sensible information.
        // We consider information sensible if it contains a path, an address, or a key.
        Self {
            env,
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
    add_documents_aggregator: DocumentsAggregator,
    delete_documents_aggregator: DocumentsDeletionAggregator,
    update_documents_aggregator: DocumentsAggregator,
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

    async fn run(mut self, index_scheduler: Arc<IndexScheduler>) {
        const INTERVAL: Duration = Duration::from_secs(60 * 60); // one hour
                                                                 // The first batch must be sent after one hour.
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + INTERVAL, INTERVAL);

        loop {
            select! {
                _ = interval.tick() => {
                    self.tick(index_scheduler.clone()).await;
                },
                msg = self.inbox.recv() => {
                    match msg {
                        Some(AnalyticsMsg::BatchMessage(msg)) => drop(self.batcher.push(msg).await),
                        Some(AnalyticsMsg::AggregateGetSearch(agreg)) => self.get_search_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregatePostSearch(agreg)) => self.post_search_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateAddDocuments(agreg)) => self.add_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateDeleteDocuments(agreg)) => self.delete_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateUpdateDocuments(agreg)) => self.update_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateTasks(agreg)) => self.get_tasks_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateHealth(agreg)) => self.health_aggregator.aggregate(agreg),
                        None => (),
                    }
                }
            }
        }
    }

    async fn tick(&mut self, index_scheduler: Arc<IndexScheduler>) {
        if let Ok(stats) = create_all_stats(index_scheduler.into(), &SearchRules::default()) {
            let _ = self
                .batcher
                .push(Identify {
                    context: Some(json!({
                        "app": {
                            "version": env!("CARGO_PKG_VERSION").to_string(),
                        },
                    })),
                    user: self.user.clone(),
                    traits: Self::compute_traits(&self.opt, stats),
                    ..Default::default()
                })
                .await;
        }
        let get_search = std::mem::take(&mut self.get_search_aggregator)
            .into_event(&self.user, "Documents Searched GET");
        let post_search = std::mem::take(&mut self.post_search_aggregator)
            .into_event(&self.user, "Documents Searched POST");
        let add_documents = std::mem::take(&mut self.add_documents_aggregator)
            .into_event(&self.user, "Documents Added");
        let delete_documents = std::mem::take(&mut self.delete_documents_aggregator)
            .into_event(&self.user, "Documents Deleted");
        let update_documents = std::mem::take(&mut self.update_documents_aggregator)
            .into_event(&self.user, "Documents Updated");
        let get_tasks =
            std::mem::take(&mut self.get_tasks_aggregator).into_event(&self.user, "Tasks Seen");
        let health =
            std::mem::take(&mut self.health_aggregator).into_event(&self.user, "Health Seen");

        if let Some(get_search) = get_search {
            let _ = self.batcher.push(get_search).await;
        }
        if let Some(post_search) = post_search {
            let _ = self.batcher.push(post_search).await;
        }
        if let Some(add_documents) = add_documents {
            let _ = self.batcher.push(add_documents).await;
        }
        if let Some(delete_documents) = delete_documents {
            let _ = self.batcher.push(delete_documents).await;
        }
        if let Some(update_documents) = update_documents {
            let _ = self.batcher.push(update_documents).await;
        }
        if let Some(get_tasks) = get_tasks {
            let _ = self.batcher.push(get_tasks).await;
        }
        if let Some(health) = health {
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
    // every time a request has a filter, this field must be incremented by the number of terms it contains
    filter_sum_of_criteria_terms: usize,
    // every time a request has a filter, this field must be incremented by one
    filter_total_number_of_criteria: usize,
    used_syntax: HashMap<String, usize>,

    // q
    // The maximum number of terms in a q request
    max_terms_number: usize,

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
}

impl SearchAggregator {
    pub fn from_query(query: &SearchQuery, request: &HttpRequest) -> Self {
        let mut ret = Self::default();
        ret.timestamp = Some(OffsetDateTime::now_utc());

        ret.total_received = 1;
        ret.user_agents = extract_user_agents(request).into_iter().collect();

        if let Some(ref sort) = query.sort {
            ret.sort_total_number_of_criteria = 1;
            ret.sort_with_geo_point = sort.iter().any(|s| s.contains("_geoPoint("));
            ret.sort_sum_of_criteria_terms = sort.len();
        }

        if let Some(ref filter) = query.filter {
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
            ret.filter_sum_of_criteria_terms = RE.split(&stringified_filters).count();
        }

        if let Some(ref q) = query.q {
            ret.max_terms_number = q.split_whitespace().count();
        }

        if query.is_finite_pagination() {
            let limit = query.hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
            ret.max_limit = limit;
            ret.max_offset = query.page.unwrap_or(1).saturating_sub(1) * limit;
            ret.finite_pagination = 1;
        } else {
            ret.max_limit = query.limit;
            ret.max_offset = query.offset;
            ret.finite_pagination = 0;
        }

        ret.matching_strategy.insert(format!("{:?}", query.matching_strategy), 1);

        ret.highlight_pre_tag = query.highlight_pre_tag != DEFAULT_HIGHLIGHT_PRE_TAG();
        ret.highlight_post_tag = query.highlight_post_tag != DEFAULT_HIGHLIGHT_POST_TAG();
        ret.crop_marker = query.crop_marker != DEFAULT_CROP_MARKER();
        ret.crop_length = query.crop_length != DEFAULT_CROP_LENGTH();
        ret.show_matches_position = query.show_matches_position;

        ret
    }

    pub fn succeed(&mut self, result: &SearchResult) {
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

        // sort
        self.sort_with_geo_point |= other.sort_with_geo_point;
        self.sort_sum_of_criteria_terms =
            self.sort_sum_of_criteria_terms.saturating_add(other.sort_sum_of_criteria_terms);
        self.sort_total_number_of_criteria =
            self.sort_total_number_of_criteria.saturating_add(other.sort_total_number_of_criteria);

        // filter
        self.filter_with_geo_radius |= other.filter_with_geo_radius;
        self.filter_sum_of_criteria_terms =
            self.filter_sum_of_criteria_terms.saturating_add(other.filter_sum_of_criteria_terms);
        self.filter_total_number_of_criteria = self
            .filter_total_number_of_criteria
            .saturating_add(other.filter_total_number_of_criteria);
        for (key, value) in other.used_syntax.into_iter() {
            let used_syntax = self.used_syntax.entry(key).or_insert(0);
            *used_syntax = used_syntax.saturating_add(value);
        }
        // q
        self.max_terms_number = self.max_terms_number.max(other.max_terms_number);

        // pagination
        self.max_limit = self.max_limit.max(other.max_limit);
        self.max_offset = self.max_offset.max(other.max_offset);
        self.finite_pagination += other.finite_pagination;

        // formatting
        self.max_attributes_to_retrieve =
            self.max_attributes_to_retrieve.max(other.max_attributes_to_retrieve);
        self.max_attributes_to_highlight =
            self.max_attributes_to_highlight.max(other.max_attributes_to_highlight);
        self.highlight_pre_tag |= other.highlight_pre_tag;
        self.highlight_post_tag |= other.highlight_post_tag;
        self.max_attributes_to_crop = self.max_attributes_to_crop.max(other.max_attributes_to_crop);
        self.crop_marker |= other.crop_marker;
        self.show_matches_position |= other.show_matches_position;
        self.crop_length |= other.crop_length;

        // facets
        self.facets_sum_of_terms =
            self.facets_sum_of_terms.saturating_add(other.facets_sum_of_terms);
        self.facets_total_number_of_facets =
            self.facets_total_number_of_facets.saturating_add(other.facets_total_number_of_facets);

        // matching strategy
        for (key, value) in other.matching_strategy.into_iter() {
            let matching_strategy = self.matching_strategy.entry(key).or_insert(0);
            *matching_strategy = matching_strategy.saturating_add(value);
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
                    "total_failed": self.total_received.saturating_sub(self.total_succeeded), // just to be sure we never panics
                    "total_received": self.total_received,
                },
                "sort": {
                    "with_geoPoint": self.sort_with_geo_point,
                    "avg_criteria_number": format!("{:.2}", self.sort_sum_of_criteria_terms as f64 / self.sort_total_number_of_criteria as f64),
                },
                "filter": {
                   "with_geoRadius": self.filter_with_geo_radius,
                   "avg_criteria_number": format!("{:.2}", self.filter_sum_of_criteria_terms as f64 / self.filter_total_number_of_criteria as f64),
                   "most_used_syntax": self.used_syntax.iter().max_by_key(|(_, v)| *v).map(|(k, _)| json!(k)).unwrap_or_else(|| json!(null)),
                },
                "q": {
                   "max_terms_number": self.max_terms_number,
                },
                "pagination": {
                   "max_limit": self.max_limit,
                   "max_offset": self.max_offset,
                   "most_used_navigation": if self.finite_pagination > (self.total_received / 2) { "exhaustive" } else { "estimated" },
                },
                "formatting": {
                    "max_attributes_to_retrieve": self.max_attributes_to_retrieve,
                    "max_attributes_to_highlight": self.max_attributes_to_highlight,
                    "highlight_pre_tag": self.highlight_pre_tag,
                    "highlight_post_tag": self.highlight_post_tag,
                    "max_attributes_to_crop": self.max_attributes_to_crop,
                    "crop_marker": self.crop_marker,
                    "show_matches_position": self.show_matches_position,
                    "crop_length": self.crop_length,
                },
                "facets": {
                    "avg_facets_number": format!("{:.2}", self.facets_sum_of_terms as f64 / self.facets_total_number_of_facets as f64),
                },
                "matching_strategy": {
                    "most_used_strategy": self.matching_strategy.iter().max_by_key(|(_, v)| *v).map(|(k, _)| json!(k)).unwrap_or_else(|| json!(null)),
                }
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
        let mut ret = Self::default();
        ret.timestamp = Some(OffsetDateTime::now_utc());

        ret.updated = true;
        ret.user_agents = extract_user_agents(request).into_iter().collect();
        if let Some(primary_key) = documents_query.primary_key.clone() {
            ret.primary_keys.insert(primary_key);
        }
        let content_type = request
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|s| s.to_str().ok())
            .unwrap_or("unknown")
            .to_string();
        ret.content_types.insert(content_type);
        ret.index_creation = index_creation;

        ret
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
}

impl DocumentsDeletionAggregator {
    pub fn from_query(kind: DocumentDeletionKind, request: &HttpRequest) -> Self {
        let mut ret = Self::default();
        ret.timestamp = Some(OffsetDateTime::now_utc());

        ret.user_agents = extract_user_agents(request).into_iter().collect();
        ret.total_received = 1;
        match kind {
            DocumentDeletionKind::PerDocumentId => ret.per_document_id = true,
            DocumentDeletionKind::ClearAll => ret.clear_all = true,
            DocumentDeletionKind::PerBatch => ret.per_batch = true,
        }

        ret
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
        Self {
            timestamp: Some(OffsetDateTime::now_utc()),
            user_agents: extract_user_agents(request).into_iter().collect(),
            filtered_by_uid: query.uids.is_some(),
            filtered_by_index_uid: query.index_uids.is_some(),
            filtered_by_type: query.types.is_some(),
            filtered_by_status: query.statuses.is_some(),
            filtered_by_canceled_by: query.canceled_by.is_some(),
            filtered_by_before_enqueued_at: query.before_enqueued_at.is_some(),
            filtered_by_after_enqueued_at: query.after_enqueued_at.is_some(),
            filtered_by_before_started_at: query.before_started_at.is_some(),
            filtered_by_after_started_at: query.after_started_at.is_some(),
            filtered_by_before_finished_at: query.before_finished_at.is_some(),
            filtered_by_after_finished_at: query.after_finished_at.is_some(),
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
        let mut ret = Self::default();
        ret.timestamp = Some(OffsetDateTime::now_utc());

        ret.user_agents = extract_user_agents(request).into_iter().collect();
        ret.total_received = 1;
        ret
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

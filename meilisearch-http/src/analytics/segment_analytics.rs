use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::http::header::USER_AGENT;
use actix_web::HttpRequest;
use http::header::CONTENT_TYPE;
use meilisearch_lib::index::{SearchQuery, SearchResult};
use meilisearch_lib::index_controller::Stats;
use meilisearch_lib::MeiliSearch;
use once_cell::sync::Lazy;
use regex::Regex;
use segment::message::{Identify, Track, User};
use segment::{AutoBatcher, Batcher, HttpClient};
use serde_json::{json, Value};
use sysinfo::{DiskExt, System, SystemExt};
use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use crate::analytics::Analytics;
use crate::routes::indexes::documents::UpdateDocumentsQuery;
use crate::Opt;

use super::{config_user_id_path, MEILISEARCH_CONFIG_PATH};

/// Write the instance-uid in the `data.ms` and in `~/.config/MeiliSearch/path-to-db-instance-uid`. Ignore the errors.
fn write_user_id(db_path: &Path, user_id: &str) {
    let _ = fs::write(db_path.join("instance-uid"), user_id.as_bytes());
    if let Some((meilisearch_config_path, user_id_path)) = MEILISEARCH_CONFIG_PATH
        .as_ref()
        .zip(config_user_id_path(db_path))
    {
        let _ = fs::create_dir_all(&meilisearch_config_path);
        let _ = fs::write(user_id_path, user_id.as_bytes());
    }
}

const SEGMENT_API_KEY: &str = "P3FWhhEsJiEDCuEHpmcN9DHcK4hVfBvb";

pub fn extract_user_agents(request: &HttpRequest) -> Vec<String> {
    request
        .headers()
        .get(USER_AGENT)
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
    AggregateUpdateDocuments(DocumentsAggregator),
}

pub struct SegmentAnalytics {
    sender: Sender<AnalyticsMsg>,
    user: User,
}

impl SegmentAnalytics {
    pub async fn new(opt: &Opt, meilisearch: &MeiliSearch) -> (Arc<dyn Analytics>, String) {
        let user_id = super::find_user_id(&opt.db_path);
        let first_time_run = user_id.is_none();
        let user_id = user_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        write_user_id(&opt.db_path, &user_id);

        let client = HttpClient::default();
        let user = User::UserId { user_id };
        let mut batcher = AutoBatcher::new(client, Batcher::new(None), SEGMENT_API_KEY.to_string());

        // If Meilisearch is Launched for the first time:
        // 1. Send an event Launched associated to the user `total_launch`.
        // 2. Batch an event Launched with the real instance-id and send it in one hour.
        if first_time_run {
            let _ = batcher
                .push(Track {
                    user: User::UserId {
                        user_id: "total_launch".to_string(),
                    },
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
            update_documents_aggregator: DocumentsAggregator::default(),
        });
        tokio::spawn(segment.run(meilisearch.clone()));

        let this = Self {
            sender,
            user: user.clone(),
        };

        (Arc::new(this), user.to_string())
    }
}

impl super::Analytics for SegmentAnalytics {
    fn publish(&self, event_name: String, mut send: Value, request: Option<&HttpRequest>) {
        let user_agent = request
            .map(|req| req.headers().get(USER_AGENT))
            .flatten()
            .map(|header| header.to_str().unwrap_or("unknown"))
            .map(|s| s.split(';').map(str::trim).collect::<Vec<&str>>());

        send["user-agent"] = json!(user_agent);
        let event = Track {
            user: self.user.clone(),
            event: event_name.clone(),
            properties: send,
            ..Default::default()
        };
        let _ = self
            .sender
            .try_send(AnalyticsMsg::BatchMessage(event.into()));
    }

    fn get_search(&self, aggregate: SearchAggregator) {
        let _ = self
            .sender
            .try_send(AnalyticsMsg::AggregateGetSearch(aggregate));
    }

    fn post_search(&self, aggregate: SearchAggregator) {
        let _ = self
            .sender
            .try_send(AnalyticsMsg::AggregatePostSearch(aggregate));
    }

    fn add_documents(
        &self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    ) {
        let aggregate = DocumentsAggregator::from_query(documents_query, index_creation, request);
        let _ = self
            .sender
            .try_send(AnalyticsMsg::AggregateAddDocuments(aggregate));
    }

    fn update_documents(
        &self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    ) {
        let aggregate = DocumentsAggregator::from_query(documents_query, index_creation, request);
        let _ = self
            .sender
            .try_send(AnalyticsMsg::AggregateUpdateDocuments(aggregate));
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
    update_documents_aggregator: DocumentsAggregator,
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
                    "cores": sys.processors().len(),
                    "ram_size": sys.total_memory(),
                    "disk_size": sys.disks().iter().map(|disk| disk.total_space()).max(),
                    "server_provider": std::env::var("MEILI_SERVER_PROVIDER").ok(),
            })
        });
        let infos = json!({
            "env": opt.env.clone(),
            "has_snapshot": opt.schedule_snapshot,
        });

        let number_of_documents = stats
            .indexes
            .values()
            .map(|index| index.number_of_documents)
            .collect::<Vec<u64>>();

        json!({
            "start_since_days": FIRST_START_TIMESTAMP.elapsed().as_secs() / (60 * 60 * 24), // one day
            "system": *SYSTEM,
            "stats": {
                "database_size": stats.database_size,
                "indexes_number": stats.indexes.len(),
                "documents_number": number_of_documents,
            },
            "infos": infos,
        })
    }

    async fn run(mut self, meilisearch: MeiliSearch) {
        const INTERVAL: Duration = Duration::from_secs(60 * 60); // one hour
                                                                 // The first batch must be sent after one hour.
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + INTERVAL, INTERVAL);

        loop {
            select! {
                _ = interval.tick() => {
                    self.tick(meilisearch.clone()).await;
                },
                msg = self.inbox.recv() => {
                    match msg {
                        Some(AnalyticsMsg::BatchMessage(msg)) => drop(self.batcher.push(msg).await),
                        Some(AnalyticsMsg::AggregateGetSearch(agreg)) => self.get_search_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregatePostSearch(agreg)) => self.post_search_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateAddDocuments(agreg)) => self.add_documents_aggregator.aggregate(agreg),
                        Some(AnalyticsMsg::AggregateUpdateDocuments(agreg)) => self.update_documents_aggregator.aggregate(agreg),
                        None => (),
                    }
                }
            }
        }
    }

    async fn tick(&mut self, meilisearch: MeiliSearch) {
        if let Ok(stats) = meilisearch.get_all_stats(&None).await {
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
        let update_documents = std::mem::take(&mut self.update_documents_aggregator)
            .into_event(&self.user, "Documents Updated");

        if let Some(get_search) = get_search {
            let _ = self.batcher.push(get_search).await;
        }
        if let Some(post_search) = post_search {
            let _ = self.batcher.push(post_search).await;
        }
        if let Some(add_documents) = add_documents {
            let _ = self.batcher.push(add_documents).await;
        }
        if let Some(update_documents) = update_documents {
            let _ = self.batcher.push(update_documents).await;
        }
        let _ = self.batcher.flush().await;
    }
}

#[derive(Default)]
pub struct SearchAggregator {
    // context
    user_agents: HashSet<String>,

    // requests
    total_received: usize,
    total_succeeded: usize,
    time_spent: BinaryHeap<usize>,

    // sort
    sort_with_geo_point: bool,
    // everytime a request has a filter, this field must be incremented by the number of terms it contains
    sort_sum_of_criteria_terms: usize,
    // everytime a request has a filter, this field must be incremented by one
    sort_total_number_of_criteria: usize,

    // filter
    filter_with_geo_radius: bool,
    // everytime a request has a filter, this field must be incremented by the number of terms it contains
    filter_sum_of_criteria_terms: usize,
    // everytime a request has a filter, this field must be incremented by one
    filter_total_number_of_criteria: usize,
    used_syntax: HashMap<String, usize>,

    // q
    // The maximum number of terms in a q request
    max_terms_number: usize,

    // pagination
    max_limit: usize,
    max_offset: usize,
}

impl SearchAggregator {
    pub fn from_query(query: &SearchQuery, request: &HttpRequest) -> Self {
        let mut ret = Self::default();
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
                    if values
                        .iter()
                        .map(|v| v.to_string())
                        .any(|s| RE.is_match(&s))
                    {
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

        ret.max_limit = query.limit;
        ret.max_offset = query.offset.unwrap_or_default();

        ret
    }

    pub fn succeed(&mut self, result: &SearchResult) {
        self.total_succeeded = self.total_succeeded.saturating_add(1);
        self.time_spent.push(result.processing_time_ms as usize);
    }

    /// Aggregate one [SearchAggregator] into another.
    pub fn aggregate(&mut self, mut other: Self) {
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
        self.sort_sum_of_criteria_terms = self
            .sort_sum_of_criteria_terms
            .saturating_add(other.sort_sum_of_criteria_terms);
        self.sort_total_number_of_criteria = self
            .sort_total_number_of_criteria
            .saturating_add(other.sort_total_number_of_criteria);
        // filter
        self.filter_with_geo_radius |= other.filter_with_geo_radius;
        self.filter_sum_of_criteria_terms = self
            .filter_sum_of_criteria_terms
            .saturating_add(other.filter_sum_of_criteria_terms);
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
    }

    pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
        if self.total_received == 0 {
            None
        } else {
            // the index of the 99th percentage of value
            let percentile_99th = 0.99 * (self.total_succeeded as f64 - 1.) + 1.;
            // we get all the values in a sorted manner
            let time_spent = self.time_spent.into_sorted_vec();
            // We are only intersted by the slowest value of the 99th fastest results
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
                },
            });

            Some(Track {
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

        ret.updated = true;
        ret.user_agents = extract_user_agents(request).into_iter().collect();
        if let Some(primary_key) = documents_query.primary_key.clone() {
            ret.primary_keys.insert(primary_key);
        }
        let content_type = request
            .headers()
            .get(CONTENT_TYPE)
            .map(|s| s.to_str().unwrap_or("unkown"))
            .unwrap()
            .to_string();
        ret.content_types.insert(content_type);
        ret.index_creation = index_creation;

        ret
    }

    /// Aggregate one [DocumentsAggregator] into another.
    pub fn aggregate(&mut self, other: Self) {
        self.updated |= other.updated;
        // we can't create a union because there is no `into_union` method
        for user_agent in other.user_agents.into_iter() {
            self.user_agents.insert(user_agent);
        }
        for primary_key in other.primary_keys.into_iter() {
            self.primary_keys.insert(primary_key);
        }
        for content_type in other.content_types.into_iter() {
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
                user: user.clone(),
                event: event_name.to_string(),
                properties,
                ..Default::default()
            })
        }
    }
}

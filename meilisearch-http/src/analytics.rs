use actix_web::HttpRequest;
use meilisearch_lib::index::SearchQuery;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::fs::read_to_string;

use crate::Opt;

// if we are in release mode and the feature analytics was enabled
#[cfg(all(not(debug_assertions), feature = "analytics"))]
mod segment {
    use crate::analytics::Analytics;
    use actix_web::http::header::USER_AGENT;
    use actix_web::HttpRequest;
    use meilisearch_lib::index::SearchQuery;
    use meilisearch_lib::index_controller::Stats;
    use meilisearch_lib::MeiliSearch;
    use once_cell::sync::Lazy;
    use segment::message::{Identify, Track, User};
    use segment::{AutoBatcher, Batcher, HttpClient};
    use serde_json::{json, Value};
    use std::collections::{HashMap, HashSet};
    use std::fmt::Display;
    use std::fs;
    use std::time::{Duration, Instant};
    use sysinfo::{DiskExt, System, SystemExt};
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use crate::Opt;

    const SEGMENT_API_KEY: &str = "vHi89WrNDckHSQssyUJqLvIyp2QFITSC";

    pub struct SegmentAnalytics {
        user: User,
        opt: Opt,
        batcher: Mutex<AutoBatcher>,
        post_search_batcher: Mutex<SearchBatcher>,
        get_search_batcher: Mutex<SearchBatcher>,
    }

    impl SegmentAnalytics {
        fn compute_traits(opt: &Opt, stats: Stats) -> Value {
            static FIRST_START_TIMESTAMP: Lazy<Instant> = Lazy::new(Instant::now);
            const SYSTEM: Lazy<Value> = Lazy::new(|| {
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
                        "disk_size": sys.disks().iter().map(|disk| disk.available_space()).max(),
                        "server_provider": std::env::var("MEILI_SERVER_PROVIDER").ok(),
                })
            });
            let infos = json!({
                "version": env!("CARGO_PKG_VERSION").to_string(),
                "env": opt.env.clone(),
                "has_snapshot": opt.schedule_snapshot,
            });

            let number_of_documents = stats
                .indexes
                .values()
                .map(|index| index.number_of_documents)
                .collect::<Vec<u64>>();

            json!({
                "system": *SYSTEM,
                "stats": {
                    "database_size": stats.database_size,
                    "indexes_number": stats.indexes.len(),
                    "documents_number": number_of_documents,
                    "start_since_days": FIRST_START_TIMESTAMP.elapsed().as_secs() / 60 * 60 * 24, // one day
                },
                "infos": infos,
            })
        }

        pub async fn new(opt: &Opt, meilisearch: &MeiliSearch) -> &'static Self {
            // see if there is already a user-id in the `data.ms`
            let user_id = fs::read_to_string(opt.db_path.join("user-id"))
                .or_else(|_| fs::read_to_string("/tmp/meilisearch-user-id"));
            let first_time_run = user_id.is_err();
            // if not, generate a new user-id and save it to the fs
            let user_id = user_id.unwrap_or_else(|_| Uuid::new_v4().to_string());
            let _ = fs::write(opt.db_path.join("user-id"), user_id.as_bytes());
            let _ = fs::write(
                opt.db_path.join("/tmp/meilisearch-user-id"),
                user_id.as_bytes(),
            );

            let client = HttpClient::default();
            let user = User::UserId {
                user_id: user_id.clone(),
            };
            let batcher = Mutex::new(AutoBatcher::new(
                client,
                Batcher::new(None),
                SEGMENT_API_KEY.to_string(),
            ));
            let segment = Box::new(Self {
                user,
                opt: opt.clone(),
                batcher,
                post_search_batcher: Mutex::new(SearchBatcher::default()),
                get_search_batcher: Mutex::new(SearchBatcher::default()),
            });
            let segment = Box::leak(segment);

            // batch the launched for the first time track event
            if first_time_run {
                segment.publish("Launched".to_string(), json!({}), None);
            }

            // start the runtime tick
            segment.tick(meilisearch.clone());

            segment
        }

        fn tick(&'static self, meilisearch: MeiliSearch) {
            tokio::spawn(async move {
                loop {
                    if let Ok(stats) = meilisearch.get_all_stats().await {
                        let traits = Self::compute_traits(&self.opt, stats);
                        let user = self.user.clone();
                        println!("ANALYTICS: Pushing our identify tick");
                        let _ = self
                            .batcher
                            .lock()
                            .await
                            .push(Identify {
                                user,
                                traits,
                                ..Default::default()
                            })
                            .await;
                    }
                    println!("ANALYTICS: taking the lock on the search batcher");
                    let get_search = std::mem::take(&mut *self.get_search_batcher.lock().await);
                    let get_search = (get_search.total_received != 0).then(|| {
                        get_search
                            .into_event(self.user.clone(), "Document Searched GET".to_string())
                    });
                    let post_search = std::mem::take(&mut *self.post_search_batcher.lock().await);
                    let post_search = (post_search.total_received != 0).then(|| {
                        post_search
                            .into_event(self.user.clone(), "Document Searched POST".to_string())
                    });
                    // keep the lock on the batcher just for these three operations
                    {
                        println!("ANALYTICS: taking the lock on the batcher");
                        let mut batcher = self.batcher.lock().await;
                        if let Some(get_search) = get_search {
                            let _ = batcher.push(get_search).await;
                        }
                        if let Some(post_search) = post_search {
                            let _ = batcher.push(post_search).await;
                        }
                        println!("ANALYTICS: Sending the batch");
                        let _ = batcher.flush().await;
                    }
                    println!("ANALYTICS: sent the batch");
                    tokio::time::sleep(Duration::from_secs(60 * 2)).await; // 2 minutes
                }
            });
        }

        fn start_search(
            &'static self,
            getter: impl Fn(&'static Self) -> &'static Mutex<SearchBatcher> + Send + Sync + 'static,
            query: &SearchQuery,
            request: &HttpRequest,
        ) {
            let user_agent = SearchBatcher::extract_user_agents(request);
            let sorted = query.sort.is_some() as usize;
            let sort_with_geo_point = query
                .sort
                .as_ref()
                .map_or(false, |s| s.iter().any(|s| s.contains("_geoPoint(")));
            let sort_criteria_terms = query.sort.as_ref().map_or(0, |s| s.len());

            // since there is quite a bit of computation made on the filter we are going to do that in the async task
            let filter = query.filter.clone();
            let queried = query.q.is_some();
            let nb_terms = query.q.as_ref().map_or(0, |s| s.split_whitespace().count());

            let max_limit = query.limit;
            let max_offset = query.offset.unwrap_or_default();

            // to avoid blocking the search we are going to do the heavier computation in an async task
            // and take the mutex in the same task
            tokio::spawn(async move {
                let filtered = filter.is_some() as usize;
                let syntax = match filter.as_ref() {
                    Some(Value::String(_)) => "string".to_string(),
                    Some(Value::Array(values)) => {
                        if values.iter().map(|v| v.to_string()).any(|s| {
                            s.contains(['=', '<', '>', '!'].as_ref())
                                || s.contains("_geoRadius")
                                || s.contains("TO")
                        }) {
                            "mixed".to_string()
                        } else {
                            "array".to_string()
                        }
                    }
                    _ => "none".to_string(),
                };
                let stringified_filters = filter.map_or(String::new(), |v| v.to_string());
                let filter_with_geo_radius = stringified_filters.contains("_geoRadius(");
                let filter_number_of_criteria = stringified_filters
                    .split("!=")
                    .map(|s| s.split("<="))
                    .flatten()
                    .map(|s| s.split(">="))
                    .flatten()
                    .map(|s| s.split(['=', '<', '>', '!'].as_ref()))
                    .flatten()
                    .map(|s| s.split("_geoRadius("))
                    .flatten()
                    .map(|s| s.split("TO"))
                    .flatten()
                    .count()
                    - 1;

                println!("Batching a search");
                let mut search_batcher = getter(self).lock().await;
                user_agent.into_iter().for_each(|ua| {
                    search_batcher.user_agents.insert(ua);
                });
                search_batcher.total_received += 1;

                // sort
                search_batcher.sort_with_geo_point |= sort_with_geo_point;
                search_batcher.sort_sum_of_criteria_terms += sort_criteria_terms;
                search_batcher.sort_total_number_of_criteria += sorted;

                // filter
                search_batcher.filter_with_geo_radius |= filter_with_geo_radius;
                search_batcher.filter_sum_of_criteria_terms += filter_number_of_criteria;
                search_batcher.filter_total_number_of_criteria += filtered as usize;
                *search_batcher.used_syntax.entry(syntax).or_insert(0) += 1;

                // q
                search_batcher.sum_of_terms_count += nb_terms;
                search_batcher.total_number_of_q += queried as usize;

                // pagination
                search_batcher.max_limit = search_batcher.max_limit.max(max_limit);
                search_batcher.max_offset = search_batcher.max_offset.max(max_offset);
            });
        }
    }

    #[async_trait::async_trait]
    impl super::Analytics for SegmentAnalytics {
        fn publish(&'static self, event_name: String, send: Value, request: Option<&HttpRequest>) {
            let content_type = request
                .map(|req| req.headers().get(USER_AGENT))
                .flatten()
                .map(|header| header.to_str().unwrap_or("unknown").to_string());

            tokio::spawn(async move {
                println!("ANALYTICS pushing {} in the batcher", event_name);
                let _ = self
                    .batcher
                    .lock()
                    .await
                    .push(Track {
                        user: self.user.clone(),
                        event: event_name.clone(),
                        context: content_type.map(|user_agent| json!({ "user-agent": user_agent.split(";").map(str::trim).collect::<Vec<&str>>() })),
                        properties: send,
                        ..Default::default()
                    })
                    .await;
                println!("ANALYTICS {} pushed", event_name);
            });
        }

        fn start_get_search(&'static self, query: &SearchQuery, request: &HttpRequest) {
            self.start_search(|s| &s.get_search_batcher, query, request)
        }

        fn end_get_search(&'static self, process_time: usize) {
            tokio::spawn(async move {
                let mut search_batcher = self.get_search_batcher.lock().await;
                search_batcher.total_succeeded += 1;
                search_batcher.time_spent.push(process_time);
            });
        }

        fn start_post_search(&'static self, query: &SearchQuery, request: &HttpRequest) {
            self.start_search(|s| &s.post_search_batcher, query, request)
        }

        fn end_post_search(&'static self, process_time: usize) {
            tokio::spawn(async move {
                let mut search_batcher = self.post_search_batcher.lock().await;
                search_batcher.total_succeeded += 1;
                search_batcher.time_spent.push(process_time);
            });
        }
    }

    impl Display for SegmentAnalytics {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.user)
        }
    }

    #[derive(Default)]
    pub struct SearchBatcher {
        // context
        user_agents: HashSet<String>,

        // requests
        total_received: usize,
        total_succeeded: usize,
        time_spent: Vec<usize>,

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
        // everytime a request has a q field, this field must be incremented by the number of terms
        sum_of_terms_count: usize,
        // everytime a request has a q field, this field must be incremented by one
        total_number_of_q: usize,

        // pagination
        max_limit: usize,
        max_offset: usize,
    }

    impl SearchBatcher {
        pub fn extract_user_agents(request: &HttpRequest) -> Vec<String> {
            request
                .headers()
                .get(USER_AGENT)
                .map(|header| header.to_str().ok())
                .flatten()
                .unwrap_or("unknown")
                .split(";")
                .map(str::trim)
                .map(ToString::to_string)
                .collect()
        }

        pub fn into_event(mut self, user: User, event_name: String) -> Track {
            let context = Some(json!({ "user-agent": self.user_agents}));
            let percentile_99th = 0.99 * (self.total_succeeded as f64 - 1.) + 1.;
            self.time_spent.drain(percentile_99th as usize..);

            let properties = json!({
                "requests": {
                    "99th_response_time":  format!("{:.2}", self.time_spent.iter().sum::<usize>() as f64 / self.time_spent.len() as f64),
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
                   "avg_terms_number": format!("{:.2}", self.sum_of_terms_count as f64 / self.total_number_of_q as f64),
                },
                "pagination": {
                   "max_limit": self.max_limit,
                   "max_offset": self.max_offset,
                },
            });

            Track {
                user,
                event: event_name,
                context,
                properties,
                ..Default::default()
            }
        }
    }
}

// if we are in debug mode OR the analytics feature is disabled
#[cfg(any(debug_assertions, not(feature = "analytics")))]
pub type SegmentAnalytics = MockAnalytics;
#[cfg(all(not(debug_assertions), feature = "analytics"))]
pub type SegmentAnalytics = segment::SegmentAnalytics;

pub struct MockAnalytics {
    user: String,
}

impl MockAnalytics {
    pub fn new(opt: &Opt) -> &'static Self {
        let user = read_to_string(opt.db_path.join("user-id"))
            .or_else(|_| read_to_string("/tmp/meilisearch-user-id"))
            .unwrap_or_else(|_| "".to_string());
        let analytics = Box::new(Self { user });
        Box::leak(analytics)
    }
}

#[async_trait::async_trait]
impl Analytics for MockAnalytics {
    // These methods are noop and should be optimized out
    fn publish(&'static self, _event_name: String, _send: Value, _request: Option<&HttpRequest>) {}
    fn start_get_search(&'static self, _query: &SearchQuery, _request: &HttpRequest) {}
    fn end_get_search(&'static self, _process_time: usize) {}
    fn start_post_search(&'static self, _query: &SearchQuery, _request: &HttpRequest) {}
    fn end_post_search(&'static self, _process_time: usize) {}
}

impl Display for MockAnalytics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.user)
    }
}

#[async_trait::async_trait]
pub trait Analytics: Display + Sync + Send {
    /// The method used to publish most analytics that do not need to be batched every hours
    fn publish(&'static self, event_name: String, send: Value, request: Option<&HttpRequest>);

    /// This method should be called to batch a get search request
    fn start_get_search(&'static self, query: &SearchQuery, request: &HttpRequest);
    /// This method should be called once a get search request has succeeded
    fn end_get_search(&'static self, process_time: usize);

    /// This method should be called to batch a get search request
    fn start_post_search(&'static self, query: &SearchQuery, request: &HttpRequest);
    /// This method should be called once a post search request has succeeded
    fn end_post_search(&'static self, process_time: usize);
}

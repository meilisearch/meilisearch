use crate::routes::indexes::documents::UpdateDocumentsQuery;
use crate::Opt;
use actix_web::HttpRequest;
use meilisearch_lib::index::SearchQuery;
use once_cell::sync::Lazy;
use platform_dirs::AppDirs;
use serde_json::Value;
use std::fmt::Display;
use std::fs;
use std::path::{Path, PathBuf};

/// The MeiliSearch config dir:
/// `~/.config/MeiliSearch` on *NIX or *BSD.
/// `~/Library/ApplicationSupport` on macOS.
/// `%APPDATA` (= `C:\Users%USERNAME%\AppData\Roaming`) on windows.
static MEILISEARCH_CONFIG_PATH: Lazy<Option<PathBuf>> =
    Lazy::new(|| AppDirs::new(Some("MeiliSearch"), false).map(|appdir| appdir.config_dir));

fn config_user_id_path(db_path: &Path) -> Option<PathBuf> {
    db_path
        .canonicalize()
        .ok()
        .map(|path| path.join("user-id").display().to_string().replace("/", "-"))
        .zip(MEILISEARCH_CONFIG_PATH.as_ref())
        .map(|(filename, config_path)| config_path.join(filename))
}

/// Look for the user-id in the `data.ms` or in `~/.config/MeiliSearch/path-to-db-user-id`
fn find_user_id(db_path: &Path) -> Option<String> {
    fs::read_to_string(db_path.join("user-id"))
        .ok()
        .or_else(|| fs::read_to_string(&config_user_id_path(db_path)?).ok())
}

#[cfg(all(not(debug_assertions), feature = "analytics"))]
/// Write the user-id in the `data.ms` and in `~/.config/MeiliSearch/path-to-db-user-id`. Ignore the errors.
fn write_user_id(db_path: &Path, user_id: &str) {
    let _ = fs::write(db_path.join("user-id"), user_id.as_bytes());
    if let Some((meilisearch_config_path, user_id_path)) = MEILISEARCH_CONFIG_PATH
        .as_ref()
        .zip(config_user_id_path(db_path))
    {
        let _ = fs::create_dir_all(&meilisearch_config_path);
        let _ = fs::write(user_id_path, user_id.as_bytes());
    }
}

// if we are in release mode and the feature analytics was enabled
#[cfg(all(not(debug_assertions), feature = "analytics"))]
mod segment {
    use crate::analytics::Analytics;
    use crate::routes::indexes::documents::UpdateDocumentsQuery;
    use actix_web::http::header::USER_AGENT;
    use actix_web::HttpRequest;
    use http::header::CONTENT_TYPE;
    use meilisearch_lib::index::SearchQuery;
    use meilisearch_lib::index_controller::Stats;
    use meilisearch_lib::MeiliSearch;
    use once_cell::sync::Lazy;
    use segment::message::{Identify, Track, User};
    use segment::{AutoBatcher, Batcher, HttpClient};
    use serde_json::{json, Value};
    use std::collections::{HashMap, HashSet};
    use std::fmt::Display;
    use std::time::{Duration, Instant};
    use sysinfo::{DiskExt, System, SystemExt};
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use crate::Opt;

    const SEGMENT_API_KEY: &str = "vHi89WrNDckHSQssyUJqLvIyp2QFITSC";

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

    pub struct SegmentAnalytics {
        user: User,
        opt: Opt,
        batcher: Mutex<AutoBatcher>,
        post_search_batcher: Mutex<SearchBatcher>,
        get_search_batcher: Mutex<SearchBatcher>,
        add_documents_batcher: Mutex<DocumentsBatcher>,
        update_documents_batcher: Mutex<DocumentsBatcher>,
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
            // see if there is already a user-id in the `data.ms` or in `/tmp/path-to-db-user-id`
            let user_id = super::find_user_id(&opt.db_path);
            let first_time_run = user_id.is_none();
            // if not, generate a new user-id and save it to the fs
            let user_id = user_id.unwrap_or_else(|| Uuid::new_v4().to_string());
            super::write_user_id(&opt.db_path, &user_id);

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
                add_documents_batcher: Mutex::new(DocumentsBatcher::default()),
                update_documents_batcher: Mutex::new(DocumentsBatcher::default()),
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
                        let _ = self
                            .batcher
                            .lock()
                            .await
                            .push(Identify {
                                context: Some(json!({
                                    "app": {
                                        "version": env!("CARGO_PKG_VERSION").to_string(),
                                    },
                                })),
                                user,
                                traits,
                                ..Default::default()
                            })
                            .await;
                    }
                    let get_search = std::mem::take(&mut *self.get_search_batcher.lock().await)
                        .into_event(&self.user, "Document Searched GET");
                    let post_search = std::mem::take(&mut *self.post_search_batcher.lock().await)
                        .into_event(&self.user, "Document Searched POST");
                    let add_documents =
                        std::mem::take(&mut *self.add_documents_batcher.lock().await)
                            .into_event(&self.user, "Documents Added");
                    let update_documents =
                        std::mem::take(&mut *self.update_documents_batcher.lock().await)
                            .into_event(&self.user, "Documents Updated");
                    // keep the lock on the batcher just for these three operations
                    {
                        let mut batcher = self.batcher.lock().await;
                        if let Some(get_search) = get_search {
                            let _ = batcher.push(get_search).await;
                        }
                        if let Some(post_search) = post_search {
                            let _ = batcher.push(post_search).await;
                        }
                        if let Some(add_documents) = add_documents {
                            let _ = batcher.push(add_documents).await;
                        }
                        if let Some(update_documents) = update_documents {
                            let _ = batcher.push(update_documents).await;
                        }
                        let _ = batcher.flush().await;
                    }
                    tokio::time::sleep(Duration::from_secs(60 * 60)).await; // one hour
                }
            });
        }

        fn start_search(
            &'static self,
            batcher: &'static Mutex<SearchBatcher>,
            query: &SearchQuery,
            request: &HttpRequest,
        ) {
            let user_agent = extract_user_agents(request);
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

            // to avoid blocking the search we are going to do the heavier computation and take the
            // batcher's mutex in an async task
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

                let mut search_batcher = batcher.lock().await;
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

        fn batch_documents(
            &'static self,
            batcher: &'static Mutex<DocumentsBatcher>,
            documents_query: &UpdateDocumentsQuery,
            index_creation: bool,
            request: &HttpRequest,
        ) {
            let user_agents = extract_user_agents(request);
            let primary_key = documents_query.primary_key.clone();
            let content_type = request
                .headers()
                .get(CONTENT_TYPE)
                .map(|s| s.to_str().unwrap_or("unkown"))
                .unwrap()
                .to_string();

            tokio::spawn(async move {
                let mut lock = batcher.lock().await;
                for user_agent in user_agents {
                    lock.user_agents.insert(user_agent);
                }
                lock.content_types.insert(content_type);
                if let Some(primary_key) = primary_key {
                    lock.primary_keys.insert(primary_key);
                }
                lock.index_creation |= index_creation;
                // drop the lock here
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
            });
        }

        fn start_get_search(&'static self, query: &SearchQuery, request: &HttpRequest) {
            self.start_search(&self.get_search_batcher, query, request)
        }

        fn end_get_search(&'static self, process_time: usize) {
            tokio::spawn(async move {
                let mut search_batcher = self.get_search_batcher.lock().await;
                search_batcher.total_succeeded += 1;
                search_batcher.time_spent.push(process_time);
            });
        }

        fn start_post_search(&'static self, query: &SearchQuery, request: &HttpRequest) {
            self.start_search(&self.post_search_batcher, query, request)
        }

        fn end_post_search(&'static self, process_time: usize) {
            tokio::spawn(async move {
                let mut search_batcher = self.post_search_batcher.lock().await;
                search_batcher.total_succeeded += 1;
                search_batcher.time_spent.push(process_time);
            });
        }

        fn add_documents(
            &'static self,
            documents_query: &UpdateDocumentsQuery,
            index_creation: bool,
            request: &HttpRequest,
        ) {
            self.batch_documents(
                &self.add_documents_batcher,
                documents_query,
                index_creation,
                request,
            )
        }

        fn update_documents(
            &'static self,
            documents_query: &UpdateDocumentsQuery,
            index_creation: bool,
            request: &HttpRequest,
        ) {
            self.batch_documents(
                &self.update_documents_batcher,
                documents_query,
                index_creation,
                request,
            )
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
        pub fn into_event(mut self, user: &User, event_name: &str) -> Option<Track> {
            if self.total_received == 0 {
                None
            } else {
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

                Some(Track {
                    user: user.clone(),
                    event: event_name.to_string(),
                    context,
                    properties,
                    ..Default::default()
                })
            }
        }
    }

    #[derive(Default)]
    pub struct DocumentsBatcher {
        // set to true when at least one request was received
        updated: bool,

        // context
        user_agents: HashSet<String>,

        content_types: HashSet<String>,
        primary_keys: HashSet<String>,
        index_creation: bool,
    }

    impl DocumentsBatcher {
        pub fn into_event(self, user: &User, event_name: &str) -> Option<Track> {
            if self.updated {
                None
            } else {
                let context = Some(json!({ "user-agent": self.user_agents}));

                let properties = json!({
                    "payload_type": self.content_types,
                    "primary_key": self.primary_keys,
                    "index_creation": self.index_creation,
                });

                Some(Track {
                    user: user.clone(),
                    event: event_name.to_string(),
                    context,
                    properties,
                    ..Default::default()
                })
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
        let user = find_user_id(&opt.db_path).unwrap_or(String::new());
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
    fn add_documents(
        &'static self,
        _documents_query: &UpdateDocumentsQuery,
        _index_creation: bool,
        _request: &HttpRequest,
    ) {
    }
    fn update_documents(
        &'static self,
        _documents_query: &UpdateDocumentsQuery,
        _index_creation: bool,
        _request: &HttpRequest,
    ) {
    }
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

    // this method should be called to batch a add documents request
    fn add_documents(
        &'static self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    );
    // this method should be called to batch a update documents request
    fn update_documents(
        &'static self,
        documents_query: &UpdateDocumentsQuery,
        index_creation: bool,
        request: &HttpRequest,
    );
}

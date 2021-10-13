use serde_json::Value;
use std::fmt::Display;
use std::fs::read_to_string;

use crate::Opt;

// if we are in release mode and the feature analytics was enabled
#[cfg(all(not(debug_assertions), feature = "analytics"))]
mod segment {
    use crate::analytics::Analytics;
    use meilisearch_lib::index_controller::Stats;
    use meilisearch_lib::MeiliSearch;
    use once_cell::sync::Lazy;
    use segment::message::{Identify, Track, User};
    use segment::{AutoBatcher, Batcher, HttpClient};
    use serde_json::{json, Value};
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
                        "core_number": sys.processors().len(),
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
            });
            let segment = Box::leak(segment);

            // batch the launched for the first time track event
            if first_time_run {
                segment.publish("Launched for the first time".to_string(), json!({}));
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
                    let _ = self.batcher.lock().await.flush().await;
                    println!("ANALYTICS: sent the batch");
                    tokio::time::sleep(Duration::from_secs(60 * 5)).await; // 5 minutes
                }
            });
        }
    }

    #[async_trait::async_trait]
    impl super::Analytics for SegmentAnalytics {
        fn publish(&'static self, event_name: String, send: Value) {
            tokio::spawn(async move {
                println!("ANALYTICS pushing {} in the batcher", event_name);
                let _ = self
                    .batcher
                    .lock()
                    .await
                    .push(Track {
                        user: self.user.clone(),
                        event: event_name.clone(),
                        properties: send,
                        ..Default::default()
                    })
                    .await;
                println!("ANALYTICS {} pushed", event_name);
            });
        }
    }

    impl Display for SegmentAnalytics {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.user)
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
            .unwrap_or_else(|_| "No user-id".to_string());
        let analytics = Box::new(Self { user });
        Box::leak(analytics)
    }
}

#[async_trait::async_trait]
impl Analytics for MockAnalytics {
    /// This is a noop and should be optimized out
    fn publish(&'static self, _event_name: String, _send: Value) {}
}

impl Display for MockAnalytics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.user)
    }
}

#[async_trait::async_trait]
pub trait Analytics: Display + Sync + Send {
    fn publish(&'static self, event_name: String, send: Value);
}

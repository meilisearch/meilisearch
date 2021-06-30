use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::debug;
use serde::Serialize;
use siphasher::sip::SipHasher;

use crate::Data;
use crate::Opt;

const AMPLITUDE_API_KEY: &str = "f7fba398780e06d8fe6666a9be7e3d47";

#[derive(Debug, Serialize)]
struct EventProperties {
    database_size: u64,
    last_update_timestamp: Option<i64>, //timestamp
    number_of_documents: Vec<u64>,
}

impl EventProperties {
    async fn from(data: Data) -> anyhow::Result<EventProperties> {
        let stats = data.index_controller.get_all_stats().await?;

        let database_size = stats.database_size;
        let last_update_timestamp = stats.last_update.map(|u| u.timestamp());
        let number_of_documents = stats
            .indexes
            .values()
            .map(|index| index.number_of_documents)
            .collect();

        Ok(EventProperties {
            database_size,
            last_update_timestamp,
            number_of_documents,
        })
    }
}

#[derive(Debug, Serialize)]
struct UserProperties<'a> {
    env: &'a str,
    start_since_days: u64,
    user_email: Option<String>,
    server_provider: Option<String>,
}

#[derive(Debug, Serialize)]
struct Event<'a> {
    user_id: &'a str,
    event_type: &'a str,
    device_id: &'a str,
    time: u64,
    app_version: &'a str,
    user_properties: UserProperties<'a>,
    event_properties: Option<EventProperties>,
}

#[derive(Debug, Serialize)]
struct AmplitudeRequest<'a> {
    api_key: &'a str,
    events: Vec<Event<'a>>,
}

pub async fn analytics_sender(data: Data, opt: Opt) {
    let username = whoami::username();
    let hostname = whoami::hostname();
    let platform = whoami::platform();

    let uid = username + &hostname + &platform.to_string();

    let mut hasher = SipHasher::new();
    uid.hash(&mut hasher);
    let hash = hasher.finish();

    let uid = format!("{:X}", hash);
    let platform = platform.to_string();
    let first_start = Instant::now();

    loop {
        let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let user_id = &uid;
        let device_id = &platform;
        let time = n.as_secs();
        let event_type = "runtime_tick";
        let elapsed_since_start = first_start.elapsed().as_secs() / 86_400; // One day
        let event_properties = EventProperties::from(data.clone()).await.ok();
        let app_version = env!("CARGO_PKG_VERSION").to_string();
        let app_version = app_version.as_str();
        let user_email = std::env::var("MEILI_USER_EMAIL").ok();
        let server_provider = std::env::var("MEILI_SERVER_PROVIDER").ok();
        let user_properties = UserProperties {
            env: &opt.env,
            start_since_days: elapsed_since_start,
            user_email,
            server_provider,
        };

        let event = Event {
            user_id,
            event_type,
            device_id,
            time,
            app_version,
            user_properties,
            event_properties,
        };

        let request = AmplitudeRequest {
            api_key: AMPLITUDE_API_KEY,
            events: vec![event],
        };

        let response = reqwest::Client::new()
            .post("https://api2.amplitude.com/2/httpapi")
            .timeout(Duration::from_secs(60)) // 1 minute max
            .json(&request)
            .send()
            .await;
        if let Err(e) = response {
            debug!("Unsuccessful call to Amplitude: {}", e);
        }

        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

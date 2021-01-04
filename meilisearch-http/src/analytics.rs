use std::hash::{Hash, Hasher};
use std::{error, thread};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::error;
use serde::Serialize;
use serde_qs as qs;
use siphasher::sip::SipHasher;
use walkdir::WalkDir;

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
    fn from(data: Data) -> Result<EventProperties, Box<dyn error::Error>> {
        let mut index_list = Vec::new();

        let reader = data.db.main_read_txn()?;

        for index_uid in data.db.indexes_uids() {
            if let Some(index) = data.db.open_index(&index_uid) {
                let number_of_documents = index.main.number_of_documents(&reader)?;
                index_list.push(number_of_documents);
            }
        }

        let database_size = WalkDir::new(&data.db_path)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.metadata().ok())
            .filter(|metadata| metadata.is_file())
            .fold(0, |acc, m| acc + m.len());

        let last_update_timestamp = data.db.last_update(&reader)?.map(|u| u.timestamp());

        Ok(EventProperties {
            database_size,
            last_update_timestamp,
            number_of_documents: index_list,
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
    event: &'a str,
}

pub fn analytics_sender(data: Data, opt: Opt) {
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
        let event_properties = EventProperties::from(data.clone()).ok();
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
            event_properties
        };
        let event = serde_json::to_string(&event).unwrap();

        let request = AmplitudeRequest {
            api_key: AMPLITUDE_API_KEY,
            event: &event,
        };

        let body = qs::to_string(&request).unwrap();
        let response = ureq::post("https://api.amplitude.com/httpapi").send_string(&body);
        match response {
            Err(ureq::Error::Status(_ , response)) => {
                error!("Unsuccessful call to Amplitude: {}", response.into_string().unwrap_or_default());
            }
            Err(e) => {
                error!("Unsuccessful call to Amplitude: {}", e);
            }
            _ => (),
        }

        thread::sleep(Duration::from_secs(3600)) // one hour
    }
}

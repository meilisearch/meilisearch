use std::hash::{Hash, Hasher};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::error;
use serde::Serialize;
use serde_qs as qs;
use siphasher::sip::SipHasher;

const AMPLITUDE_API_KEY: &str = "f7fba398780e06d8fe6666a9be7e3d47";

#[derive(Debug, Serialize)]
struct Event<'a> {
    user_id: &'a str,
    event_type: &'a str,
    device_id: &'a str,
    time: u64,
}

#[derive(Debug, Serialize)]
struct AmplitudeRequest<'a> {
    api_key: &'a str,
    event: &'a str,
}

pub fn analytics_sender() {
    let username = whoami::username();
    let hostname = whoami::hostname();
    let platform = whoami::platform();

    let uid = username + &hostname + &platform.to_string();

    let mut hasher = SipHasher::new();
    uid.hash(&mut hasher);
    let hash = hasher.finish();

    let uid = format!("{:X}", hash);
    let platform = platform.to_string();

    loop {
        let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let user_id = &uid;
        let device_id = &platform;
        let time = n.as_secs();
        let event_type = "runtime_tick";

        let event = Event {
            user_id,
            event_type,
            device_id,
            time,
        };
        let event = serde_json::to_string(&event).unwrap();

        let request = AmplitudeRequest {
            api_key: AMPLITUDE_API_KEY,
            event: &event,
        };

        let body = qs::to_string(&request).unwrap();
        let response = ureq::post("https://api.amplitude.com/httpapi").send_string(&body);
        if !response.ok() {
            let body = response.into_string().unwrap();
            error!("Unsuccessful call to Amplitude: {}", body);
        }

        thread::sleep(Duration::from_secs(86_400)) // one day
    }
}

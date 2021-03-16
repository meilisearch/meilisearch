use actix_web::{get, HttpResponse};
use serde::{Deserialize, Serialize};

pub mod document;
pub mod health;
pub mod index;
pub mod key;
pub mod search;
pub mod setting;
pub mod stats;
pub mod stop_words;
pub mod synonym;
pub mod dump;

#[derive(Deserialize)]
pub struct IndexParam {
    index_uid: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

impl IndexUpdateResponse {
    pub fn with_id(update_id: u64) -> Self {
        Self { update_id }
    }
}

/// Return the dashboard, should not be used in production. See [running]
#[get("/")]
pub async fn load_html() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../../public/interface.html").to_string())
}

/// Always return a 200 with:
/// ```json
/// {
///     "status": "Meilisearch is running"
/// }
/// ```
#[get("/")]
pub async fn running() -> HttpResponse {
    let payload = serde_json::json!({ "status": "MeiliSearch is running" }).to_string();
    HttpResponse::Ok().body(payload)
}

#[get("/bulma.min.css")]
pub async fn load_css() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/css; charset=utf-8")
        .body(include_str!("../../public/bulma.min.css").to_string())
}

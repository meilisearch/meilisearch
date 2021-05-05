use actix_web::{get, HttpResponse};
use serde::{Deserialize, Serialize};

pub mod document;
pub mod health;
pub mod index;
pub mod key;
pub mod search;
pub mod settings;
pub mod stats;
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

/// Always return a 200 with:
/// ```json
/// {
///     "status": "Meilisearch is running"
/// }
/// ```
#[get("/")]
pub async fn running() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({ "status": "MeiliSearch is running" }))
}

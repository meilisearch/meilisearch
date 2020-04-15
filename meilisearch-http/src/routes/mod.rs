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

#[derive(Default, Deserialize)]
pub struct IndexParam {
    index_uid: String,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

impl IndexUpdateResponse {
    pub fn with_id(update_id: u64) -> Self {
        Self { update_id }
    }
}

#[get("/")]
pub async fn load_html() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../../public/interface.html").to_string())
}

#[get("/bulma.min.css")]
pub async fn load_css() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/css; charset=utf-8")
        .body(include_str!("../../public/bulma.min.css").to_string())
}

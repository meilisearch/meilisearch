use actix_web::{web, HttpResponse};
use serde::Serialize;

use crate::Data;
use crate::extractors::authentication::{GuardedData, policies::*};

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.route("/keys", web::get().to(list));
}

#[derive(Serialize)]
struct KeysResponse {
    private: Option<String>,
    public: Option<String>,
}

async fn list(data: GuardedData<Admin, Data>) -> HttpResponse {
    let api_keys = data.api_keys.clone();
    HttpResponse::Ok().json(&KeysResponse {
        private: api_keys.private,
        public: api_keys.public,
    })
}

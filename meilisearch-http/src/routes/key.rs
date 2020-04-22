use actix_web::web;
use actix_web_macros::get;
use serde::Serialize;

use crate::helpers::Authentication;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(list);
}

#[derive(Default, Serialize)]
struct KeysResponse {
    private: Option<String>,
    public: Option<String>,
}

#[get("/keys", wrap = "Authentication::Admin")]
async fn list(data: web::Data<Data>) -> web::Json<KeysResponse> {
    let api_keys = data.api_keys.clone();
    web::Json(KeysResponse {
        private: api_keys.private,
        public: api_keys.public,
    })
}

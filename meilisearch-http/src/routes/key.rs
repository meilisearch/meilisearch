use crate::Data;
use actix_web::{get, web};
use serde::Serialize;

#[derive(Default, Serialize)]
pub struct KeysResponse {
    private: Option<String>,
    public: Option<String>,
}

#[get("/keys")]
pub async fn list(data: web::Data<Data>) -> web::Json<KeysResponse> {
    let api_keys = data.api_keys.clone();
    web::Json(KeysResponse {
        private: api_keys.private,
        public: api_keys.public,
    })
}

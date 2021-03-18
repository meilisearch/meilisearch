use actix_web::get;
use actix_web::{web, HttpResponse};

use crate::error::ResponseError;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(get_health);
}

#[get("/health")]
async fn get_health() -> Result<HttpResponse, ResponseError> {
    let payload = serde_json::json!({ "status": "available" });
    Ok(HttpResponse::Ok().json(payload))
}

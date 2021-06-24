use actix_web::{web, HttpResponse};

use crate::error::ResponseError;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("/health").route(web::get().to(get_health)));
}

async fn get_health() -> Result<HttpResponse, ResponseError> {
    Ok(HttpResponse::Ok().json(serde_json::json!({ "status": "available" })))
}

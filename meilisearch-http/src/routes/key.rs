use crate::Data;
use serde_json::json;
use actix_web::*;

#[get("/keys")]
pub async fn list(
    data: web::Data<Data>,
) -> Result<HttpResponse> {
    let keys = &data.api_keys;

    HttpResponse::Ok().json(&json!({
        "private": keys.private,
        "public": keys.public,
    })).await
}

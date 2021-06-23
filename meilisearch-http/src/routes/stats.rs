use actix_web::get;
use actix_web::web;
use actix_web::HttpResponse;
use log::debug;
use serde::Serialize;

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(get_index_stats)
        .service(get_stats)
        .service(get_version);
}

#[get("/indexes/{index_uid}/stats", wrap = "Authentication::Private")]
async fn get_index_stats(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let response = data.get_index_stats(path.index_uid.clone()).await?;

    debug!("returns: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}

#[get("/stats", wrap = "Authentication::Private")]
async fn get_stats(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let response = data.get_all_stats().await?;

    debug!("returns: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionResponse {
    commit_sha: String,
    commit_date: String,
    pkg_version: String,
}

#[get("/version", wrap = "Authentication::Private")]
async fn get_version() -> HttpResponse {
    let commit_sha = match option_env!("COMMIT_SHA") {
        Some("") | None => env!("VERGEN_SHA"),
        Some(commit_sha) => commit_sha,
    };
    let commit_date = match option_env!("COMMIT_DATE") {
        Some("") | None => env!("VERGEN_COMMIT_DATE"),
        Some(commit_date) => commit_date,
    };

    HttpResponse::Ok().json(VersionResponse {
        commit_sha: commit_sha.to_string(),
        commit_date: commit_date.to_string(),
        pkg_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

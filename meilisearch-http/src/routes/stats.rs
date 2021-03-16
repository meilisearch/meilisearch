use std::collections::{BTreeMap, HashMap};

use actix_web::get;
use actix_web::web;
use actix_web::HttpResponse;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(index_stats)
        .service(get_stats)
        .service(get_version);
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexStatsResponse {
    number_of_documents: u64,
    is_indexing: bool,
    fields_distribution: BTreeMap<String, usize>,
}

#[get("/indexes/{index_uid}/stats", wrap = "Authentication::Private")]
async fn index_stats(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsResult {
    database_size: u64,
    last_update: Option<DateTime<Utc>>,
    indexes: HashMap<String, IndexStatsResponse>,
}

#[get("/stats", wrap = "Authentication::Private")]
async fn get_stats(_data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionResponse {
    commit_sha: String,
    build_date: String,
    pkg_version: String,
}

#[get("/version", wrap = "Authentication::Private")]
async fn get_version() -> HttpResponse {
   HttpResponse::Ok().json(VersionResponse {
        commit_sha: env!("VERGEN_SHA").to_string(),
        build_date: env!("VERGEN_BUILD_TIMESTAMP").to_string(),
        pkg_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

use std::collections::BTreeMap;
use std::iter::FromIterator;

use actix_web::get;
use actix_web::web;
use actix_web::HttpResponse;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::data::Stats;
use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::index_controller::IndexStats;
use crate::routes::IndexParam;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(get_index_stats)
        .service(get_stats)
        .service(get_version);
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexStatsResponse {
    number_of_documents: u64,
    is_indexing: bool,
    fields_distribution: BTreeMap<String, u64>,
}

impl From<IndexStats> for IndexStatsResponse {
    fn from(stats: IndexStats) -> Self {
        Self {
            number_of_documents: stats.number_of_documents,
            is_indexing: stats.is_indexing,
            fields_distribution: BTreeMap::from_iter(stats.fields_distribution.into_iter()),
        }
    }
}

#[get("/indexes/{index_uid}/stats", wrap = "Authentication::Private")]
async fn get_index_stats(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let response: IndexStatsResponse = data.get_index_stats(path.index_uid.clone()).await?.into();

    Ok(HttpResponse::Ok().json(response))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsResponse {
    database_size: u64,
    last_update: Option<DateTime<Utc>>,
    indexes: BTreeMap<String, IndexStatsResponse>,
}

impl From<Stats> for StatsResponse {
    fn from(stats: Stats) -> Self {
        Self {
            database_size: stats.database_size,
            last_update: stats.last_update,
            indexes: stats
                .indexes
                .into_iter()
                .map(|(uid, index_stats)| (uid, index_stats.into()))
                .collect(),
        }
    }
}

#[get("/stats", wrap = "Authentication::Private")]
async fn get_stats(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let response: StatsResponse = data.get_stats().await?.into();

    Ok(HttpResponse::Ok().json(response))
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
        build_date: commit_date.to_string(),
        pkg_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

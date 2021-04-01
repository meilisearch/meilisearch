use std::collections::HashMap;

use actix_web::get;
use actix_web::web;
use actix_web::HttpResponse;
use chrono::{DateTime, Utc};
use milli::FieldsDistribution;
use serde::Serialize;

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
    fields_distribution: FieldsDistribution,
}

impl From<IndexStats> for IndexStatsResponse {
    fn from(stats: IndexStats) -> Self {
        Self {
            number_of_documents: stats.number_of_documents,
            is_indexing: stats.is_indexing,
            fields_distribution: stats.fields_distribution,
        }
    }
}

#[get("/indexes/{index_uid}/stats", wrap = "Authentication::Private")]
async fn get_index_stats(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let response: IndexStatsResponse = data
        .index_controller
        .get_stats(path.index_uid.clone())
        .await?
        .into();

    Ok(HttpResponse::Ok().json(response))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsResponse {
    database_size: u64,
    last_update: Option<DateTime<Utc>>,
    indexes: HashMap<String, IndexStatsResponse>,
}

#[get("/stats", wrap = "Authentication::Private")]
async fn get_stats(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let mut response = StatsResponse {
        database_size: 0,
        last_update: None,
        indexes: HashMap::new(),
    };

    for index in data.index_controller.list_indexes().await? {
        let stats = data.index_controller.get_stats(index.uid.clone()).await?;

        response.database_size += stats.size;
        response.last_update = Some(match response.last_update {
            Some(last_update) => last_update.max(index.meta.updated_at),
            None => index.meta.updated_at,
        });
        response.indexes.insert(index.uid, stats.into());
    }

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

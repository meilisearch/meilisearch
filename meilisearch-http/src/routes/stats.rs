use std::collections::{HashMap, BTreeMap};

use actix_web::web;
use actix_web::HttpResponse;
use actix_web::get;
use chrono::{DateTime, Utc};
use log::error;
use serde::Serialize;
use walkdir::WalkDir;

use crate::error::{Error, ResponseError};
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
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let reader = data.db.main_read_txn()?;

    let number_of_documents = index.main.number_of_documents(&reader)?;

    let fields_distribution = index.main.fields_distribution(&reader)?.unwrap_or_default();

    let update_reader = data.db.update_read_txn()?;

    let is_indexing =
        data.db.is_indexing(&update_reader, &path.index_uid)?
            .ok_or(Error::internal(
                "Impossible to know if the database is indexing",
            ))?;

    Ok(HttpResponse::Ok().json(IndexStatsResponse {
        number_of_documents,
        is_indexing,
        fields_distribution,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsResult {
    database_size: u64,
    last_update: Option<DateTime<Utc>>,
    indexes: HashMap<String, IndexStatsResponse>,
}

#[get("/stats", wrap = "Authentication::Private")]
async fn get_stats(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let mut index_list = HashMap::new();

    let reader = data.db.main_read_txn()?;
    let update_reader = data.db.update_read_txn()?;

    let indexes_set = data.db.indexes_uids();
    for index_uid in indexes_set {
        let index = data.db.open_index(&index_uid);
        match index {
            Some(index) => {
                let number_of_documents = index.main.number_of_documents(&reader)?;

                let fields_distribution = index.main.fields_distribution(&reader)?.unwrap_or_default();

                let is_indexing = data.db.is_indexing(&update_reader, &index_uid)?.ok_or(
                    Error::internal("Impossible to know if the database is indexing"),
                )?;

                let response = IndexStatsResponse {
                    number_of_documents,
                    is_indexing,
                    fields_distribution,
                };
                index_list.insert(index_uid, response);
            }
            None => error!(
                "Index {:?} is referenced in the indexes list but cannot be found",
                index_uid
            ),
        }
    }

    let database_size = WalkDir::new(&data.db_path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .fold(0, |acc, m| acc + m.len());

    let last_update = data.db.last_update(&reader)?;

    Ok(HttpResponse::Ok().json(StatsResult {
        database_size,
        last_update,
        indexes: index_list,
    }))
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

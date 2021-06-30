use actix_web::{web, HttpResponse};
use log::debug;
use serde::Serialize;

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::routes::IndexParam;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("/indexes/{index_uid}/stats").route(web::get().to(get_index_stats)))
        .service(web::resource("/stats").route(web::get().to(get_stats)))
        .service(web::resource("/version").route(web::get().to(get_version)));
}

async fn get_index_stats(
    data: GuardedData<Private, Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let response = data.get_index_stats(path.index_uid.clone()).await?;

    debug!("returns: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}

async fn get_stats(data: GuardedData<Private, Data>) -> Result<HttpResponse, ResponseError> {
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

async fn get_version(_data: GuardedData<Private, Data>) -> HttpResponse {
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

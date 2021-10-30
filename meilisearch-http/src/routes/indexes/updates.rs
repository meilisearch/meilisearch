use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use log::debug;
use meilisearch_lib::MeiliSearch;
use serde::{Deserialize, Serialize};

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::routes::{IndexParam, UpdateStatusResponse};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(get_all_updates_status)))
        .service(web::resource("{update_id}").route(web::get().to(get_update_status)));
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateParam {
    index_uid: String,
    update_id: u64,
}

pub async fn get_update_status(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<UpdateParam>,
) -> Result<HttpResponse, ResponseError> {
    let params = path.into_inner();
    let meta = meilisearch
        .update_status(params.index_uid, params.update_id)
        .await?;
    let meta = UpdateStatusResponse::from(meta);
    debug!("returns: {:?}", meta);
    Ok(HttpResponse::Ok().json(meta))
}

pub async fn get_all_updates_status(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let metas = meilisearch
        .all_update_status(path.into_inner().index_uid)
        .await?;
    let metas = metas
        .into_iter()
        .map(UpdateStatusResponse::from)
        .collect::<Vec<_>>();

    debug!("returns: {:?}", metas);
    Ok(HttpResponse::Ok().json(metas))
}

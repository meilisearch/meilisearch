use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use log::debug;
use serde::{Deserialize, Serialize};

use super::{IndexParam, UpdateStatusResponse};
use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("indexes")
            .route(web::get().to(list_indexes))
            .route(web::post().to(create_index)),
    )
    .service(
        web::resource("/indexes/{index_uid}")
            .route(web::get().to(get_index))
            .route(web::put().to(update_index))
            .route(web::delete().to(delete_index)),
    )
    .service(
        web::resource("/indexes/{index_uid}/updates").route(web::get().to(get_all_updates_status)),
    )
    .service(
        web::resource("/indexes/{index_uid}/updates/{update_id}")
            .route(web::get().to(get_update_status)),
    );
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IndexCreateRequest {
    uid: String,
    primary_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateIndexRequest {
    uid: Option<String>,
    primary_key: Option<String>,
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

async fn list_indexes(data: GuardedData<Private, Data>) -> Result<HttpResponse, ResponseError> {
    let indexes = data.list_indexes().await?;
    debug!("returns: {:?}", indexes);
    Ok(HttpResponse::Ok().json(indexes))
}

async fn create_index(
    data: GuardedData<Private, Data>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    let body = body.into_inner();
    let meta = data.create_index(body.uid, body.primary_key).await?;
    Ok(HttpResponse::Ok().json(meta))
}

async fn get_index(
    data: GuardedData<Private, Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let meta = data.index(path.index_uid.clone()).await?;
    debug!("returns: {:?}", meta);
    Ok(HttpResponse::Ok().json(meta))
}

async fn update_index(
    data: GuardedData<Private, Data>,
    path: web::Path<IndexParam>,
    body: web::Json<UpdateIndexRequest>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", body);
    let body = body.into_inner();
    let meta = data
        .update_index(path.into_inner().index_uid, body.primary_key, body.uid)
        .await?;
    debug!("returns: {:?}", meta);
    Ok(HttpResponse::Ok().json(meta))
}

async fn delete_index(
    data: GuardedData<Private, Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    data.delete_index(path.index_uid.clone()).await?;
    Ok(HttpResponse::NoContent().finish())
}

#[derive(Deserialize)]
struct UpdateParam {
    index_uid: String,
    update_id: u64,
}

async fn get_update_status(
    data: GuardedData<Private, Data>,
    path: web::Path<UpdateParam>,
) -> Result<HttpResponse, ResponseError> {
    let params = path.into_inner();
    let meta = data
        .get_update_status(params.index_uid, params.update_id)
        .await?;
    let meta = UpdateStatusResponse::from(meta);
    debug!("returns: {:?}", meta);
    Ok(HttpResponse::Ok().json(meta))
}

async fn get_all_updates_status(
    data: GuardedData<Private, Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let metas = data.get_updates_status(path.into_inner().index_uid).await?;
    let metas = metas
        .into_iter()
        .map(UpdateStatusResponse::from)
        .collect::<Vec<_>>();

    debug!("returns: {:?}", metas);
    Ok(HttpResponse::Ok().json(metas))
}

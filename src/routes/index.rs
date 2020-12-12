use actix_web::{delete, get, post, put};
use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use log::error;
use meilisearch_core::{Database, MainReader, UpdateReader};
use meilisearch_core::update::UpdateStatus;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use crate::Data;
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::routes::IndexParam;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(list_indexes)
        .service(get_index)
        .service(create_index)
        .service(update_index)
        .service(delete_index)
        .service(get_update_status)
        .service(get_all_updates_status);
}

fn generate_uid() -> String {
    todo!()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexResponse {
    pub name: String,
    pub uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    pub primary_key: Option<String>,
}

pub fn list_indexes_sync(data: &web::Data<Data>, reader: &MainReader) -> Result<Vec<IndexResponse>, ResponseError> {
    todo!()
}

#[get("/indexes", wrap = "Authentication::Private")]
async fn list_indexes(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let reader = data.db.main_read_txn()?;
    let indexes = list_indexes_sync(&data, &reader)?;

    Ok(HttpResponse::Ok().json(indexes))
}

#[get("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn get_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IndexCreateRequest {
    name: Option<String>,
    uid: Option<String>,
    primary_key: Option<String>,
}


pub fn create_index_sync(
    database: &std::sync::Arc<Database>,
    uid: String,
    name: String,
    primary_key: Option<String>,
) -> Result<IndexResponse, Error> {
    todo!()
}

#[post("/indexes", wrap = "Authentication::Private")]
async fn create_index(
    data: web::Data<Data>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateIndexRequest {
    name: Option<String>,
    primary_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateIndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

#[put("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn update_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn delete_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[derive(Deserialize)]
struct UpdateParam {
    index_uid: String,
    update_id: u64,
}

#[get(
    "/indexes/{index_uid}/updates/{update_id}",
    wrap = "Authentication::Private"
)]
async fn get_update_status(
    data: web::Data<Data>,
    path: web::Path<UpdateParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}
pub fn get_all_updates_status_sync(
    data: &web::Data<Data>,
    reader: &UpdateReader,
    index_uid: &str,
) -> Result<Vec<UpdateStatus>, Error> {
    todo!()
}

#[get("/indexes/{index_uid}/updates", wrap = "Authentication::Private")]
async fn get_all_updates_status(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

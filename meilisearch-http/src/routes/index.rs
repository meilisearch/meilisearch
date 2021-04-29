use actix_web::{delete, get, post, put};
use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::ResponseError;
use crate::helpers::Authentication;
use super::{UpdateStatusResponse, IndexParam};
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(list_indexes)
        .service(get_index)
        .service(create_index)
        .service(update_index)
        .service(delete_index)
        .service(get_update_status)
        .service(get_all_updates_status);
}

#[get("/indexes", wrap = "Authentication::Private")]
async fn list_indexes(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    match data.list_indexes().await {
        Ok(indexes) => Ok(HttpResponse::Ok().json(indexes)),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[get("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn get_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    match data.index(path.index_uid.clone()).await {
        Ok(meta) => Ok(HttpResponse::Ok().json(meta)),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IndexCreateRequest {
    uid: String,
    primary_key: Option<String>,
}

#[post("/indexes", wrap = "Authentication::Private")]
async fn create_index(
    data: web::Data<Data>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    let body = body.into_inner();
    match data.create_index(body.uid, body.primary_key).await {
        Ok(meta) => Ok(HttpResponse::Ok().json(meta)),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
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

#[put("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn update_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<UpdateIndexRequest>,
) -> Result<HttpResponse, ResponseError> {
    let body = body.into_inner();
    match data
        .update_index(path.into_inner().index_uid, body.primary_key, body.uid)
        .await
    {
        Ok(meta) => Ok(HttpResponse::Ok().json(meta)),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[delete("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn delete_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    match data.delete_index(path.index_uid.clone()).await {
        Ok(_) => Ok(HttpResponse::NoContent().finish()),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
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
    let params = path.into_inner();
    let result = data
        .get_update_status(params.index_uid, params.update_id)
        .await;
    match result {
        Ok(meta) => {
            let meta = UpdateStatusResponse::from(meta);
            Ok(HttpResponse::Ok().json(meta))
        },
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[get("/indexes/{index_uid}/updates", wrap = "Authentication::Private")]
async fn get_all_updates_status(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let result = data.get_updates_status(path.into_inner().index_uid).await;
    match result {
        Ok(metas) => {
            let metas = metas
                .into_iter()
                .map(UpdateStatusResponse::from)
                .collect::<Vec<_>>();

            Ok(HttpResponse::Ok().json(metas))
        },
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

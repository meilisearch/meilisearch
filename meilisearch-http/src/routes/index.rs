use actix_web::{delete, get, post, put};
use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::Data;
use crate::error::ResponseError;
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


#[get("/indexes", wrap = "Authentication::Private")]
async fn list_indexes(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    match data.list_indexes().await {
        Ok(indexes) => {
            let json = serde_json::to_string(&indexes).unwrap();
            Ok(HttpResponse::Ok().body(&json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[get("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn get_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    match data.index(&path.index_uid).await? {
        Some(meta) => {
            let json = serde_json::to_string(&meta).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        None => {
            let e = format!("Index {:?} doesn't exist.", path.index_uid);
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
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
    match data.create_index(&body.uid, body.primary_key.clone()).await {
        Ok(meta) => {
            let json = serde_json::to_string(&meta).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
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
    body: web::Json<UpdateIndexRequest>,
) -> Result<HttpResponse, ResponseError> {
    match data.update_index(&path.index_uid, body.primary_key.as_ref(),  body.uid.as_ref()).await {
        Ok(meta) => {
            let json = serde_json::to_string(&meta).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[delete("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn delete_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    match data.delete_index(path.index_uid.clone()).await {
        Ok(_) => Ok(HttpResponse::Ok().finish()),
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
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
    let result = data.get_update_status(&path.index_uid, path.update_id).await;
    match result {
        Ok(Some(meta)) => {
            let json = serde_json::to_string(&meta).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Ok(None) => {
            let e = format!("update {} for index {:?} doesn't exists.", path.update_id, path.index_uid);
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[get("/indexes/{index_uid}/updates", wrap = "Authentication::Private")]
async fn get_all_updates_status(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let result = data.get_updates_status(&path.index_uid).await;
    match result {
        Ok(metas) => {
            let json = serde_json::to_string(&metas).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

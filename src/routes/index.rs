use actix_web::{delete, get, post, put};
use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use log::error;
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
    match data.list_indexes() {
        Ok(indexes) => {
            let json = serde_json::to_string(&indexes).unwrap();
            Ok(HttpResponse::Ok().body(&json))
        }
        Err(e) => {
            error!("error listing indexes: {}", e);
            unimplemented!()
        }
    }

}

#[get("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn get_index(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
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

#[post("/indexes", wrap = "Authentication::Private")]
async fn create_index(
    _data: web::Data<Data>,
    _body: web::Json<IndexCreateRequest>,
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
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn delete_index(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
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
    let result = data.get_update_status(&path.index_uid, path.update_id);
    match result {
        Ok(Some(meta)) => {
            let json = serde_json::to_string(&meta).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Ok(None) => {
            todo!()
        }
        Err(e) => {
            error!("{}", e);
            todo!()
        }
    }
}

#[get("/indexes/{index_uid}/updates", wrap = "Authentication::Private")]
async fn get_all_updates_status(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let result = data.get_updates_status(&path.index_uid);
    match result {
        Ok(metas) => {
            let json = serde_json::to_string(&metas).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            error!("{}", e);
            todo!()
        }
    }
}

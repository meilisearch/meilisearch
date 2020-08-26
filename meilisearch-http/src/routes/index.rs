use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post, put};
use log::error;
use serde::Deserialize;

use crate::data::{Data, IndexCreateRequest, IndexResponse};
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;

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
    let reader = data.db.main_read_txn()?;
    let mut indexes = Vec::new();

    for index_uid in data.db.indexes_uids() {
        let index = data.db.open_index(&index_uid);

        match index {
            Some(index) => {
                let name = index
                    .main
                    .name(&reader)?
                    .ok_or(Error::internal("Impossible to get the name of an index"))?;
                let created_at = index.main.created_at(&reader)?.ok_or(Error::internal(
                    "Impossible to get the create date of an index",
                ))?;
                let updated_at = index.main.updated_at(&reader)?.ok_or(Error::internal(
                    "Impossible to get the last update date of an index",
                ))?;

                let primary_key = match index.main.schema(&reader) {
                    Ok(Some(schema)) => match schema.primary_key() {
                        Some(primary_key) => Some(primary_key.to_owned()),
                        None => None,
                    },
                    _ => None,
                };

                let index_response = IndexResponse {
                    name,
                    uid: index_uid,
                    created_at,
                    updated_at,
                    primary_key,
                };
                indexes.push(index_response);
            }
            None => error!(
                "Index {} is referenced in the indexes list but cannot be found",
                index_uid
            ),
        }
    }

    Ok(HttpResponse::Ok().json(indexes))
}

#[get("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn get_index(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;

    let reader = data.db.main_read_txn()?;
    let name = index
        .main
        .name(&reader)?
        .ok_or(Error::internal("Impossible to get the name of an index"))?;
    let created_at = index.main.created_at(&reader)?.ok_or(Error::internal(
        "Impossible to get the create date of an index",
    ))?;
    let updated_at = index.main.updated_at(&reader)?.ok_or(Error::internal(
        "Impossible to get the last update date of an index",
    ))?;

    let primary_key = match index.main.schema(&reader) {
        Ok(Some(schema)) => match schema.primary_key() {
            Some(primary_key) => Some(primary_key.to_owned()),
            None => None,
        },
        _ => None,
    };
    let index_response = IndexResponse {
        name,
        uid: index_uid.clone(),
        created_at,
        updated_at,
        primary_key,
    };

    Ok(HttpResponse::Ok().json(index_response))
}

#[post("/indexes", wrap = "Authentication::Private")]
async fn create_index(
    data: web::Data<Data>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    let response = data.create_index(&body.into_inner())?;
    Ok(HttpResponse::Created().json(response))
}

#[put("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn update_index(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    let response = data.update_index(index_uid.as_ref(), body.into_inner())?;
    Ok(HttpResponse::Ok().json(response))
}

#[delete("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn delete_index(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    data.delete_index(index_uid.as_ref())?;
    Ok(HttpResponse::NoContent().finish())
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
    let index = data
        .db
        .open_index(path.index_uid.as_str())
        .ok_or(Error::index_not_found(path.index_uid.as_str()))?;

    let reader = data.db.update_read_txn()?;

    let status = index.update_status(&reader, path.update_id)?;

    match status {
        Some(status) => Ok(HttpResponse::Ok().json(status)),
        None => Err(Error::NotFound(format!("Update {}", path.update_id)).into()),
    }
}

#[get("/indexes/{index_uid}/updates", wrap = "Authentication::Private")]
async fn get_all_updates_status(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;

    let reader = data.db.update_read_txn()?;

    let response = index.all_updates_status(&reader)?;

    Ok(HttpResponse::Ok().json(response))
}

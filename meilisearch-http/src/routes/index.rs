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
    let mut rng = rand::thread_rng();
    let sample = b"abcdefghijklmnopqrstuvwxyz0123456789";
    sample
        .choose_multiple(&mut rng, 8)
        .map(|c| *c as char)
        .collect()
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
    let mut indexes = Vec::new();

    for index_uid in data.db.indexes_uids() {
        let index = data.db.open_index(&index_uid);

        match index {
            Some(index) => {
                let name = index.main.name(reader)?.ok_or(Error::internal(
                        "Impossible to get the name of an index",
                ))?;
                let created_at = index
                    .main
                    .created_at(reader)?
                    .ok_or(Error::internal(
                            "Impossible to get the create date of an index",
                    ))?;
                let updated_at = index
                    .main
                    .updated_at(reader)?
                    .ok_or(Error::internal(
                            "Impossible to get the last update date of an index",
                    ))?;

                let primary_key = match index.main.schema(reader) {
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

    Ok(indexes)
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
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let reader = data.db.main_read_txn()?;
    let name = index.main.name(&reader)?.ok_or(Error::internal(
            "Impossible to get the name of an index",
    ))?;
    let created_at = index
        .main
        .created_at(&reader)?
        .ok_or(Error::internal(
                "Impossible to get the create date of an index",
        ))?;
    let updated_at = index
        .main
        .updated_at(&reader)?
        .ok_or(Error::internal(
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
        uid: path.index_uid.clone(),
        created_at,
        updated_at,
        primary_key,
    };

    Ok(HttpResponse::Ok().json(index_response))
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

    let created_index = database
        .create_index(&uid)
        .map_err(|e| match e {
            meilisearch_core::Error::IndexAlreadyExists => Error::IndexAlreadyExists(uid.clone()),
            _ => Error::create_index(e)
    })?;

    let index_response = database.main_write::<_, _, Error>(|mut write_txn| {
        created_index.main.put_name(&mut write_txn, &name)?;

        let created_at = created_index
            .main
            .created_at(&write_txn)?
            .ok_or(Error::internal("Impossible to read created at"))?;
    
        let updated_at = created_index
            .main
            .updated_at(&write_txn)?
            .ok_or(Error::internal("Impossible to read updated at"))?;
    
        if let Some(id) = primary_key.clone() {
            if let Some(mut schema) = created_index.main.schema(&write_txn)? {
                schema
                    .set_primary_key(&id)
                    .map_err(Error::bad_request)?;
                created_index.main.put_schema(&mut write_txn, &schema)?;
            }
        }
        let index_response = IndexResponse {
            name,
            uid,
            created_at,
            updated_at,
            primary_key,
        };
        Ok(index_response)
    })?;

    Ok(index_response)
}

#[post("/indexes", wrap = "Authentication::Private")]
async fn create_index(
    data: web::Data<Data>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    if let (None, None) = (body.name.clone(), body.uid.clone()) {
        return Err(Error::bad_request(
            "Index creation must have an uid",
        ).into());
    }

    let uid = match &body.uid {
        Some(uid) => {
            if uid
                .chars()
                .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
            {
                uid.to_owned()
            } else {
                return Err(Error::InvalidIndexUid.into());
            }
        }
        None => loop {
            let uid = generate_uid();
            if data.db.open_index(&uid).is_none() {
                break uid;
            }
        },
    };

    let name = body.name.as_ref().unwrap_or(&uid).to_string();

    let index_response = create_index_sync(&data.db, uid, name, body.primary_key.clone())?;

    Ok(HttpResponse::Created().json(index_response))
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
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    data.db.main_write::<_, _, ResponseError>(|writer| {
        if let Some(name) = &body.name {
            index.main.put_name(writer, name)?;
        }

        if let Some(id) = body.primary_key.clone() {
            if let Some(mut schema) = index.main.schema(writer)? {
                schema.set_primary_key(&id)?;
                index.main.put_schema(writer, &schema)?;
            }
        }
        index.main.put_updated_at(writer)?;
        Ok(())
    })?;

    let reader = data.db.main_read_txn()?;
    let name = index.main.name(&reader)?.ok_or(Error::internal(
            "Impossible to get the name of an index",
    ))?;
    let created_at = index
        .main
        .created_at(&reader)?
        .ok_or(Error::internal(
                "Impossible to get the create date of an index",
        ))?;
    let updated_at = index
        .main
        .updated_at(&reader)?
        .ok_or(Error::internal(
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
        uid: path.index_uid.clone(),
        created_at,
        updated_at,
        primary_key,
    };

    Ok(HttpResponse::Ok().json(index_response))
}

#[delete("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn delete_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    if data.db.delete_index(&path.index_uid)? {
        Ok(HttpResponse::NoContent().finish())
    } else {
        Err(Error::index_not_found(&path.index_uid).into())
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
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let reader = data.db.update_read_txn()?;

    let status = index.update_status(&reader, path.update_id)?;

    match status {
        Some(status) => Ok(HttpResponse::Ok().json(status)),
        None => Err(Error::NotFound(format!(
            "Update {}",
            path.update_id
        )).into()),
    }
}
pub fn get_all_updates_status_sync(
    data: &web::Data<Data>,
    reader: &UpdateReader,
    index_uid: &str,
) -> Result<Vec<UpdateStatus>, Error> {
    let index = data
        .db
        .open_index(index_uid)
        .ok_or(Error::index_not_found(index_uid))?;

    Ok(index.all_updates_status(reader)?)
}

#[get("/indexes/{index_uid}/updates", wrap = "Authentication::Private")]
async fn get_all_updates_status(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {

    let reader = data.db.update_read_txn()?;

    let response = get_all_updates_status_sync(&data, &reader, &path.index_uid)?;

    Ok(HttpResponse::Ok().json(response))
}

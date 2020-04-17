use actix_web::{delete, get, post, put, web, HttpResponse};
use chrono::{DateTime, Utc};
use log::error;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use crate::error::ResponseError;
use crate::routes::IndexParam;
use crate::Data;

fn generate_uid() -> String {
    let mut rng = rand::thread_rng();
    let sample = b"abcdefghijklmnopqrstuvwxyz0123456789";
    sample
        .choose_multiple(&mut rng, 8)
        .map(|c| *c as char)
        .collect()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

#[get("/indexes")]
pub async fn list_indexes(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let reader = data.db.main_read_txn()?;

    let mut response = Vec::new();

    for index_uid in data.db.indexes_uids() {
        let index = data.db.open_index(&index_uid);

        match index {
            Some(index) => {
                let name = index.main.name(&reader)?.ok_or(ResponseError::internal(
                    "Impossible to get the name of an index",
                ))?;
                let created_at = index
                    .main
                    .created_at(&reader)?
                    .ok_or(ResponseError::internal(
                        "Impossible to get the create date of an index",
                    ))?;
                let updated_at = index
                    .main
                    .updated_at(&reader)?
                    .ok_or(ResponseError::internal(
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
                response.push(index_response);
            }
            None => error!(
                "Index {} is referenced in the indexes list but cannot be found",
                index_uid
            ),
        }
    }

    Ok(HttpResponse::Ok().json(response))
}

#[get("/indexes/{index_uid}")]
pub async fn get_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let reader = data.db.main_read_txn()?;

    let name = index.main.name(&reader)?.ok_or(ResponseError::internal(
        "Impossible to get the name of an index",
    ))?;
    let created_at = index
        .main
        .created_at(&reader)?
        .ok_or(ResponseError::internal(
            "Impossible to get the create date of an index",
        ))?;
    let updated_at = index
        .main
        .updated_at(&reader)?
        .ok_or(ResponseError::internal(
            "Impossible to get the last update date of an index",
        ))?;

    let primary_key = match index.main.schema(&reader) {
        Ok(Some(schema)) => match schema.primary_key() {
            Some(primary_key) => Some(primary_key.to_owned()),
            None => None,
        },
        _ => None,
    };

    Ok(HttpResponse::Ok().json(IndexResponse {
        name,
        uid: path.index_uid.clone(),
        created_at,
        updated_at,
        primary_key,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexCreateRequest {
    name: Option<String>,
    uid: Option<String>,
    primary_key: Option<String>,
}

#[post("/indexes")]
pub async fn create_index(
    data: web::Data<Data>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    if let (None, None) = (body.name.clone(), body.uid.clone()) {
        return Err(ResponseError::bad_request(
            "Index creation must have an uid",
        ));
    }

    let uid = match body.uid.clone() {
        Some(uid) => {
            if uid
                .chars()
                .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
            {
                uid
            } else {
                return Err(ResponseError::InvalidIndexUid);
            }
        }
        None => loop {
            let uid = generate_uid();
            if data.db.open_index(&uid).is_none() {
                break uid;
            }
        },
    };

    let created_index = data
        .db
        .create_index(&uid)
        .map_err(ResponseError::create_index)?;

    let mut writer = data.db.main_write_txn()?;

    let name = body.name.clone().unwrap_or(uid.clone());
    created_index.main.put_name(&mut writer, &name)?;

    let created_at = created_index
        .main
        .created_at(&writer)?
        .ok_or(ResponseError::internal("Impossible to read created at"))?;

    let updated_at = created_index
        .main
        .updated_at(&writer)?
        .ok_or(ResponseError::internal("Impossible to read updated at"))?;

    if let Some(id) = body.primary_key.clone() {
        if let Some(mut schema) = created_index.main.schema(&writer)? {
            schema
                .set_primary_key(&id)
                .map_err(ResponseError::bad_request)?;
            created_index.main.put_schema(&mut writer, &schema)?;
        }
    }

    writer.commit()?;

    Ok(HttpResponse::Created().json(IndexResponse {
        name,
        uid,
        created_at,
        updated_at,
        primary_key: body.primary_key.clone(),
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateIndexRequest {
    name: Option<String>,
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

#[put("/indexes/{index_uid}")]
pub async fn update_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let mut writer = data.db.main_write_txn()?;

    if let Some(name) = body.name.clone() {
        index.main.put_name(&mut writer, &name)?;
    }

    if let Some(id) = body.primary_key.clone() {
        if let Some(mut schema) = index.main.schema(&writer)? {
            match schema.primary_key() {
                Some(_) => {
                    return Err(ResponseError::bad_request(
                        "The primary key cannot be updated",
                    ));
                }
                None => {
                    schema.set_primary_key(&id)?;
                    index.main.put_schema(&mut writer, &schema)?;
                }
            }
        }
    }

    index.main.put_updated_at(&mut writer)?;
    writer.commit()?;

    let reader = data.db.main_read_txn()?;

    let name = index.main.name(&reader)?.ok_or(ResponseError::internal(
        "Impossible to get the name of an index",
    ))?;
    let created_at = index
        .main
        .created_at(&reader)?
        .ok_or(ResponseError::internal(
            "Impossible to get the create date of an index",
        ))?;
    let updated_at = index
        .main
        .updated_at(&reader)?
        .ok_or(ResponseError::internal(
            "Impossible to get the last update date of an index",
        ))?;

    let primary_key = match index.main.schema(&reader) {
        Ok(Some(schema)) => match schema.primary_key() {
            Some(primary_key) => Some(primary_key.to_owned()),
            None => None,
        },
        _ => None,
    };

    Ok(HttpResponse::Ok().json(IndexResponse {
        name,
        uid: path.index_uid.clone(),
        created_at,
        updated_at,
        primary_key,
    }))
}

#[delete("/indexes/{index_uid}")]
pub async fn delete_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    data.db.delete_index(&path.index_uid)?;

    Ok(HttpResponse::NoContent().finish())
}

#[derive(Default, Deserialize)]
pub struct UpdateParam {
    index_uid: String,
    update_id: u64,
}

#[get("/indexes/{index_uid}/updates/{update_id}")]
pub async fn get_update_status(
    data: web::Data<Data>,
    path: web::Path<UpdateParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let reader = data.db.update_read_txn()?;

    let status = index.update_status(&reader, path.update_id)?;

    match status {
        Some(status) => Ok(HttpResponse::Ok().json(status)),
        None => Err(ResponseError::NotFound(format!(
            "Update {} not found",
            path.update_id
        ))),
    }
}

#[get("/indexes/{index_uid}/updates")]
pub async fn get_all_updates_status(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let reader = data.db.update_read_txn()?;

    let response = index.all_updates_status(&reader)?;

    Ok(HttpResponse::Ok().json(response))
}

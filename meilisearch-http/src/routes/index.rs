use chrono::{DateTime, Utc};
use log::error;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use actix_web::{web, get, post, delete, HttpResponse};
use actix_web as aweb;
use meilisearch_core::UpdateStatus;

use crate::error::ResponseError;
use crate::Data;
use crate::routes::IndexParam;

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
pub async fn list_indexes(
    data: web::Data<Data>,
) -> aweb::Result<web::Json<Vec<IndexResponse>>> {

    let reader = data.db.main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let mut response_body = Vec::new();

    for index_uid in data.db.indexes_uids() {
        let index = data.db.open_index(&index_uid);

        match index {
            Some(index) => {
                let name = index.main.name(&reader)
                    .map_err(|e| ResponseError::Internal(e.to_string()))?
                    .ok_or(ResponseError::Internal("Impossible to get the name of an index".to_string()))?;
                let created_at = index.main.created_at(&reader)
                    .map_err(|e| ResponseError::Internal(e.to_string()))?
                    .ok_or(ResponseError::Internal("Impossible to get the create date of an index".to_string()))?;
                let updated_at = index.main.updated_at(&reader)
                    .map_err(|e| ResponseError::Internal(e.to_string()))?
                    .ok_or(ResponseError::Internal("Impossible to get the last update date of an index".to_string()))?;

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
                response_body.push(index_response);
            }
            None => error!(
                "Index {} is referenced in the indexes list but cannot be found",
                index_uid
            ),
        }
    }

    Ok(web::Json(response_body))
}

#[get("/indexes/{index_uid}")]
pub async fn get_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<web::Json<IndexResponse>> {

    let index = data.db.open_index(path.index_uid.clone())
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let reader = data.db.main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let name = index.main.name(&reader)
        .map_err(|e| ResponseError::Internal(e.to_string()))?
        .ok_or(ResponseError::Internal("Impossible to get the name of an index".to_string()))?;
    let created_at = index.main.created_at(&reader)
        .map_err(|e| ResponseError::Internal(e.to_string()))?
        .ok_or(ResponseError::Internal("Impossible to get the create date of an index".to_string()))?;
    let updated_at = index.main.updated_at(&reader)
        .map_err(|e| ResponseError::Internal(e.to_string()))?
        .ok_or(ResponseError::Internal("Impossible to get the last update date of an index".to_string()))?;

    let primary_key = match index.main.schema(&reader) {
        Ok(Some(schema)) => match schema.primary_key() {
            Some(primary_key) => Some(primary_key.to_owned()),
            None => None,
        },
        _ => None,
    };

    Ok(web::Json(IndexResponse {
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
    body: web::Json<IndexCreateRequest>
) -> aweb::Result<web::Json<IndexResponse>> {

    if let (None, None) = (body.name.clone(), body.uid.clone()) {
        return Err(ResponseError::BadRequest("Index creation must have an uid".to_string()).into());
    }

    let uid = match body.uid.clone() {
        Some(uid) => {
            if uid
                .chars()
                .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
            {
                uid
            } else {
                return Err(ResponseError::InvalidIndexUid.into());
            }
        }
        None => loop {
            let uid = generate_uid();
            if data.db.open_index(&uid).is_none() {
                break uid;
            }
        },
    };

    let created_index = data.db.create_index(&uid)
        .map_err(|e| ResponseError::CreateIndex(e.to_string()))?;

    let mut writer = data.db.main_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let name = body.name.clone().unwrap_or(uid.clone());
    created_index.main.put_name(&mut writer, &name)
        .map_err(|e| ResponseError::Internal(e.to_string()))?;

    let created_at = created_index
        .main
        .created_at(&writer)
        .map_err(|e| ResponseError::Internal(e.to_string()))?
        .ok_or(ResponseError::Internal("".to_string()))?;

    let updated_at = created_index
        .main
        .updated_at(&writer)
        .map_err(|e| ResponseError::Internal(e.to_string()))?
        .ok_or(ResponseError::Internal("".to_string()))?;

    if let Some(id) = body.primary_key.clone() {
        if let Some(mut schema) = created_index.main.schema(&mut writer)
            .map_err(|e| ResponseError::Internal(e.to_string()))? {
            schema.set_primary_key(&id)
                .map_err(|e| ResponseError::BadRequest(e.to_string()))?;
            created_index.main.put_schema(&mut writer, &schema)
                .map_err(|e| ResponseError::Internal(e.to_string()))?;
        }
    }

    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(web::Json(IndexResponse {
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

#[post("/indexes/{index_uid}")]
pub async fn update_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<IndexCreateRequest>
) -> aweb::Result<web::Json<IndexResponse>> {

    let index = data.db.open_index(path.index_uid.clone())
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let mut writer = data.db.main_write_txn()
            .map_err(|err| ResponseError::Internal(err.to_string()))?;

    if let Some(name) = body.name.clone() {
        index.main.put_name(&mut writer, &name)
            .map_err(|e| ResponseError::Internal(e.to_string()))?;
    }

    if let Some(id) = body.primary_key.clone() {
        if let Some(mut schema) = index.main.schema(&mut writer)
            .map_err(|e| ResponseError::Internal(e.to_string()))? {
            match schema.primary_key() {
                Some(_) => {
                    return Err(ResponseError::BadRequest("The primary key cannot be updated".to_string()).into());
                }
                None => {
                    schema
                        .set_primary_key(&id)
                        .map_err(|e| ResponseError::Internal(e.to_string()))?;
                    index.main.put_schema(&mut writer, &schema)
                        .map_err(|e| ResponseError::Internal(e.to_string()))?;
                }
            }
        }
    }

    index.main.put_updated_at(&mut writer)
        .map_err(|e| ResponseError::Internal(e.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let reader = data.db.main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let name = index.main.name(&reader)
        .map_err(|e| ResponseError::Internal(e.to_string()))?
        .ok_or(ResponseError::Internal("Impossible to get the name of an index".to_string()))?;
    let created_at = index.main.created_at(&reader)
        .map_err(|e| ResponseError::Internal(e.to_string()))?
        .ok_or(ResponseError::Internal("Impossible to get the create date of an index".to_string()))?;
    let updated_at = index.main.updated_at(&reader)
        .map_err(|e| ResponseError::Internal(e.to_string()))?
        .ok_or(ResponseError::Internal("Impossible to get the last update date of an index".to_string()))?;

    let primary_key = match index.main.schema(&reader) {
        Ok(Some(schema)) => match schema.primary_key() {
            Some(primary_key) => Some(primary_key.to_owned()),
            None => None,
        },
        _ => None,
    };

    Ok(web::Json(IndexResponse {
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
) -> aweb::Result<HttpResponse> {

    data.db.delete_index(&path.index_uid)
        .map_err(|e| ResponseError::Internal(e.to_string()))?;

    HttpResponse::NoContent().await
}

#[derive(Default, Deserialize)]
pub struct UpdateParam {
    index_uid: String,
    update_id: u64
}

#[get("/indexes/{index_uid}/updates/{update_id}")]
pub async fn get_update_status(
    data: web::Data<Data>,
    path: web::Path<UpdateParam>,
) -> aweb::Result<web::Json<UpdateStatus>> {

    let index = data.db.open_index(path.index_uid.clone())
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let reader = data.db.update_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let status = index.update_status(&reader, path.update_id)
        .map_err(|e| ResponseError::Internal(e.to_string()))?;

    match status {
        Some(status) => Ok(web::Json(status)),
        None => Err(ResponseError::NotFound(format!("Update {} not found", path.update_id)).into())
    }
}

#[get("/indexes/{index_uid}/updates")]
pub async fn get_all_updates_status(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<web::Json<Vec<UpdateStatus>>> {

    let index = data.db.open_index(path.index_uid.clone())
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let reader = data.db.update_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let response = index.all_updates_status(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(web::Json(response))
}

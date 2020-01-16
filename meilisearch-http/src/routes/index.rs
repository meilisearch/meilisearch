use chrono::{DateTime, Utc};
use log::error;
use meilisearch_core::ProcessedUpdateResult;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tide::{Request, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::RequestExt;
use crate::models::token::ACL::*;
use crate::Data;

fn generate_uid() -> String {
    let mut rng = rand::thread_rng();
    let sample = b"abcdefghijklmnopqrstuvwxyz0123456789";
    sample
        .choose_multiple(&mut rng, 8)
        .map(|c| *c as char)
        .collect()
}

pub async fn list_indexes(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let indexes_uids = ctx.state().db.indexes_uids();

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let mut response_body = Vec::new();

    for index_uid in indexes_uids {
        let index = ctx.state().db.open_index(&index_uid);

        match index {
            Some(index) => {
                let name = index
                    .main
                    .name(&reader)
                    .map_err(ResponseError::internal)?
                    .ok_or(ResponseError::internal("'name' not found"))?;
                let created_at = index
                    .main
                    .created_at(&reader)
                    .map_err(ResponseError::internal)?
                    .ok_or(ResponseError::internal("'created_at' date not found"))?;
                let updated_at = index
                    .main
                    .updated_at(&reader)
                    .map_err(ResponseError::internal)?
                    .ok_or(ResponseError::internal("'updated_at' date not found"))?;

                let index_response = IndexResponse {
                    name,
                    uid: index_uid,
                    created_at,
                    updated_at,
                };
                response_body.push(index_response);
            }
            None => error!(
                "Index {} is referenced in the indexes list but cannot be found",
                index_uid
            ),
        }
    }

    Ok(tide::Response::new(200).body_json(&response_body).unwrap())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

pub async fn get_index(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let index = ctx.index()?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let uid = ctx.url_param("index")?;
    let name = index
        .main
        .name(&reader)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::internal("'name' not found"))?;
    let created_at = index
        .main
        .created_at(&reader)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::internal("'created_at' date not found"))?;
    let updated_at = index
        .main
        .updated_at(&reader)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::internal("'updated_at' date not found"))?;

    let response_body = IndexResponse {
        name,
        uid,
        created_at,
        updated_at,
    };

    Ok(tide::Response::new(200).body_json(&response_body).unwrap())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IndexCreateRequest {
    name: Option<String>,
    uid: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexCreateResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

pub async fn create_index(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesWrite)?;

    let body = ctx
        .body_json::<IndexCreateRequest>()
        .await
        .map_err(ResponseError::bad_request)?;

    if let (None, None) = (body.name.clone(), body.uid.clone()) {
        return Err(ResponseError::bad_request("Index creation must have an uid"));
    }

    let db = &ctx.state().db;

    let uid = match body.uid {
        Some(uid) => uid,
        None => loop {
            let uid = generate_uid();
            if db.open_index(&uid).is_none() {
                break uid;
            }
        },
    };

    let created_index = match db.create_index(&uid) {
        Ok(index) => index,
        Err(e) => return Err(ResponseError::create_index(e)),
    };

    let mut writer = db.main_write_txn().map_err(ResponseError::internal)?;

    let name = body.name.unwrap_or(uid.clone());

    created_index.main
        .put_name(&mut writer, &name)
        .map_err(ResponseError::internal)?;

    let created_at = created_index.main
        .created_at(&writer)
        .map_err(ResponseError::internal)?
        .unwrap_or(Utc::now());
    let updated_at = created_index.main
        .updated_at(&writer)
        .map_err(ResponseError::internal)?
        .unwrap_or(Utc::now());

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexCreateResponse {
        name: name,
        uid,
        created_at,
        updated_at,
    };

    Ok(tide::Response::new(201).body_json(&response_body).unwrap())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateIndexRequest {
    name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateIndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

pub async fn update_index(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesWrite)?;

    let body = ctx
        .body_json::<UpdateIndexRequest>()
        .await
        .map_err(ResponseError::bad_request)?;

    let index_uid = ctx.url_param("index")?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.main_write_txn().map_err(ResponseError::internal)?;

    index
        .main
        .put_name(&mut writer, &body.name)
        .map_err(ResponseError::internal)?;

    index
        .main
        .put_updated_at(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let created_at = index
        .main
        .created_at(&reader)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::internal("'created_at' date not found"))?;
    let updated_at = index
        .main
        .updated_at(&reader)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::internal("'updated_at' date not found"))?;

    let response_body = UpdateIndexResponse {
        name: body.name,
        uid: index_uid,
        created_at,
        updated_at,
    };

    Ok(tide::Response::new(200).body_json(&response_body).unwrap())
}

pub async fn get_update_status(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let db = &ctx.state().db;
    let reader = db.update_read_txn().map_err(ResponseError::internal)?;

    let update_id = ctx
        .param::<u64>("update_id")
        .map_err(|e| ResponseError::bad_parameter("update_id", e))?;

    let index = ctx.index()?;
    let status = index
        .update_status(&reader, update_id)
        .map_err(ResponseError::internal)?;

    let response = match status {
        Some(status) => tide::Response::new(200).body_json(&status).unwrap(),
        None => tide::Response::new(404).body_json(&json!({ "message": "unknown update id" })).unwrap(),
    };

    Ok(response)
}

pub async fn get_all_updates_status(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let db = &ctx.state().db;
    let reader = db.update_read_txn().map_err(ResponseError::internal)?;

    let index = ctx.index()?;
    let response = index
        .all_updates_status(&reader)
        .map_err(ResponseError::internal)?;

    Ok(tide::Response::new(200).body_json(&response).unwrap())
}

pub async fn delete_index(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesWrite)?;
    let _ = ctx.index()?;
    let index_uid = ctx.url_param("index")?;
    ctx.state().db.delete_index(&index_uid).map_err(ResponseError::internal)?;
    Ok(tide::Response::new(204))
}

pub fn index_update_callback(index_uid: &str, data: &Data, status: ProcessedUpdateResult) {
    if status.error.is_some() {
        return;
    }

    if let Some(index) = data.db.open_index(&index_uid) {
        let db = &data.db;
        let mut writer = match db.main_write_txn() {
            Ok(writer) => writer,
            Err(e) => {
                error!("Impossible to get write_txn; {}", e);
                return;
            }
        };

        if let Err(e) = data.compute_stats(&mut writer, &index_uid) {
            error!("Impossible to compute stats; {}", e)
        }

        if let Err(e) = data.set_last_update(&mut writer) {
            error!("Impossible to update last_update; {}", e)
        }

        if let Err(e) = index.main.put_updated_at(&mut writer) {
            error!("Impossible to update updated_at; {}", e)
        }

        if let Err(e) = writer.commit() {
            error!("Impossible to get write_txn; {}", e);
        }
    }
}

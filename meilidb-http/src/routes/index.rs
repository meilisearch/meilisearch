use chrono::{DateTime, Utc};
use http::StatusCode;
use log::error;
use meilidb_core::ProcessedUpdateResult;
use meilidb_schema::{Schema, SchemaBuilder};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tide::querystring::ContextExt as QSContextExt;
use tide::response::IntoResponse;
use tide::{Context, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::ContextExt;
use crate::models::schema::SchemaBody;
use crate::models::token::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

fn generate_uid() -> String {
    let mut rng = rand::thread_rng();
    let sample = b"abcdefghijklmnopqrstuvwxyz0123456789";
    sample
        .choose_multiple(&mut rng, 8)
        .map(|c| *c as char)
        .collect()
}

pub async fn list_indexes(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let indexes_uids = ctx.state().db.indexes_uids();

    let env = &ctx.state().db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

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

                let index_reponse = IndexResponse {
                    name,
                    uid: index_uid,
                    created_at,
                    updated_at,
                };
                response_body.push(index_reponse);
            }
            None => error!(
                "Index {} is referenced in the indexes list but cannot be found",
                index_uid
            ),
        }
    }

    Ok(tide::response::json(response_body))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

pub async fn get_index(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let index = ctx.index()?;

    let env = &ctx.state().db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

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

    Ok(tide::response::json(response_body))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IndexCreateRequest {
    name: String,
    schema: Option<SchemaBody>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexCreateResponse {
    name: String,
    uid: String,
    schema: Option<SchemaBody>,
    #[serde(skip_serializing_if = "Option::is_none")]
    update_id: Option<u64>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

pub async fn create_index(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesWrite)?;

    let body = ctx
        .body_json::<IndexCreateRequest>()
        .await
        .map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;

    let generated_uid = loop {
        let uid = generate_uid();
        if db.open_index(&uid).is_none() {
            break uid;
        }
    };

    let created_index = match db.create_index(&generated_uid) {
        Ok(index) => index,
        Err(e) => return Err(ResponseError::create_index(e)),
    };

    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    created_index
        .main
        .put_name(&mut writer, &body.name)
        .map_err(ResponseError::internal)?;
    created_index
        .main
        .put_created_at(&mut writer)
        .map_err(ResponseError::internal)?;
    created_index
        .main
        .put_updated_at(&mut writer)
        .map_err(ResponseError::internal)?;

    let schema: Option<Schema> = body.schema.clone().map(Into::into);
    let mut response_update_id = None;
    if let Some(schema) = schema {
        let update_id = created_index
            .schema_update(&mut writer, schema)
            .map_err(ResponseError::internal)?;
        response_update_id = Some(update_id)
    }

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexCreateResponse {
        name: body.name,
        uid: generated_uid,
        schema: body.schema,
        update_id: response_update_id,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    Ok(tide::response::json(response_body)
        .with_status(StatusCode::CREATED)
        .into_response())
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

pub async fn update_index(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesWrite)?;

    let body = ctx
        .body_json::<UpdateIndexRequest>()
        .await
        .map_err(ResponseError::bad_request)?;

    let index_uid = ctx.url_param("index")?;
    let index = ctx.index()?;

    let db = &ctx.state().db;

    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    index
        .main
        .put_name(&mut writer, &body.name)
        .map_err(ResponseError::internal)?;

    index
        .main
        .put_updated_at(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

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

    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SchemaParams {
    raw: bool,
}

pub async fn get_index_schema(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let index = ctx.index()?;

    // Tide doesn't support "no query param"
    let params: SchemaParams = ctx.url_query().unwrap_or_default();

    let env = &ctx.state().db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

    let schema = index
        .main
        .schema(&reader)
        .map_err(ResponseError::open_index)?;

    match schema {
        Some(schema) => {
            if params.raw {
                Ok(tide::response::json(schema))
            } else {
                Ok(tide::response::json(SchemaBody::from(schema)))
            }
        }
        None => Err(ResponseError::not_found("missing index schema")),
    }
}

pub async fn update_schema(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesWrite)?;

    let index_uid = ctx.url_param("index")?;

    let params: SchemaParams = ctx.url_query().unwrap_or_default();

    let schema = if params.raw {
        ctx.body_json::<SchemaBuilder>()
            .await
            .map_err(ResponseError::bad_request)?
            .build()
    } else {
        ctx.body_json::<SchemaBody>()
            .await
            .map_err(ResponseError::bad_request)?
            .into()
    };

    let db = &ctx.state().db;
    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    let index = db
        .open_index(&index_uid)
        .ok_or(ResponseError::index_not_found(index_uid))?;

    let update_id = index
        .schema_update(&mut writer, schema.clone())
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn get_update_status(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let env = &ctx.state().db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

    let update_id = ctx
        .param::<u64>("update_id")
        .map_err(|e| ResponseError::bad_parameter("update_id", e))?;

    let index = ctx.index()?;
    let status = index
        .update_status(&reader, update_id)
        .map_err(ResponseError::internal)?;

    let response = match status {
        Some(status) => tide::response::json(status)
            .with_status(StatusCode::OK)
            .into_response(),
        None => tide::response::json(json!({ "message": "unknown update id" }))
            .with_status(StatusCode::NOT_FOUND)
            .into_response(),
    };

    Ok(response)
}

pub async fn get_all_updates_status(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let env = &ctx.state().db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

    let index = ctx.index()?;
    let all_status = index
        .all_updates_status(&reader)
        .map_err(ResponseError::internal)?;

    let response = tide::response::json(all_status)
        .with_status(StatusCode::OK)
        .into_response();

    Ok(response)
}

pub async fn delete_index(ctx: Context<Data>) -> SResult<StatusCode> {
    ctx.is_allowed(IndexesWrite)?;
    let index_uid = ctx.url_param("index")?;

    let found = ctx
        .state()
        .db
        .delete_index(&index_uid)
        .map_err(ResponseError::internal)?;

    if found {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

pub fn index_update_callback(index_uid: &str, data: &Data, status: ProcessedUpdateResult) {
    if status.error.is_some() {
        return;
    }

    if let Some(index) = data.db.open_index(&index_uid) {
        let env = &data.db.env;
        let mut writer = match env.write_txn() {
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

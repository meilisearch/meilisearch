use chrono::{DateTime, Utc};
use http::StatusCode;
use meilidb_core::ProcessedUpdateResult;
use meilidb_schema::Schema;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tide::querystring::ContextExt as QSContextExt;
use tide::response::IntoResponse;
use tide::{Context, Response};
use chrono::{DateTime, Utc};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::ContextExt;
use crate::models::schema::SchemaBody;
use crate::models::token::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

fn generate_uid() -> String {
    let mut rng = rand::thread_rng();
    let sample = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    sample
        .choose_multiple(&mut rng, 8)
        .map(|c| *c as char)
        .collect()
}

pub async fn list_indexes(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;
    let list = ctx
        .state()
        .db
        .indexes_uids()
        .map_err(ResponseError::internal)?;
    Ok(tide::response::json(list))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GetIndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

pub async fn get_index(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let index = ctx.index()?;

    let env = &ctx.state().db.env;
    let mut reader = env.read_txn().map_err(ResponseError::internal)?;

    let uid = ctx.url_param("index")?.to_string();
    let name = index.main.name(&mut reader)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::internal("Name not found"))?;
    let created_at = index.main.created_at(&mut reader)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::internal("Created date not found"))?;
    let updated_at = index.main.updated_at(&mut reader)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::internal("Updated date not found"))?;

    let response_body = GetIndexResponse {
        name,
        uid,
        created_at,
        updated_at,
    };

    Ok(tide::response::json(response_body))
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetSchemaParams {
    raw: bool,
}

pub async fn get_index_schema(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let index = ctx.index()?;

    // Tide doesn't support "no query param"
    let params: GetSchemaParams = ctx.url_query().unwrap_or_default();

    let env = &ctx.state().db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

    let schema = index
        .main
        .schema(&reader)
        .map_err(ResponseError::open_index)?;

    match schema {
        Some(schema) => {
            if params.raw {
                Ok(tide::response::json(schema.to_builder()))
            } else {
                Ok(tide::response::json(SchemaBody::from(schema)))
            }
        }
        None => Ok(
            tide::response::json(json!({ "message": "missing index schema" }))
                .with_status(StatusCode::NOT_FOUND)
                .into_response(),
        ),
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IndexCreateRequest {
    name: String,
    schema: Option<SchemaBody>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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

    let body = ctx.body_json::<IndexCreateRequest>().await.map_err(ResponseError::bad_request)?;

    let generated_uid = generate_uid();

    let db = &ctx.state().db;

    let created_index = match db.create_index(&generated_uid) {
        Ok(index) => index,
        Err(e) => return Err(ResponseError::create_index(e)),
    };

    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    created_index.main
        .put_name(&mut writer, &body.name)
        .map_err(ResponseError::internal)?;
    created_index.main
        .put_created_at(&mut writer)
        .map_err(ResponseError::internal)?;
    created_index.main
        .put_updated_at(&mut writer)
        .map_err(ResponseError::internal)?;

    let schema: Option<Schema> = body.schema.clone().map(|s| s.into());
    let mut response_update_id = None;
    if let Some(schema) = schema {
        let update_id = created_index
                .schema_update(&mut writer, schema.clone())
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

pub async fn update_schema(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesWrite)?;

    let index_uid = ctx.url_param("index")?;

    let schema = ctx
        .body_json::<SchemaBody>()
        .await
        .map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;
    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    let index = db
        .open_index(&index_uid)
        .ok_or(ResponseError::index_not_found(index_uid))?;

    let schema: meilidb_schema::Schema = schema.into();
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

pub fn index_update_callback(index_uid: &str, data: &Data, _status: ProcessedUpdateResult) {
    let env = &data.db.env;
    let mut writer = env.write_txn().unwrap();

    data.compute_stats(&mut writer, &index_uid).unwrap();
    data.set_last_update(&mut writer, &index_uid).unwrap();

    writer.commit().unwrap();
}

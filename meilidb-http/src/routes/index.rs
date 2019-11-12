use http::StatusCode;
use meilidb_core::{ProcessedUpdateResult, UpdateStatus};
use meilidb_schema::Schema;
use serde_json::json;
use tide::response::IntoResponse;
use tide::{Context, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::ContextExt;
use crate::models::schema::SchemaBody;
use crate::models::token::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

pub async fn list_indexes(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;
    let list = ctx
        .state()
        .db
        .indexes_names()
        .map_err(ResponseError::internal)?;
    Ok(tide::response::json(list))
}

pub async fn get_index_schema(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesRead)?;

    let index = ctx.index()?;

    let env = &ctx.state().db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

    let schema = index
        .main
        .schema(&reader)
        .map_err(ResponseError::create_index)?;

    match schema {
        Some(schema) => {
            let schema = SchemaBody::from(schema);
            Ok(tide::response::json(schema))
        }
        None => Ok(
            tide::response::json(json!({ "message": "missing index schema" }))
                .with_status(StatusCode::NOT_FOUND)
                .into_response(),
        ),
    }
}

pub async fn create_index(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesWrite)?;

    let index_name = ctx.url_param("index")?;

    let body = ctx.body_bytes().await.map_err(ResponseError::bad_request)?;
    let schema: Option<Schema> = if body.is_empty() {
        None
    } else {
        serde_json::from_slice::<SchemaBody>(&body)
            .map_err(ResponseError::bad_request)
            .map(|s| Some(s.into()))?
    };

    let db = &ctx.state().db;

    let created_index = match db.create_index(&index_name) {
        Ok(index) => index,
        Err(meilidb_core::Error::IndexAlreadyExists) => db.open_index(&index_name).ok_or(
            ResponseError::internal("index not found but must have been found"),
        )?,
        Err(e) => return Err(ResponseError::create_index(e)),
    };

    let callback_context = ctx.state().clone();
    let callback_name = index_name.clone();
    db.set_update_callback(
        &index_name,
        Box::new(move |status| {
            index_update_callback(&callback_name, &callback_context, status);
        }),
    );

    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    match schema {
        Some(schema) => {
            let update_id = created_index
                .schema_update(&mut writer, schema.clone())
                .map_err(ResponseError::internal)?;

            writer.commit().map_err(ResponseError::internal)?;

            let response_body = IndexUpdateResponse { update_id };
            Ok(tide::response::json(response_body)
                .with_status(StatusCode::CREATED)
                .into_response())
        }
        None => Ok(Response::new(tide::Body::empty())
            .with_status(StatusCode::NO_CONTENT)
            .into_response()),
    }
}

pub async fn update_schema(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(IndexesWrite)?;

    let index_name = ctx.url_param("index")?;

    let schema = ctx
        .body_json::<SchemaBody>()
        .await
        .map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;
    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    let index = db
        .open_index(&index_name)
        .ok_or(ResponseError::index_not_found(index_name))?;

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
        UpdateStatus::Unknown => {
            tide::response::json(json!({ "message": "unknown update id" }))
                .with_status(StatusCode::NOT_FOUND)
                .into_response()
        }
        status => {
            tide::response::json(status)
                .with_status(StatusCode::OK)
                .into_response()
        }
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
    let index_name = ctx.url_param("index")?;

    let found = ctx
        .state()
        .db
        .delete_index(&index_name)
        .map_err(ResponseError::internal)?;

    if found {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

pub fn index_update_callback(index_name: &str, data: &Data, _status: ProcessedUpdateResult) {
    let env = &data.db.env;
    let mut writer = env.write_txn().unwrap();

    data.compute_stats(&mut writer, &index_name).unwrap();
    data.set_last_update(&mut writer, &index_name).unwrap();

    writer.commit().unwrap();
}

use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use crypto::digest::Digest;
use crypto::sha1::Sha1;
use heed::types::{SerdeBincode, Str};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tide::response::IntoResponse;
use tide::{Context, Response};
use uuid::Uuid;

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::ContextExt;
use crate::models::token::ACL::*;
use crate::models::token::*;
use crate::Data;

fn generate_api_key() -> String {
    let mut hasher = Sha1::new();
    hasher.input_str(&Uuid::new_v4().to_string());
    hasher.result_str().to_string()
}

pub async fn list(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;

    let db = &ctx.state().db;
    let env = &db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

    let common_store = db.common_store();

    let mut response: Vec<Token> = Vec::new();

    let iter = common_store
        .prefix_iter::<Str, SerdeBincode<Token>>(&reader, TOKEN_PREFIX_KEY)
        .map_err(ResponseError::internal)?;

    for result in iter {
        let (_, token) = result.map_err(ResponseError::internal)?;
        response.push(token);
    }

    Ok(tide::response::json(response))
}

pub async fn get(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;
    let request_key = ctx.url_param("key")?;

    let db = &ctx.state().db;
    let env = &db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

    let token_key = format!("{}{}", TOKEN_PREFIX_KEY, request_key);

    let token_config = db
        .common_store()
        .get::<Str, SerdeBincode<Token>>(&reader, &token_key)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::not_found(format!(
            "token key: {}",
            token_key
        )))?;

    Ok(tide::response::json(token_config))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreatedRequest {
    description: String,
    acl: Vec<ACL>,
    indexes: Vec<Wildcard>,
    #[serde(with = "ts_seconds")]
    expires_at: DateTime<Utc>,
}

pub async fn create(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;

    let data: CreatedRequest = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let key = generate_api_key();
    let token_key = format!("{}{}", TOKEN_PREFIX_KEY, key);

    let token_definition = Token {
        key,
        description: data.description,
        acl: data.acl,
        indexes: data.indexes,
        expires_at: data.expires_at,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        revoked: false,
    };

    let db = &ctx.state().db;
    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    db.common_store()
        .put::<Str, SerdeBincode<Token>>(&mut writer, &token_key, &token_definition)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    Ok(tide::response::json(token_definition)
        .with_status(StatusCode::CREATED)
        .into_response())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdatedRequest {
    description: Option<String>,
    acl: Option<Vec<ACL>>,
    indexes: Option<Vec<Wildcard>>,
}

pub async fn update(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;
    let request_key = ctx.url_param("key")?;

    let data: UpdatedRequest = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;
    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    let common_store = db.common_store();

    let token_key = format!("{}{}", TOKEN_PREFIX_KEY, request_key);

    let mut token_config = common_store
        .get::<Str, SerdeBincode<Token>>(&writer, &token_key)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::not_found(format!(
            "token key: {}",
            token_key
        )))?;

    // apply the modifications
    if let Some(description) = data.description {
        token_config.description = description;
    }

    if let Some(acl) = data.acl {
        token_config.acl = acl;
    }

    if let Some(indexes) = data.indexes {
        token_config.indexes = indexes;
    }

    token_config.updated_at = Utc::now();

    common_store
        .put::<Str, SerdeBincode<Token>>(&mut writer, &token_key, &token_config)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    Ok(tide::response::json(token_config)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn delete(ctx: Context<Data>) -> SResult<StatusCode> {
    ctx.is_allowed(Admin)?;
    let request_key = ctx.url_param("key")?;

    let db = &ctx.state().db;
    let env = &db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    let common_store = db.common_store();

    let token_key = format!("{}{}", TOKEN_PREFIX_KEY, request_key);

    common_store
        .delete::<Str>(&mut writer, &token_key)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    Ok(StatusCode::ACCEPTED)
}

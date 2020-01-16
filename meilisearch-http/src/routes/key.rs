use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use heed::types::{SerdeBincode, Str};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tide::{Request, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::RequestExt;
use crate::models::token::ACL::*;
use crate::models::token::*;
use crate::Data;

fn generate_api_key() -> String {
    let mut rng = rand::thread_rng();
    let sample = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    sample
        .choose_multiple(&mut rng, 40)
        .map(|c| *c as char)
        .collect()
}

pub async fn list(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let common_store = db.common_store();

    let mut response: Vec<Token> = Vec::new();

    let iter = common_store
        .prefix_iter::<_, Str, SerdeBincode<Token>>(&reader, TOKEN_PREFIX_KEY)?;

    for result in iter {
        let (_, token) = result?;
        response.push(token);
    }

    Ok(tide::Response::new(200).body_json(&response).unwrap())
}

pub async fn get(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;
    let request_key = ctx.url_param("key")?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let token_key = format!("{}{}", TOKEN_PREFIX_KEY, request_key);

    let token_config = db
        .common_store()
        .get::<_, Str, SerdeBincode<Token>>(&reader, &token_key)?
        .ok_or(ResponseError::not_found(format!(
            "token key: {}",
            token_key
        )))?;

    Ok(tide::Response::new(200).body_json(&token_config).unwrap())
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

pub async fn create(mut ctx: Request<Data>) -> SResult<Response> {
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
    let mut writer = db.main_write_txn()?;

    db.common_store().put::<_, Str, SerdeBincode<Token>>(&mut writer, &token_key, &token_definition)?;

    writer.commit()?;
    Ok(tide::Response::new(201).body_json(&token_definition).unwrap())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdatedRequest {
    description: Option<String>,
    acl: Option<Vec<ACL>>,
    indexes: Option<Vec<Wildcard>>,
    expires_at: Option<DateTime<Utc>>,
    revoked: Option<bool>,
}

pub async fn update(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;
    let request_key = ctx.url_param("key")?;

    let data: UpdatedRequest = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;
    let mut writer = db.main_write_txn()?;

    let common_store = db.common_store();

    let token_key = format!("{}{}", TOKEN_PREFIX_KEY, request_key);

    let mut token_config = common_store
        .get::<_, Str, SerdeBincode<Token>>(&writer, &token_key)?
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
    if let Some(expires_at) = data.expires_at {
        token_config.expires_at = expires_at;
    }
    if let Some(revoked) = data.revoked {
        token_config.revoked = revoked;
    }

    token_config.updated_at = Utc::now();
    common_store.put::<_, Str, SerdeBincode<Token>>(&mut writer, &token_key, &token_config)?;
    writer.commit()?;

    Ok(tide::Response::new(200).body_json(&token_config).unwrap())
}

pub async fn delete(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;
    let request_key = ctx.url_param("key")?;
    let db = &ctx.state().db;
    let mut writer = db.main_write_txn()?;
    let common_store = db.common_store();
    let token_key = format!("{}{}", TOKEN_PREFIX_KEY, request_key);
    common_store.delete::<_, Str>(&mut writer, &token_key)?;
    writer.commit()?;
    Ok(tide::Response::new(204))
}

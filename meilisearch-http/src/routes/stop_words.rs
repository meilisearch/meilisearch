use std::collections::BTreeSet;

use http::StatusCode;
use tide::response::IntoResponse;
use tide::{Context, Response};
use meilisearch_core::settings::{SettingsUpdate, UpdateState};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::ContextExt;
use crate::models::token::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

pub async fn get(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let stop_words_fst = index
        .main
        .stop_words_fst(&reader)
        .map_err(ResponseError::internal)?;

    let stop_words = stop_words_fst
        .unwrap_or_default()
        .stream()
        .into_strs()
        .map_err(ResponseError::internal)?;

    Ok(tide::response::json(stop_words))
}

pub async fn update(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;

    let data: BTreeSet<String> = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let settings = SettingsUpdate {
        stop_words: UpdateState::Update(data),
        .. SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn delete(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let settings = SettingsUpdate {
        stop_words: UpdateState::Clear,
        .. SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

use http::StatusCode;
use tide::response::IntoResponse;
use tide::{Context, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::ContextExt;
use crate::models::token::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

pub async fn list(ctx: Context<Data>) -> SResult<Response> {
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

pub async fn add(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;

    let data: Vec<String> = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut stop_words_addition = index.stop_words_addition();
    for stop_word in data {
        stop_words_addition.add_stop_word(stop_word);
    }

    let update_id = stop_words_addition
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn delete(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;

    let data: Vec<String> = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut stop_words_deletion = index.stop_words_deletion();
    for stop_word in data {
        stop_words_deletion.delete_stop_word(stop_word);
    }

    let update_id = stop_words_deletion
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

use std::collections::BTreeSet;

use meilisearch_core::settings::{SettingsUpdate, UpdateState};
use tide::{Request, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::RequestExt;
use crate::helpers::tide::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

pub async fn get(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;
    let stop_words_fst = index.main.stop_words_fst(&reader)?;
    let stop_words = stop_words_fst.unwrap_or_default().stream().into_strs()?;

    Ok(tide::Response::new(200).body_json(&stop_words).unwrap())
}

pub async fn update(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;

    let data: BTreeSet<String> = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        stop_words: UpdateState::Update(data),
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn delete(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        stop_words: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

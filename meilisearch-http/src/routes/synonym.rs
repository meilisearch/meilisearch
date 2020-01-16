use std::collections::BTreeMap;

use tide::{Request, Response};
use indexmap::IndexMap;
use meilisearch_core::settings::{SettingsUpdate, UpdateState};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::RequestExt;
use crate::models::token::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

pub async fn get(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let synonyms_fst = index
        .main
        .synonyms_fst(&reader)?;

    let synonyms_fst = synonyms_fst.unwrap_or_default();
    let synonyms_list = synonyms_fst.stream().into_strs()?;

    let mut response = IndexMap::new();

    let index_synonyms = &index.synonyms;

    for synonym in synonyms_list {
        let alternative_list = index_synonyms.synonyms(&reader, synonym.as_bytes())?;

        if let Some(list) = alternative_list {
            let list = list.stream().into_strs()?;
            response.insert(synonym, list);
        }
    }

    Ok(tide::Response::new(200).body_json(&response).unwrap())
}

pub async fn update(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;

    let data: BTreeMap<String, Vec<String>> = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        synonyms: UpdateState::Update(data),
        .. SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}


pub async fn delete(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;

    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        synonyms: UpdateState::Clear,
        .. SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

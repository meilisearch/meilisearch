use std::collections::HashMap;

use http::StatusCode;
use tide::response::IntoResponse;
use tide::{Context, Response};
use indexmap::IndexMap;

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

    let synonyms_fst = index
        .main
        .synonyms_fst(&reader)
        .map_err(ResponseError::internal)?;

    let synonyms_fst = synonyms_fst.unwrap_or_default();
    let synonyms_list = synonyms_fst.stream().into_strs().map_err(ResponseError::internal)?;

    let mut response = IndexMap::new();

    let index_synonyms = &index.synonyms;

    for synonym in synonyms_list {
        let alternative_list = index_synonyms
            .synonyms(&reader, synonym.as_bytes())
            .map_err(ResponseError::internal)?;

        if let Some(list) = alternative_list {
            let list = list.stream().into_strs().map_err(ResponseError::internal)?;
            response.insert(synonym, list);
        }
    }

    Ok(tide::response::json(response))
}

pub async fn update(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;

    let data: HashMap<String, Vec<String>> = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut synonyms_update = index.synonyms_update();

    for (input, synonyms) in data {
        synonyms_update.add_synonym(input, synonyms.into_iter());
    }

    let update_id = synonyms_update
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

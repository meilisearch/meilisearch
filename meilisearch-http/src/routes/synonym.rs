use std::collections::HashMap;

use http::StatusCode;
use serde::{Deserialize, Serialize};
use tide::response::IntoResponse;
use tide::{Context, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::ContextExt;
use crate::models::token::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Synonym {
    OneWay(SynonymOneWay),
    MultiWay { synonyms: Vec<String> },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SynonymOneWay {
    pub input: String,
    pub synonyms: Vec<String>,
}

pub type Synonyms = Vec<Synonym>;

pub async fn list(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let synonyms_fst = index
        .main
        .synonyms_fst(&reader)
        .map_err(ResponseError::internal)?;

    let synonyms_fst = synonyms_fst.unwrap_or_default();
    let synonyms_list = synonyms_fst.stream().into_strs().unwrap();

    let mut response = HashMap::new();

    let index_synonyms = &index.synonyms;

    for synonym in synonyms_list {
        let alternative_list = index_synonyms
            .synonyms(&reader, synonym.as_bytes())
            .unwrap()
            .unwrap()
            .stream()
            .into_strs()
            .unwrap();
        response.insert(synonym, alternative_list);
    }

    Ok(tide::response::json(response))
}

pub async fn get(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let synonym = ctx.url_param("synonym")?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let synonym_list = index
        .synonyms
        .synonyms(&reader, synonym.as_bytes())
        .unwrap()
        .unwrap()
        .stream()
        .into_strs()
        .unwrap();

    Ok(tide::response::json(synonym_list))
}

pub async fn create(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;

    let data: Synonym = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut synonyms_addition = index.synonyms_addition();

    match data.clone() {
        Synonym::OneWay(content) => {
            synonyms_addition.add_synonym(content.input, content.synonyms.into_iter())
        }
        Synonym::MultiWay { mut synonyms } => {
            if synonyms.len() > 1 {
                for _ in 0..synonyms.len() {
                    let (first, elems) = synonyms.split_first().unwrap();
                    synonyms_addition.add_synonym(first, elems.iter());
                    synonyms.rotate_left(1);
                }
            }
        }
    }

    let update_id = synonyms_addition
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn update(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let synonym = ctx.url_param("synonym")?;
    let index = ctx.index()?;
    let data: Vec<String> = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut synonyms_addition = index.synonyms_addition();
    synonyms_addition.add_synonym(synonym.clone(), data.clone().into_iter());
    let update_id = synonyms_addition
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn delete(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let synonym = ctx.url_param("synonym")?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut synonyms_deletion = index.synonyms_deletion();
    synonyms_deletion.delete_all_alternatives_of(synonym);
    let update_id = synonyms_deletion
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn batch_write(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;

    let data: Synonyms = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut synonyms_addition = index.synonyms_addition();
    for raw in data {
        match raw {
            Synonym::OneWay(content) => {
                synonyms_addition.add_synonym(content.input, content.synonyms.into_iter())
            }
            Synonym::MultiWay { mut synonyms } => {
                if synonyms.len() > 1 {
                    for _ in 0..synonyms.len() {
                        let (first, elems) = synonyms.split_first().unwrap();
                        synonyms_addition.add_synonym(first, elems.iter());
                        synonyms.rotate_left(1);
                    }
                }
            }
        }
    }
    let update_id = synonyms_addition
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn clear(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let synonyms_fst = index
        .main
        .synonyms_fst(&reader)
        .map_err(ResponseError::internal)?;

    let synonyms_fst = synonyms_fst.unwrap_or_default();
    let synonyms_list = synonyms_fst.stream().into_strs().unwrap();

    let mut synonyms_deletion = index.synonyms_deletion();
    for synonym in synonyms_list {
        synonyms_deletion.delete_all_alternatives_of(synonym);
    }
    let update_id = synonyms_deletion
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

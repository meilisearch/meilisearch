use meilisearch_core::settings::{Settings, SettingsUpdate, UpdateState, DEFAULT_RANKING_RULES};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use tide::{Request, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::RequestExt;
use crate::helpers::tide::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

pub async fn get_all(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let stop_words_fst = index.main.stop_words_fst(&reader)?;
    let stop_words = stop_words_fst.unwrap_or_default().stream().into_strs()?;
    let stop_words: BTreeSet<String> = stop_words.into_iter().collect();

    let synonyms_fst = index.main.synonyms_fst(&reader)?.unwrap_or_default();
    let synonyms_list = synonyms_fst.stream().into_strs()?;

    let mut synonyms = BTreeMap::new();
    let index_synonyms = &index.synonyms;
    for synonym in synonyms_list {
        let alternative_list = index_synonyms.synonyms(&reader, synonym.as_bytes())?;
        if let Some(list) = alternative_list {
            let list = list.stream().into_strs()?;
            synonyms.insert(synonym, list);
        }
    }

    let ranking_rules = index
        .main
        .ranking_rules(&reader)?
        .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
        .into_iter()
        .map(|r| r.to_string())
        .collect();

    let distinct_attribute = index.main.distinct_attribute(&reader)?;

    let schema = index.main.schema(&reader)?;

    let searchable_attributes = schema.clone().map(|s| {
        s.indexed_name()
            .iter()
            .map(|s| (*s).to_string())
            .collect::<Vec<String>>()
    });

    let displayed_attributes = schema.clone().map(|s| {
        s.displayed_name()
            .iter()
            .map(|s| (*s).to_string())
            .collect::<HashSet<String>>()
    });

    let accept_new_fields = schema.map(|s| s.accept_new_fields());

    let settings = Settings {
        ranking_rules: Some(Some(ranking_rules)),
        distinct_attribute: Some(distinct_attribute),
        searchable_attributes: Some(searchable_attributes),
        displayed_attributes: Some(displayed_attributes),
        stop_words: Some(Some(stop_words)),
        synonyms: Some(Some(synonyms)),
        accept_new_fields: Some(accept_new_fields),
    };

    Ok(tide::Response::new(200).body_json(&settings).unwrap())
}

#[derive(Default, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateSettings {
    pub ranking_rules: Option<Vec<String>>,
    pub distinct_attribute: Option<String>,
    pub primary_key: Option<String>,
    pub searchable_attributes: Option<Vec<String>>,
    pub displayed_attributes: Option<HashSet<String>>,
    pub stop_words: Option<BTreeSet<String>>,
    pub synonyms: Option<BTreeMap<String, Vec<String>>>,
    pub accept_new_fields: Option<bool>,
}

pub async fn update_all(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let settings_update: UpdateSettings =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        ranking_rules: Some(settings_update.ranking_rules),
        distinct_attribute: Some(settings_update.distinct_attribute),
        searchable_attributes: Some(settings_update.searchable_attributes),
        displayed_attributes: Some(settings_update.displayed_attributes),
        stop_words: Some(settings_update.stop_words),
        synonyms: Some(settings_update.synonyms),
        accept_new_fields: Some(settings_update.accept_new_fields),
    };

    let mut writer = db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn delete_all(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        distinct_attribute: UpdateState::Clear,
        primary_key: UpdateState::Clear,
        searchable_attributes: UpdateState::Clear,
        displayed_attributes: UpdateState::Clear,
        stop_words: UpdateState::Clear,
        synonyms: UpdateState::Clear,
        accept_new_fields: UpdateState::Clear,
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn get_rules(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let ranking_rules = index
        .main
        .ranking_rules(&reader)?
        .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
        .into_iter()
        .map(|r| r.to_string())
        .collect::<Vec<String>>();

    Ok(tide::Response::new(200).body_json(&ranking_rules).unwrap())
}

pub async fn update_rules(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let ranking_rules: Option<Vec<String>> =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        ranking_rules: Some(ranking_rules),
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn delete_rules(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn get_distinct(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let distinct_attribute = index.main.distinct_attribute(&reader)?;

    Ok(tide::Response::new(200)
        .body_json(&distinct_attribute)
        .unwrap())
}

pub async fn update_distinct(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let distinct_attribute: Option<String> =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        distinct_attribute: Some(distinct_attribute),
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn delete_distinct(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        distinct_attribute: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn get_searchable(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let searchable_attributes: Option<Vec<String>> =
        schema.map(|s| s.indexed_name().iter().map(|i| (*i).to_string()).collect());

    Ok(tide::Response::new(200)
        .body_json(&searchable_attributes)
        .unwrap())
}

pub async fn update_searchable(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let searchable_attributes: Option<Vec<String>> =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        searchable_attributes: Some(searchable_attributes),
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn delete_searchable(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;

    let settings = SettingsUpdate {
        searchable_attributes: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn displayed(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let displayed_attributes: Option<HashSet<String>> = schema.map(|s| {
        s.displayed_name()
            .iter()
            .map(|i| (*i).to_string())
            .collect()
    });

    Ok(tide::Response::new(200)
        .body_json(&displayed_attributes)
        .unwrap())
}

pub async fn update_displayed(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let displayed_attributes: Option<HashSet<String>> =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        displayed_attributes: Some(displayed_attributes),
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn delete_displayed(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;

    let settings = SettingsUpdate {
        displayed_attributes: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn get_accept_new_fields(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let accept_new_fields = schema.map(|s| s.accept_new_fields());

    Ok(tide::Response::new(200)
        .body_json(&accept_new_fields)
        .unwrap())
}

pub async fn update_accept_new_fields(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;
    let index = ctx.index()?;
    let accept_new_fields: Option<bool> =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        accept_new_fields: Some(accept_new_fields),
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

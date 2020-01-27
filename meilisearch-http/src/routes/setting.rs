use meilisearch_core::settings::{Settings, SettingsUpdate, UpdateState};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use tide::{Request, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::RequestExt;
use crate::models::token::ACL::*;
use crate::routes::document::IndexUpdateResponse;
use crate::Data;

pub async fn get_all(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let stop_words_fst = index.main.stop_words_fst(&reader)?;
    let stop_words = stop_words_fst.unwrap_or_default().stream().into_strs()?;
    let stop_words: BTreeSet<String> = stop_words.into_iter().collect();
    let stop_words = if stop_words.len() > 0 {
        Some(stop_words)
    } else {
        None
    };

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

    let synonyms = if synonyms.len() > 0 {
        Some(synonyms)
    } else {
        None
    };

    let ranking_rules = match index.main.ranking_rules(&reader)? {
        Some(rules) => Some(rules.iter().map(|r| r.to_string()).collect()),
        None => None,
    };
    let ranking_distinct = index.main.ranking_distinct(&reader)?;

    let schema = index.main.schema(&reader)?;

    let attribute_identifier = schema.clone().map(|s| s.identifier());
    let attributes_searchable = schema.clone().map(|s| s.get_indexed_name());
    let attributes_displayed = schema.clone().map(|s| s.get_displayed_name());
    let index_new_fields = schema.map(|s| s.must_index_new_fields());

    let settings = Settings {
        ranking_rules,
        ranking_distinct,
        attribute_identifier,
        attributes_searchable,
        attributes_displayed,
        stop_words,
        synonyms,
        index_new_fields,
    };

    Ok(tide::Response::new(200).body_json(&settings).unwrap())
}

pub async fn update_all(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let settings: Settings = ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings.into_cleared())?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

pub async fn delete_all(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        ranking_distinct: UpdateState::Clear,
        attribute_identifier: UpdateState::Clear,
        attributes_searchable: UpdateState::Clear,
        attributes_displayed: UpdateState::Clear,
        stop_words: UpdateState::Clear,
        synonyms: UpdateState::Clear,
        index_new_fields: UpdateState::Clear,
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetRankingSettings {
    pub ranking_rules: Option<Vec<String>>,
    pub ranking_distinct: Option<String>,
}

pub async fn get_ranking(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let ranking_rules = match index.main.ranking_rules(&reader)? {
        Some(rules) => Some(rules.iter().map(|r| r.to_string()).collect()),
        None => None,
    };

    let ranking_distinct = index.main.ranking_distinct(&reader)?;
    let settings = GetRankingSettings {
        ranking_rules,
        ranking_distinct,
    };

    Ok(tide::Response::new(200).body_json(&settings).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetRankingSettings {
    pub ranking_rules: Option<Vec<String>>,
    pub ranking_distinct: Option<String>,
}

pub async fn update_ranking(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let settings: SetRankingSettings = ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        ranking_rules: settings.ranking_rules,
        ranking_distinct: settings.ranking_distinct,
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings.into_cleared())?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

pub async fn delete_ranking(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        ranking_distinct: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetRankingRulesSettings {
    pub ranking_rules: Option<Vec<String>>,
}

pub async fn get_rules(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let ranking_rules = match index.main.ranking_rules(&reader)? {
        Some(rules) => Some(rules.iter().map(|r| r.to_string()).collect()),
        None => None,
    };

    let settings = GetRankingRulesSettings { ranking_rules };

    Ok(tide::Response::new(200).body_json(&settings).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetRankingRulesSettings {
    pub ranking_rules: Option<Vec<String>>,
}

pub async fn update_rules(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let settings: SetRankingRulesSettings =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        ranking_rules: settings.ranking_rules,
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings.into_cleared())?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

pub async fn delete_rules(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
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
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetRankingDistinctSettings {
    pub ranking_distinct: Option<String>,
}

pub async fn get_distinct(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let ranking_distinct = index.main.ranking_distinct(&reader)?;
    let settings = GetRankingDistinctSettings { ranking_distinct };

    Ok(tide::Response::new(200).body_json(&settings).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetRankingDistinctSettings {
    pub ranking_distinct: Option<String>,
}

pub async fn update_distinct(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let settings: SetRankingDistinctSettings =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        ranking_distinct: settings.ranking_distinct,
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings.into_cleared())?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

pub async fn delete_distinct(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let settings = SettingsUpdate {
        ranking_distinct: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetAttributesSettings {
    pub attribute_identifier: Option<String>,
    pub attributes_searchable: Option<Vec<String>>,
    pub attributes_displayed: Option<HashSet<String>>,
}

pub async fn get_attributes(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let attribute_identifier = schema.clone().map(|s| s.identifier());
    let attributes_searchable = schema.clone().map(|s| s.get_indexed_name());
    let attributes_displayed = schema.clone().map(|s| s.get_displayed_name());

    let settings = GetAttributesSettings {
        attribute_identifier,
        attributes_searchable,
        attributes_displayed,
    };

    Ok(tide::Response::new(200).body_json(&settings).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetAttributesSettings {
    pub attribute_identifier: Option<String>,
    pub attributes_searchable: Option<Vec<String>>,
    pub attributes_displayed: Option<HashSet<String>>,
}

pub async fn update_attributes(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let settings: SetAttributesSettings =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        attribute_identifier: settings.attribute_identifier,
        attributes_searchable: settings.attributes_searchable,
        attributes_displayed: settings.attributes_displayed,
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings.into_cleared())?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

pub async fn delete_attributes(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;

    let settings = SettingsUpdate {
        attributes_searchable: UpdateState::Clear,
        attributes_displayed: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AttributesIdentifierSettings {
    pub attribute_identifier: Option<String>,
}

pub async fn get_identifier(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let attribute_identifier = schema.map(|s| s.identifier());

    let settings = AttributesIdentifierSettings {
        attribute_identifier,
    };

    Ok(tide::Response::new(200).body_json(&settings).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetAttributesSearchableSettings {
    pub attributes_searchable: Option<Vec<String>>,
}

pub async fn get_searchable(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let attributes_searchable = schema.map(|s| s.get_indexed_name());

    let settings = GetAttributesSearchableSettings {
        attributes_searchable,
    };

    Ok(tide::Response::new(200).body_json(&settings).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetAttributesSearchableSettings {
    pub attributes_searchable: Option<Vec<String>>,
}

pub async fn update_searchable(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let settings: SetAttributesSearchableSettings =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        attributes_searchable: settings.attributes_searchable,
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings.into_cleared())?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

pub async fn delete_searchable(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;

    let settings = SettingsUpdate {
        attributes_searchable: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AttributesDisplayedSettings {
    pub attributes_displayed: Option<HashSet<String>>,
}

pub async fn get_displayed(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let attributes_displayed = schema.map(|s| s.get_displayed_name());

    let settings = AttributesDisplayedSettings {
        attributes_displayed,
    };

    Ok(tide::Response::new(200).body_json(&settings).unwrap())
}

pub async fn update_displayed(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let settings: AttributesDisplayedSettings =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let db = &ctx.state().db;

    let settings = Settings {
        attributes_displayed: settings.attributes_displayed,
        ..Settings::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings.into_cleared())?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

pub async fn delete_displayed(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;
    let index = ctx.index()?;
    let db = &ctx.state().db;

    let settings = SettingsUpdate {
        attributes_displayed: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body).unwrap())
}

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

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SettingBody {
    pub ranking_order: Option<RankingOrder>,
    pub distinct_field: Option<DistinctField>,
    pub ranking_rules: Option<RankingRules>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RankingOrdering {
    Asc,
    Dsc,
}

pub type RankingOrder = Vec<String>;
pub type DistinctField = String;
pub type RankingRules = HashMap<String, RankingOrdering>;

pub async fn get(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsRead)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let settings = match index.main.customs(&reader).unwrap() {
        Some(bytes) => bincode::deserialize(bytes).unwrap(),
        None => SettingBody::default(),
    };

    Ok(tide::response::json(settings))
}

pub async fn update(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(SettingsWrite)?;

    let settings: SettingBody = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    let index = ctx.index()?;

    let db = &ctx.state().db;
    let reader = db.main_write_txn().map_err(ResponseError::internal)?;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut current_settings = match index.main.customs(&reader).unwrap() {
        Some(bytes) => bincode::deserialize(bytes).unwrap(),
        None => SettingBody::default(),
    };

    if let Some(ranking_order) = settings.ranking_order {
        current_settings.ranking_order = Some(ranking_order);
    }

    if let Some(distinct_field) = settings.distinct_field {
        current_settings.distinct_field = Some(distinct_field);
    }

    if let Some(ranking_rules) = settings.ranking_rules {
        current_settings.ranking_rules = Some(ranking_rules);
    }

    let bytes = bincode::serialize(&current_settings).unwrap();

    let update_id = index
        .customs_update(&mut writer, bytes)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

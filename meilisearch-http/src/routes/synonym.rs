use std::collections::BTreeMap;

use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post};
use indexmap::IndexMap;
use meilisearch_core::settings::{SettingsUpdate, UpdateState};

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::{IndexParam, IndexUpdateResponse};
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(get).service(update).service(delete);
}

#[get(
    "/indexes/{index_uid}/settings/synonyms",
    wrap = "Authentication::Private"
)]
async fn get(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let reader = data.db.main_read_txn()?;

    let synonyms_fst = index.main.synonyms_fst(&reader)?.unwrap_or_default();
    let synonyms_list = synonyms_fst.stream().into_strs()?;

    let mut synonyms = IndexMap::new();
    let index_synonyms = &index.synonyms;
    for synonym in synonyms_list {
        let alternative_list = index_synonyms.synonyms(&reader, synonym.as_bytes())?;

        if let Some(list) = alternative_list {
            let list = list.stream().into_strs()?;
            synonyms.insert(synonym, list);
        }
    }

    Ok(HttpResponse::Ok().json(synonyms))
}

#[post(
    "/indexes/{index_uid}/settings/synonyms",
    wrap = "Authentication::Private"
)]
async fn update(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<BTreeMap<String, Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        synonyms: UpdateState::Update(body.into_inner()),
        ..SettingsUpdate::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete(
    "/indexes/{index_uid}/settings/synonyms",
    wrap = "Authentication::Private"
)]
async fn delete(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        synonyms: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

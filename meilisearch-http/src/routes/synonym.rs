use std::collections::BTreeMap;

use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post};
use indexmap::IndexMap;
use meilisearch_core::settings::{SettingsUpdate, UpdateState};

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::routes::IndexUpdateResponse;
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
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;

    let reader = data.db.main_read_txn()?;

    let synonyms_list = index.main.synonyms(&reader)?;

    let mut synonyms = IndexMap::new();
    let index_synonyms = &index.synonyms;
    for synonym in synonyms_list {
        let list = index_synonyms.synonyms(&reader, synonym.as_bytes())?;
        synonyms.insert(synonym, list);
    }

    Ok(HttpResponse::Ok().json(synonyms))
}

#[post(
    "/indexes/{index_uid}/settings/synonyms",
    wrap = "Authentication::Private"
)]
async fn update(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
    body: web::Json<BTreeMap<String, Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;

    let settings = SettingsUpdate {
        synonyms: UpdateState::Update(body.into_inner()),
        ..SettingsUpdate::default()
    };

    let update_id = data
        .db
        .update_write(|w| index.settings_update(w, settings))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete(
    "/indexes/{index_uid}/settings/synonyms",
    wrap = "Authentication::Private"
)]
async fn delete(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;

    let settings = SettingsUpdate {
        synonyms: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = data
        .db
        .update_write(|w| index.settings_update(w, settings))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

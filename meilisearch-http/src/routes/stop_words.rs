use actix_web::{delete, get, post, web, HttpResponse};
use meilisearch_core::settings::{SettingsUpdate, UpdateState};
use std::collections::BTreeSet;

use crate::error::ResponseError;
use crate::routes::{IndexParam, IndexUpdateResponse};
use crate::Data;

#[get("/indexes/{index_uid}/settings/stop-words")]
pub async fn get(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let reader = data.db.main_read_txn()?;
    let stop_words_fst = index.main.stop_words_fst(&reader)?;
    let stop_words = stop_words_fst.unwrap_or_default().stream().into_strs()?;

    Ok(HttpResponse::Ok().json(stop_words))
}

#[post("/indexes/{index_uid}/settings/stop-words")]
pub async fn update(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<BTreeSet<String>>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        stop_words: UpdateState::Update(body.into_inner()),
        ..SettingsUpdate::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete("/indexes/{index_uid}/settings/stop-words")]
pub async fn delete(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        stop_words: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

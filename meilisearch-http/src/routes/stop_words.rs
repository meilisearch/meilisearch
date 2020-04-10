use std::collections::BTreeSet;

use meilisearch_core::settings::{SettingsUpdate, UpdateState};
use actix_web::{web, get, post, delete, HttpResponse};
use actix_web as aweb;

use crate::error::{ResponseError};
use crate::Data;
use crate::routes::{IndexUpdateResponse, IndexParam};

#[get("/indexes/{index_uid}/settings/stop-words")]
pub async fn get(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let reader = data.db.main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let stop_words_fst = index.main.stop_words_fst(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let stop_words = stop_words_fst.unwrap_or_default().stream().into_strs()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Ok().json(stop_words))
}

#[post("/indexes/{index_uid}/settings/stop-words")]
pub async fn update(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<BTreeSet<String>>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = SettingsUpdate {
        stop_words: UpdateState::Update(body.into_inner()),
        ..SettingsUpdate::default()
    };

    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete("/indexes/{index_uid}/settings/stop-words")]
pub async fn delete(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = SettingsUpdate {
        stop_words: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

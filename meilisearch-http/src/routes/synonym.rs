use std::collections::BTreeMap;

use indexmap::IndexMap;
use meilisearch_core::settings::{SettingsUpdate, UpdateState};
use actix_web::{web, get, post, delete, HttpResponse};
use actix_web as aweb;

use crate::error::{ResponseError};
use crate::Data;
use crate::routes::{IndexUpdateResponse, IndexParam};

#[get("/indexes/{index_uid}/settings/synonyms")]
pub async fn get(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let reader = data.db.main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let synonyms_fst = index.main.synonyms_fst(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?
        .unwrap_or_default();
    let synonyms_list = synonyms_fst.stream().into_strs()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let mut synonyms = IndexMap::new();

    let index_synonyms = &index.synonyms;

    for synonym in synonyms_list {
        let alternative_list = index_synonyms.synonyms(&reader, synonym.as_bytes())
            .map_err(|err| ResponseError::Internal(err.to_string()))?;

        if let Some(list) = alternative_list {
            let list = list.stream().into_strs()
                .map_err(|err| ResponseError::Internal(err.to_string()))?;
            synonyms.insert(synonym, list);
        }
    }

    Ok(HttpResponse::Ok().json(synonyms))
}

#[post("/indexes/{index_uid}/settings/synonyms")]
pub async fn update(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<BTreeMap<String, Vec<String>>>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = SettingsUpdate {
        synonyms: UpdateState::Update(body.into_inner()),
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

#[delete("/indexes/{index_uid}/settings/synonyms")]
pub async fn delete(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = SettingsUpdate {
        synonyms: UpdateState::Clear,
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

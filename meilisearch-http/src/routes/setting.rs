use meilisearch_core::settings::{Settings, SettingsUpdate, UpdateState, DEFAULT_RANKING_RULES};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use actix_web::{web, get, post, delete, HttpResponse};
use actix_web as aweb;

use crate::error::{ResponseError};
use crate::Data;
use crate::routes::{IndexUpdateResponse, IndexParam};

#[post("/indexes/{index_uid}/settings")]
pub async fn update_all(
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
    let stop_words: BTreeSet<String> = stop_words.into_iter().collect();

    let synonyms_fst = index.main.synonyms_fst(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?
        .unwrap_or_default();
    let synonyms_list = synonyms_fst.stream().into_strs()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let mut synonyms = BTreeMap::new();
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

    let ranking_rules = index
        .main
        .ranking_rules(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?
        .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
        .into_iter()
        .map(|r| r.to_string())
        .collect();

    let distinct_attribute = index.main.distinct_attribute(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let schema = index.main.schema(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

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

    Ok(HttpResponse::Ok().json(settings))
}

#[get("/indexes/{index_uid}/settings")]
pub async fn get_all(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Settings>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let settings = body.into_inner().into_update()
        .map_err(|e| ResponseError::BadRequest(e.to_string()))?;
    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete("/indexes/{index_uid}/settings")]
pub async fn delete_all(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

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

    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get("/indexes/{index_uid}/settings/ranking-rules")]
pub async fn get_rules(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let reader = data.db.main_read_txn()
            .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let ranking_rules = index
        .main
        .ranking_rules(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?
        .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
        .into_iter()
        .map(|r| r.to_string())
        .collect::<Vec<String>>();

    Ok(HttpResponse::Ok().json(ranking_rules))
}

#[post("/indexes/{index_uid}/settings/ranking-rules")]
pub async fn update_rules(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<Vec<String>>>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = Settings {
        ranking_rules: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let settings = settings.into_update()
        .map_err(|e| ResponseError::BadRequest(e.to_string()))?;
    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete("/indexes/{index_uid}/settings/ranking-rules")]
pub async fn delete_rules(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let settings = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get("/indexes/{index_uid}/settings/distinct-attribute")]
pub async fn get_distinct(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let reader = data.db.main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let distinct_attribute = index.main.distinct_attribute(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Ok().json(distinct_attribute))
}

#[post("/indexes/{index_uid}/settings/distinct-attribute")]
pub async fn update_distinct(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<String>>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = Settings {
        distinct_attribute: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let settings = settings.into_update()
        .map_err(|e| ResponseError::BadRequest(e.to_string()))?;
    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete("/indexes/{index_uid}/settings/distinct-attribute")]
pub async fn delete_distinct(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let mut writer = data.db.update_write_txn()
            .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let settings = SettingsUpdate {
        distinct_attribute: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)
            .map_err(|err| ResponseError::Internal(err.to_string()))?;

    writer.commit()
            .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get("/indexes/{index_uid}/settings/searchable-attributes")]
pub async fn get_searchable(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let reader = data.db.main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let schema = index.main.schema(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let searchable_attributes: Option<Vec<String>> =
        schema.map(|s| s.indexed_name().iter().map(|i| (*i).to_string()).collect());

    Ok(HttpResponse::Ok().json(searchable_attributes))
}

#[post("/indexes/{index_uid}/settings/searchable-attributes")]
pub async fn update_searchable(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<Vec<String>>>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = Settings {
        searchable_attributes: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let settings = settings.into_update()
        .map_err(|e| ResponseError::BadRequest(e.to_string()))?;
    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete("/indexes/{index_uid}/settings/searchable-attributes")]
pub async fn delete_searchable(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = SettingsUpdate {
        searchable_attributes: UpdateState::Clear,
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

#[get("/indexes/{index_uid}/settings/displayed-attributes")]
pub async fn get_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let reader = data.db.main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let schema = index.main.schema(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let displayed_attributes: Option<HashSet<String>> = schema.map(|s| {
        s.displayed_name()
            .iter()
            .map(|i| (*i).to_string())
            .collect()
    });

    Ok(HttpResponse::Ok().json(displayed_attributes))
}

#[post("/indexes/{index_uid}/settings/displayed-attributes")]
pub async fn update_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<HashSet<String>>>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = Settings {
        displayed_attributes: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let settings = settings.into_update()
        .map_err(|e| ResponseError::BadRequest(e.to_string()))?;
    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete("/indexes/{index_uid}/settings/displayed-attributes")]
pub async fn delete_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = SettingsUpdate {
        displayed_attributes: UpdateState::Clear,
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

#[get("/indexes/{index_uid}/settings/accept-new-fields")]
pub async fn get_accept_new_fields(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let reader = data.db.main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let schema = index.main.schema(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let accept_new_fields = schema.map(|s| s.accept_new_fields());

    Ok(HttpResponse::Ok().json(accept_new_fields))
}

#[post("/indexes/{index_uid}/settings/accept-new-fields")]
pub async fn update_accept_new_fields(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<bool>>,
) -> aweb::Result<HttpResponse> {
    let index = data.db.open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let settings = Settings {
        accept_new_fields: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let settings = settings.into_update()
        .map_err(|e| ResponseError::BadRequest(e.to_string()))?;
    let update_id = index.settings_update(&mut writer, settings)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    writer.commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

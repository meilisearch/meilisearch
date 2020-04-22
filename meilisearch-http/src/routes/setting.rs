use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post};
use meilisearch_core::settings::{Settings, SettingsUpdate, UpdateState, DEFAULT_RANKING_RULES};
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::{IndexParam, IndexUpdateResponse};
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(update_all)
        .service(get_all)
        .service(delete_all)
        .service(get_rules)
        .service(update_rules)
        .service(delete_rules)
        .service(get_distinct)
        .service(update_distinct)
        .service(delete_distinct)
        .service(get_searchable)
        .service(update_searchable)
        .service(delete_searchable)
        .service(get_displayed)
        .service(update_displayed)
        .service(delete_displayed)
        .service(get_accept_new_fields)
        .service(update_accept_new_fields);
}

#[post("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn update_all(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Settings>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let mut writer = data.db.update_write_txn()?;
    let settings = body
        .into_inner()
        .into_update()
        .map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn get_all(
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

    Ok(HttpResponse::Ok().json(settings))
}

#[delete("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn delete_all(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let mut writer = data.db.update_write_txn()?;

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

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn get_rules(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let reader = data.db.main_read_txn()?;

    let ranking_rules = index
        .main
        .ranking_rules(&reader)?
        .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
        .into_iter()
        .map(|r| r.to_string())
        .collect::<Vec<String>>();

    Ok(HttpResponse::Ok().json(ranking_rules))
}

#[post(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn update_rules(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = Settings {
        ranking_rules: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn delete_rules(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let mut writer = data.db.update_write_txn()?;

    let settings = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn get_distinct(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let reader = data.db.main_read_txn()?;
    let distinct_attribute = index.main.distinct_attribute(&reader)?;

    Ok(HttpResponse::Ok().json(distinct_attribute))
}

#[post(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn update_distinct(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<String>>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = Settings {
        distinct_attribute: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn delete_distinct(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let mut writer = data.db.update_write_txn()?;

    let settings = SettingsUpdate {
        distinct_attribute: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = index.settings_update(&mut writer, settings)?;

    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn get_searchable(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let reader = data.db.main_read_txn()?;
    let schema = index.main.schema(&reader)?;
    let searchable_attributes: Option<Vec<String>> =
        schema.map(|s| s.indexed_name().iter().map(|i| (*i).to_string()).collect());

    Ok(HttpResponse::Ok().json(searchable_attributes))
}

#[post(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn update_searchable(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = Settings {
        searchable_attributes: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn delete_searchable(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        searchable_attributes: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn get_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let reader = data.db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let displayed_attributes: Option<HashSet<String>> = schema.map(|s| {
        s.displayed_name()
            .iter()
            .map(|i| (*i).to_string())
            .collect()
    });

    Ok(HttpResponse::Ok().json(displayed_attributes))
}

#[post(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn update_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<HashSet<String>>>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = Settings {
        displayed_attributes: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn delete_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        displayed_attributes: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get(
    "/indexes/{index_uid}/settings/accept-new-fields",
    wrap = "Authentication::Private"
)]
async fn get_accept_new_fields(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let reader = data.db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let accept_new_fields = schema.map(|s| s.accept_new_fields());

    Ok(HttpResponse::Ok().json(accept_new_fields))
}

#[post(
    "/indexes/{index_uid}/settings/accept-new-fields",
    wrap = "Authentication::Private"
)]
async fn update_accept_new_fields(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<bool>>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let settings = Settings {
        accept_new_fields: Some(body.into_inner()),
        ..Settings::default()
    };

    let mut writer = data.db.update_write_txn()?;
    let settings = settings.into_update().map_err(ResponseError::bad_request)?;
    let update_id = index.settings_update(&mut writer, settings)?;
    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

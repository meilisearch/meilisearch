use std::collections::{BTreeMap, BTreeSet};

use actix_web::{delete, get, post};
use actix_web::{web, HttpResponse};
use meilisearch_core::{MainReader, UpdateWriter};
use meilisearch_core::settings::{Settings, SettingsUpdate, UpdateState, DEFAULT_RANKING_RULES};
use meilisearch_schema::Schema;

use crate::Data;
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::routes::{IndexParam, IndexUpdateResponse};

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
        .service(get_attributes_for_faceting)
        .service(delete_attributes_for_faceting)
        .service(update_attributes_for_faceting);
}

pub fn update_all_settings_txn(
    data: &web::Data<Data>,
    settings: SettingsUpdate,
    index_uid: &str,
    write_txn: &mut UpdateWriter,
) -> Result<u64, Error> {
    let index = data
        .db
        .open_index(index_uid)
        .ok_or(Error::index_not_found(index_uid))?;

    let update_id = index.settings_update(write_txn, settings)?;
    Ok(update_id)
}

#[post("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn update_all(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Settings>,
) -> Result<HttpResponse, ResponseError> {
    let update_id = data.get_or_create_index(&path.index_uid, |index| {
        Ok(data.db.update_write::<_, _, ResponseError>(|writer| {
            let settings = body.into_inner().to_update().map_err(Error::bad_request)?;
            let update_id = index.settings_update(writer, settings)?;
            Ok(update_id)
        })?)
    })?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

pub fn get_all_sync(data: &web::Data<Data>, reader: &MainReader, index_uid: &str) -> Result<Settings, Error> {
    let index = data
        .db
        .open_index(index_uid)
        .ok_or(Error::index_not_found(index_uid))?;

    let stop_words: BTreeSet<String> = index.main.stop_words(&reader)?.into_iter().collect();

    let synonyms_list = index.main.synonyms(reader)?;

    let mut synonyms = BTreeMap::new();
    let index_synonyms = &index.synonyms;
    for synonym in synonyms_list {
        let list = index_synonyms.synonyms(reader, synonym.as_bytes())?;
        synonyms.insert(synonym, list);
    }

    let ranking_rules = index
        .main
        .ranking_rules(reader)?
        .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
        .into_iter()
        .map(|r| r.to_string())
        .collect();

    let schema = index.main.schema(&reader)?;

    let distinct_attribute = match (index.main.distinct_attribute(reader)?, &schema) {
        (Some(id), Some(schema)) => schema.name(id).map(str::to_string),
        _ => None,
    };

    let attributes_for_faceting = match (&schema, &index.main.attributes_for_faceting(&reader)?) {
        (Some(schema), Some(attrs)) => attrs
            .iter()
            .filter_map(|&id| schema.name(id))
            .map(str::to_string)
            .collect(),
        _ => vec![],
    };

    let searchable_attributes = schema.as_ref().map(get_indexed_attributes);
    let displayed_attributes = schema.as_ref().map(get_displayed_attributes);

    Ok(Settings {
        ranking_rules: Some(Some(ranking_rules)),
        distinct_attribute: Some(distinct_attribute),
        searchable_attributes: Some(searchable_attributes),
        displayed_attributes: Some(displayed_attributes),
        stop_words: Some(Some(stop_words)),
        synonyms: Some(Some(synonyms)),
        attributes_for_faceting: Some(Some(attributes_for_faceting)),
    })
}

#[get("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn get_all(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let reader = data.db.main_read_txn()?;
    let settings = get_all_sync(&data, &reader, &path.index_uid)?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        distinct_attribute: UpdateState::Clear,
        primary_key: UpdateState::Clear,
        searchable_attributes: UpdateState::Clear,
        displayed_attributes: UpdateState::Clear,
        stop_words: UpdateState::Clear,
        synonyms: UpdateState::Clear,
        attributes_for_faceting: UpdateState::Clear,
    };

    let update_id = data
        .db
        .update_write(|w| index.settings_update(w, settings))?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;
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
    let update_id = data.get_or_create_index(&path.index_uid, |index| {
        let settings = Settings {
            ranking_rules: Some(body.into_inner()),
            ..Settings::default()
        };

        let settings = settings.to_update().map_err(Error::bad_request)?;
        Ok(data
            .db
            .update_write(|w| index.settings_update(w, settings))?)
    })?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = data
        .db
        .update_write(|w| index.settings_update(w, settings))?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;
    let reader = data.db.main_read_txn()?;
    let distinct_attribute_id = index.main.distinct_attribute(&reader)?;
    let schema = index.main.schema(&reader)?;
    let distinct_attribute = match (schema, distinct_attribute_id) {
        (Some(schema), Some(id)) => schema.name(id).map(str::to_string),
        _ => None,
    };

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
    let update_id = data.get_or_create_index(&path.index_uid, |index| {
        let settings = Settings {
            distinct_attribute: Some(body.into_inner()),
            ..Settings::default()
        };

        let settings = settings.to_update().map_err(Error::bad_request)?;
        Ok(data
            .db
            .update_write(|w| index.settings_update(w, settings))?)
    })?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        distinct_attribute: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = data
        .db
        .update_write(|w| index.settings_update(w, settings))?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;
    let reader = data.db.main_read_txn()?;
    let schema = index.main.schema(&reader)?;
    let searchable_attributes: Option<Vec<String>> = schema.as_ref().map(get_indexed_attributes);

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
    let update_id = data.get_or_create_index(&path.index_uid, |index| {
        let settings = Settings {
            searchable_attributes: Some(body.into_inner()),
            ..Settings::default()
        };

        let settings = settings.to_update().map_err(Error::bad_request)?;

        Ok(data
            .db
            .update_write(|w| index.settings_update(w, settings))?)
    })?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        searchable_attributes: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = data
        .db
        .update_write(|w| index.settings_update(w, settings))?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;
    let reader = data.db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let displayed_attributes = schema.as_ref().map(get_displayed_attributes);

    Ok(HttpResponse::Ok().json(displayed_attributes))
}

#[post(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn update_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<BTreeSet<String>>>,
) -> Result<HttpResponse, ResponseError> {
    let update_id = data.get_or_create_index(&path.index_uid, |index| {
        let settings = Settings {
            displayed_attributes: Some(body.into_inner()),
            ..Settings::default()
        };

        let settings = settings.to_update().map_err(Error::bad_request)?;
        Ok(data
            .db
            .update_write(|w| index.settings_update(w, settings))?)
    })?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        displayed_attributes: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = data
        .db
        .update_write(|w| index.settings_update(w, settings))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[get(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn get_attributes_for_faceting(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let attributes_for_faceting = data.db.main_read::<_, _, ResponseError>(|reader| {
        let schema = index.main.schema(reader)?;
        let attrs = index.main.attributes_for_faceting(reader)?;
        let attr_names = match (&schema, &attrs) {
            (Some(schema), Some(attrs)) => attrs
                .iter()
                .filter_map(|&id| schema.name(id))
                .map(str::to_string)
                .collect(),
            _ => vec![],
        };
        Ok(attr_names)
    })?;

    Ok(HttpResponse::Ok().json(attributes_for_faceting))
}

#[post(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn update_attributes_for_faceting(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    let update_id = data.get_or_create_index(&path.index_uid, |index| {
        let settings = Settings {
            attributes_for_faceting: Some(body.into_inner()),
            ..Settings::default()
        };

        let settings = settings.to_update().map_err(Error::bad_request)?;
        Ok(data
            .db
            .update_write(|w| index.settings_update(w, settings))?)
    })?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn delete_attributes_for_faceting(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let settings = SettingsUpdate {
        attributes_for_faceting: UpdateState::Clear,
        ..SettingsUpdate::default()
    };

    let update_id = data
        .db
        .update_write(|w| index.settings_update(w, settings))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

fn get_indexed_attributes(schema: &Schema) -> Vec<String> {
    if schema.is_searchable_all() {
        ["*"].iter().map(|s| s.to_string()).collect()
    } else {
        schema
            .searchable_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

fn get_displayed_attributes(schema: &Schema) -> BTreeSet<String> {
    if schema.is_displayed_all() {
        ["*"].iter().map(|s| s.to_string()).collect()
    } else {
        schema
            .displayed_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post};
use meilisearch_core::settings::{Settings, SettingsUpdate, UpdateState, DEFAULT_RANKING_RULES};
use meilisearch_schema::Schema;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::Data;

macro_rules! make_delete_route {
    ($route:literal, $name:ident, $attr:ident) => {
        #[delete($route, wrap = "Authentication::Private")]
        async fn $name(
            data: web::Data<Data>,
            index_uid: web::Path<String>,
        ) -> Result<HttpResponse, ResponseError> {
            let settings_update = SettingsUpdate {
                $attr: UpdateState::Clear,
                ..SettingsUpdate::default()
            };
            let response = data.update_settings(index_uid.as_ref(), settings_update)?;
            Ok(HttpResponse::Accepted().json(response))
        }
    };
}

macro_rules! make_update_route {
    ($route:literal, $name:ident, $type:ty, $attr:ident) => {
        #[post($route, wrap = "Authentication::Private")]
        async fn $name(
            data: web::Data<Data>,
            index_uid: web::Path<String>,
            body: web::Json<$type>,
        ) -> Result<HttpResponse, ResponseError> {
            let settings = Settings {
                $attr: Some(body.into_inner()),
                ..Settings::default()
            };

            let settings_update = settings.to_update().map_err(Error::bad_request)?;
            let response = data.update_settings(index_uid.as_ref(), settings_update)?;

            Ok(HttpResponse::Accepted().json(response))
        }
    };
}

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

#[post("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn update_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
    body: web::Json<Settings>,
) -> Result<HttpResponse, ResponseError> {
    let settings_update = body.to_update().map_err(Error::bad_request)?;
    let response = data.update_settings(index_uid.as_ref(), settings_update)?;
    Ok(HttpResponse::Accepted().json(response))
}

#[get("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn get_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;

    let reader = data.db.main_read_txn()?;

    let stop_words: BTreeSet<String> = index.main.stop_words(&reader)?.into_iter().collect();

    let synonyms_list = index.main.synonyms(&reader)?;

    let mut synonyms = BTreeMap::new();
    let index_synonyms = &index.synonyms;
    for synonym in synonyms_list {
        let list = index_synonyms.synonyms(&reader, synonym.as_bytes())?;
        synonyms.insert(synonym, list);
    }

    let ranking_rules = index
        .main
        .ranking_rules(&reader)?
        .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
        .into_iter()
        .map(|r| r.to_string())
        .collect();

    let schema = index.main.schema(&reader)?;

    let distinct_attribute = match (index.main.distinct_attribute(&reader)?, &schema) {
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

    let settings = Settings {
        ranking_rules: Some(Some(ranking_rules)),
        distinct_attribute: Some(distinct_attribute),
        searchable_attributes: Some(searchable_attributes),
        displayed_attributes: Some(displayed_attributes),
        stop_words: Some(Some(stop_words)),
        synonyms: Some(Some(synonyms)),
        attributes_for_faceting: Some(Some(attributes_for_faceting)),
    };

    Ok(HttpResponse::Ok().json(settings))
}

#[delete("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn delete_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
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
    let response = data.update_settings(index_uid.as_ref(), settings)?;

    Ok(HttpResponse::Accepted().json(response))
}

#[get(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn get_rules(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;
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

make_update_route!(
    "/indexes/{index_uid}/settings/ranking-rules",
    update_rules,
    Option<Vec<String>>,
    ranking_rules
);

make_delete_route!(
    "/indexes/{index_uid}/settings/ranking-rules",
    delete_rules,
    ranking_rules
);

#[get(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn get_distinct(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;
    let reader = data.db.main_read_txn()?;
    let distinct_attribute_id = index.main.distinct_attribute(&reader)?;
    let schema = index.main.schema(&reader)?;
    let distinct_attribute = match (schema, distinct_attribute_id) {
        (Some(schema), Some(id)) => schema.name(id).map(str::to_string),
        _ => None,
    };

    Ok(HttpResponse::Ok().json(distinct_attribute))
}

make_update_route!(
    "/indexes/{index_uid}/settings/distinct-attribute",
    update_distinct,
    Option<String>,
    distinct_attribute
);

make_delete_route!(
    "/indexes/{index_uid}/settings/distinct-attribute",
    delete_distinct,
    distinct_attribute
);

#[get(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn get_searchable(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;
    let reader = data.db.main_read_txn()?;
    let schema = index.main.schema(&reader)?;
    let searchable_attributes: Option<Vec<String>> = schema.as_ref().map(get_indexed_attributes);

    Ok(HttpResponse::Ok().json(searchable_attributes))
}

make_update_route!(
    "/indexes/{index_uid}/settings/searchable-attributes",
    update_searchable,
    Option<Vec<String>>,
    searchable_attributes
);

make_delete_route!(
    "/indexes/{index_uid}/settings/searchable-attributes",
    delete_searchable,
    searchable_attributes
);

#[get(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn get_displayed(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;
    let reader = data.db.main_read_txn()?;

    let schema = index.main.schema(&reader)?;

    let displayed_attributes = schema.as_ref().map(get_displayed_attributes);

    Ok(HttpResponse::Ok().json(displayed_attributes))
}

make_update_route!(
    "/indexes/{index_uid}/settings/displayed-attributes",
    update_displayed,
    Option<HashSet<String>>,
    displayed_attributes
);

make_delete_route!(
    "/indexes/{index_uid}/settings/displayed-attributes",
    delete_displayed,
    displayed_attributes
);

#[get(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn get_attributes_for_faceting(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;

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

make_update_route!(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    update_attributes_for_faceting,
    Option<Vec<String>>,
    attributes_for_faceting
);

make_delete_route!(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    delete_attributes_for_faceting,
    attributes_for_faceting
);

fn get_indexed_attributes(schema: &Schema) -> Vec<String> {
    if schema.is_indexed_all() {
        ["*"].iter().map(|s| s.to_string()).collect()
    } else {
        schema
            .indexed_name()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

fn get_displayed_attributes(schema: &Schema) -> HashSet<String> {
    if schema.is_displayed_all() {
        ["*"].iter().map(|s| s.to_string()).collect()
    } else {
        schema
            .displayed_name()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}
